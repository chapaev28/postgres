/*-------------------------------------------------------------------------
 *
 * gistvacuum.c
 *	  vacuuming routines for the postgres GiST index access method.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/gist/gistvacuum.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/gist_private.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"
#include "utils/snapmgr.h"
#include "access/xact.h"

/*
 * VACUUM cleanup: update FSM
 */
Datum
gistvacuumcleanup(PG_FUNCTION_ARGS)
{
	IndexVacuumInfo *info = (IndexVacuumInfo *) PG_GETARG_POINTER(0);
	IndexBulkDeleteResult *stats = (IndexBulkDeleteResult *) PG_GETARG_POINTER(1);
	Relation	rel = info->index;
	BlockNumber npages,
				blkno;
	BlockNumber totFreePages;
	bool		needLock;

	/* No-op in ANALYZE ONLY mode */
	if (info->analyze_only)
		PG_RETURN_POINTER(stats);

	/* Set up all-zero stats if gistbulkdelete wasn't called */
	if (stats == NULL)
	{
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
		/* use heap's tuple count */
		stats->num_index_tuples = info->num_heap_tuples;
		stats->estimated_count = info->estimated_count;

		/*
		 * XXX the above is wrong if index is partial.  Would it be OK to just
		 * return NULL, or is there work we must do below?
		 */
	}

	/*
	 * Need lock unless it's local to this backend.
	 */
	needLock = !RELATION_IS_LOCAL(rel);

	/* try to find deleted pages */
	if (needLock)
		LockRelationForExtension(rel, ExclusiveLock);
	npages = RelationGetNumberOfBlocks(rel);
	if (needLock)
		UnlockRelationForExtension(rel, ExclusiveLock);

	totFreePages = 0;
	for (blkno = GIST_ROOT_BLKNO + 1; blkno < npages; blkno++)
	{
		Buffer		buffer;
		Page		page;

		vacuum_delay_point();

		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL,
									info->strategy);
		LockBuffer(buffer, GIST_SHARE);
		page = (Page) BufferGetPage(buffer);

		if (PageIsNew(page) || GistPageIsDeleted(page))
		{
			totFreePages++;
			RecordFreeIndexPage(rel, blkno);
		}
		UnlockReleaseBuffer(buffer);
	}

	/* Finally, vacuum the FSM */
	IndexFreeSpaceMapVacuum(info->index);

	/* return statistics */
	stats->pages_free = totFreePages;
	if (needLock)
		LockRelationForExtension(rel, ExclusiveLock);
	stats->num_pages = RelationGetNumberOfBlocks(rel);
	if (needLock)
		UnlockRelationForExtension(rel, ExclusiveLock);

	PG_RETURN_POINTER(stats);
}

typedef struct GistBDItem
{
	GistNSN		parentlsn;
	BlockNumber blkno;
	struct GistBDItem *next;
} GistBDItem;

typedef struct GistBDSItem
{
	BlockNumber blkno;
	bool isParent;
	struct GistBDSItem *next;
} GistBDSItem;

typedef struct GistBlockInfo {
	BlockNumber blkno;
	BlockNumber parent;
	BlockNumber leftblock;		/* block that has rightlink on blkno */
	bool toDelete;				/* is need delete this block? */
	bool isDeleted;				/* this block was processed 	*/
	bool hasRightLink;
} GistBlockInfo;

static void
pushStackIfSplited(Page page, GistBDItem *stack)
{
	GISTPageOpaque opaque = GistPageGetOpaque(page);

	if (stack->blkno != GIST_ROOT_BLKNO && !XLogRecPtrIsInvalid(stack->parentlsn) &&
		(GistFollowRight(page) || stack->parentlsn < GistPageGetNSN(page)) &&
		opaque->rightlink != InvalidBlockNumber /* sanity check */ )
	{
		/* split page detected, install right link to the stack */

		GistBDItem *ptr = (GistBDItem *) palloc(sizeof(GistBDItem));

		ptr->blkno = opaque->rightlink;
		ptr->parentlsn = stack->parentlsn;
		ptr->next = stack->next;
		stack->next = ptr;
	}
}
static void
gistFillBlockInfo(HTAB * map, BlockNumber blkno)
{
	GistBlockInfo *entry;
	bool		found;

	entry = (GistBlockInfo *) hash_search(map,
										   (const void *) &blkno,
										   HASH_ENTER,
										   &found);
	if(!found) {
		entry->parent = InvalidBlockNumber;
		entry->leftblock = InvalidBlockNumber;
		entry->hasRightLink = false;
		entry->toDelete = false;
		entry->isDeleted = false;
	}
}

static void
gistMemorizeParentTab(HTAB * map, BlockNumber child, BlockNumber parent)
{
	GistBlockInfo *entry;
	bool		found;

	entry = (GistBlockInfo *) hash_search(map,
										   (const void *) &child,
										   HASH_ENTER,
										   &found);
	if(!found) {
		entry->parent = InvalidBlockNumber;
		entry->leftblock = InvalidBlockNumber;
		entry->hasRightLink = false;
		entry->toDelete = false;
		entry->isDeleted = false;
	}
	entry->parent = parent;
}
static BlockNumber
gistGetParentTab(HTAB * map, BlockNumber child)
{
	GistBlockInfo *entry;
	bool		found;

	/* Find node buffer in hash table */
	entry = (GistBlockInfo *) hash_search(map,
										   (const void *) &child,
										   HASH_FIND,
										   &found);
	if (!found)
		elog(ERROR, "could not find parent of block %d in lookup table", child);

	return entry->parent;
}

static BlockNumber gistGetLeftLink(HTAB * map, BlockNumber right)
{
	GistBlockInfo *entry;
	bool		found;
	entry = (GistBlockInfo *) hash_search(map,
										   (const void *) &right,
										   HASH_FIND,
										   &found);
	if (!found)
		return InvalidBlockNumber;
	if(entry->hasRightLink == false) {
		return InvalidBlockNumber;
	}
	return entry->leftblock;
}
static void gistMemorizeLeftLink(HTAB * map, BlockNumber right, BlockNumber left, bool hasRightLink)
{
	GistBlockInfo *entry;
	bool		found;
	entry = (GistBlockInfo *) hash_search(map,
										   (const void *) &right,
										   HASH_ENTER,
										   &found);
	if (!found) {
		entry->leftblock = InvalidBlockNumber;
		entry->parent = InvalidBlockNumber;
		entry->hasRightLink = false;
		entry->toDelete = false;
		entry->isDeleted = false;
	}

	if(hasRightLink) {
		entry->leftblock = left;
		entry->hasRightLink = hasRightLink;
	} else {
		if(!found) {
			entry->leftblock = left;
			entry->hasRightLink = hasRightLink;
		}
	}

}


static bool gistGetDeleteLink(HTAB* map, BlockNumber blkno) {
	GistBlockInfo *entry;
	bool		found;

	/* Find node buffer in hash table */
	entry = (GistBlockInfo *) hash_search(map,
										   (const void *) &blkno,
										   HASH_FIND,
										   &found);

	if (!found)
		return false;

	return entry->toDelete;
}
static bool gistIsDeletedLink(HTAB* map, BlockNumber blkno) {
	GistBlockInfo *entry;
	bool		found;

	/* Find node buffer in hash table */
	entry = (GistBlockInfo *) hash_search(map,
										   (const void *) &blkno,
										   HASH_FIND,
										   &found);

	return entry ? entry->isDeleted: false;
}
static void gistMemorizeLinkToDelete(HTAB* map, BlockNumber blkno, bool isDeleted) {
	GistBlockInfo *entry;
	bool		found;
	entry = (GistBlockInfo *) hash_search(map,
										   (const void *) &blkno,
										   HASH_ENTER,
										   &found);
	if (!found) {
		entry->parent = InvalidBlockNumber;
		entry->leftblock = InvalidBlockNumber;
		entry->hasRightLink = false;
		entry->toDelete = false;
		entry->isDeleted = false;
	}
	entry->toDelete = true;
	entry->isDeleted = isDeleted;
}

/*
 * Bulk deletion of all index entries pointing to a set of heap tuples and
 * check invalid tuples left after upgrade.
 * The set of target tuples is specified via a callback routine that tells
 * whether any given heap tuple (identified by ItemPointer) is being deleted.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
static Datum
gistbulkdeletelogical(IndexVacuumInfo * info, IndexBulkDeleteResult * stats, IndexBulkDeleteCallback callback, void* callback_state)
{
	/*
	IndexVacuumInfo *info = (IndexVacuumInfo *) PG_GETARG_POINTER(0);
	IndexBulkDeleteResult *stats = (IndexBulkDeleteResult *) PG_GETARG_POINTER(1);
	IndexBulkDeleteCallback callback = (IndexBulkDeleteCallback) PG_GETARG_POINTER(2);
	void	   *callback_state = (void *) PG_GETARG_POINTER(3); */
	Relation	rel = info->index;
	GistBDItem *stack,
			   *ptr;

	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
	stats->estimated_count = false;
	stats->num_index_tuples = 0;

	stack = (GistBDItem *) palloc0(sizeof(GistBDItem));
	stack->blkno = GIST_ROOT_BLKNO;

	while (stack)
	{
		Buffer		buffer;
		Page		page;
		OffsetNumber i,
					maxoff;
		IndexTuple	idxtuple;
		ItemId		iid;

		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, stack->blkno,
									RBM_NORMAL, info->strategy);
		LockBuffer(buffer, GIST_SHARE);
		gistcheckpage(rel, buffer);
		page = (Page) BufferGetPage(buffer);

		if (GistPageIsLeaf(page))
		{
			OffsetNumber todelete[MaxOffsetNumber];
			int			ntodelete = 0;

			LockBuffer(buffer, GIST_UNLOCK);
			LockBuffer(buffer, GIST_EXCLUSIVE);

			page = (Page) BufferGetPage(buffer);
			if (stack->blkno == GIST_ROOT_BLKNO && !GistPageIsLeaf(page))
			{
				UnlockReleaseBuffer(buffer);
				continue;
			}

			pushStackIfSplited(page, stack);


			maxoff = PageGetMaxOffsetNumber(page);

			for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
			{
				iid = PageGetItemId(page, i);
				idxtuple = (IndexTuple) PageGetItem(page, iid);

				if (callback(&(idxtuple->t_tid), callback_state))
				{
					todelete[ntodelete] = i - ntodelete;
					ntodelete++;
					stats->tuples_removed += 1;
				}
				else
					stats->num_index_tuples += 1;
			}

			if (ntodelete)
			{
				START_CRIT_SECTION();

				MarkBufferDirty(buffer);

				for (i = 0; i < ntodelete; i++)
					PageIndexTupleDelete(page, todelete[i]);
				GistMarkTuplesDeleted(page);

				if (RelationNeedsWAL(rel))
				{
					XLogRecPtr	recptr;

					recptr = gistXLogUpdate(rel->rd_node, buffer,
											todelete, ntodelete,
											NULL, 0, InvalidBuffer);
					PageSetLSN(page, recptr);
				}
				else
					PageSetLSN(page, gistGetFakeLSN(rel));

				END_CRIT_SECTION();
			}

		}
		else
		{
			pushStackIfSplited(page, stack);

			maxoff = PageGetMaxOffsetNumber(page);

			for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
			{
				iid = PageGetItemId(page, i);
				idxtuple = (IndexTuple) PageGetItem(page, iid);

				ptr = (GistBDItem *) palloc(sizeof(GistBDItem));
				ptr->blkno = ItemPointerGetBlockNumber(&(idxtuple->t_tid));
				ptr->parentlsn = PageGetLSN(page);
				ptr->next = stack->next;
				stack->next = ptr;

				if (GistTupleIsInvalid(idxtuple))
					ereport(LOG,
							(errmsg("index \"%s\" contains an inner tuple marked as invalid",
									RelationGetRelationName(rel)),
							 errdetail("This is caused by an incomplete page split at crash recovery before upgrading to PostgreSQL 9.1."),
							 errhint("Please REINDEX it.")));
			}
		}

		UnlockReleaseBuffer(buffer);

		ptr = stack->next;
		pfree(stack);
		stack = ptr;

		vacuum_delay_point();
	}

	PG_RETURN_POINTER(stats);
}

static void
gistvacuumcheckrightlink(Relation rel, IndexVacuumInfo * info,
		HTAB* infomap, BlockNumber blkno, Page page)
{
	/*
	 *
	 * if there is right link on this page but not rightlink from this page. remove rightlink from left page.
	 * if there is right link on this page and there is a right link . right link of left page must be rightlink to rightlink of this page.
	 * */

	BlockNumber leftblkno;
	GISTPageOpaque childopaque;

	leftblkno = gistGetLeftLink(infomap, blkno);
	if (leftblkno != InvalidBlockNumber) {
		/*
		 * there is a right link to child page
		 * */
		BlockNumber newRight = InvalidBuffer;
		GISTPageOpaque leftOpaque;
		Page left;
		Buffer leftbuffer;
		leftbuffer = ReadBufferExtended(rel, MAIN_FORKNUM, leftblkno,
				RBM_NORMAL, info->strategy);
		left = (Page) BufferGetPage(leftbuffer);

		LockBuffer(leftbuffer, GIST_EXCLUSIVE);
		childopaque = GistPageGetOpaque(page);
		leftOpaque = GistPageGetOpaque(left);

		while (leftOpaque->rightlink != InvalidBlockNumber
				&& leftOpaque->rightlink != blkno) {
			leftblkno = leftOpaque->rightlink;
			UnlockReleaseBuffer(leftbuffer);
			leftbuffer = ReadBufferExtended(rel, MAIN_FORKNUM, leftblkno,
					RBM_NORMAL, info->strategy);
			left = (Page) BufferGetPage(leftbuffer);

			LockBuffer(leftbuffer, GIST_EXCLUSIVE);
			leftOpaque = GistPageGetOpaque(left);

		}
		if (leftOpaque->rightlink == InvalidBlockNumber) {
			/*
			 * error!! we dont find leftpage.
			 * */

			UnlockReleaseBuffer(leftbuffer);
			return;
		}
		if (childopaque->rightlink != InvalidBlockNumber) {
			newRight = childopaque->rightlink;
		}
		leftOpaque->rightlink = newRight;

		if (RelationNeedsWAL(rel)) {
			XLogRecPtr recptr;
			recptr = gistXLogRightLinkChange(rel->rd_node, leftbuffer, newRight);
			PageSetLSN(left, recptr);
		} else
			PageSetLSN(left, gistGetFakeLSN(rel));

		UnlockReleaseBuffer(leftbuffer);
	}
}
static void
gistvacuumrepairpage(Relation rel, IndexVacuumInfo * info, IndexBulkDeleteResult * stats,
		IndexBulkDeleteCallback callback, void* callback_state,

		HTAB* infomap, BlockNumber blkno)
{
	Buffer buffer;
	Page page;
	PageHeader header;
	OffsetNumber maxoff, i;
	IndexTuple idxtuple;
	ItemId iid;
	OffsetNumber todelete[MaxOffsetNumber];
	int ntodelete = 0;
	bool isNew;

	buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blkno,
			RBM_NORMAL, info->strategy);
	LockBuffer(buffer, GIST_EXCLUSIVE);

	gistcheckpage(rel, buffer);
	page = (Page) BufferGetPage(buffer);
	/*
	 * if page is inner do nothing.
	 * */
	if(GistPageIsLeaf(page)) {
		maxoff = PageGetMaxOffsetNumber(page);
		for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i)) {
			iid = PageGetItemId(page, i);
			idxtuple = (IndexTuple) PageGetItem(page, iid);

			if (callback(&(idxtuple->t_tid), callback_state)) {
				todelete[ntodelete] = i - ntodelete;
				ntodelete++;
			}
		}
		isNew = PageIsNew(page) || PageIsEmpty(page);
		if (ntodelete || isNew) {
			START_CRIT_SECTION();

			MarkBufferDirty(buffer);

			for (i = 0; i < ntodelete; i++)
				PageIndexTupleDelete(page, todelete[i]);
			GistMarkTuplesDeleted(page);

			if (RelationNeedsWAL(rel)) {
				XLogRecPtr recptr;

				recptr = gistXLogUpdate(rel->rd_node, buffer, todelete,
						ntodelete,
						NULL, 0, InvalidBuffer);
				PageSetLSN(page, recptr);
			} else
				PageSetLSN(page, gistGetFakeLSN(rel));
			END_CRIT_SECTION();

			/*
			 * ok. links has been deleted. and this in wal! now we can set deleted and repair rightlinks
			 * */

			gistvacuumcheckrightlink(rel, info, infomap, blkno, page);

			/*
			 * ok. rightlinks has been repaired.
			 * */
			header = (PageHeader) page;

			header->pd_prune_xid = GetCurrentTransactionId();

			GistPageSetDeleted(page);
			stats->pages_deleted++;

			if (RelationNeedsWAL(rel)) {
				XLogRecPtr recptr;

				recptr = gistXLogSetDeleted(rel->rd_node, buffer, header->pd_prune_xid);
				PageSetLSN(page, recptr);
			} else
				PageSetLSN(page, gistGetFakeLSN(rel));
		}
	}

	UnlockReleaseBuffer(buffer);
}
static void
gistphysicalvacuum(Relation rel, IndexVacuumInfo * info, IndexBulkDeleteResult * stats,
		IndexBulkDeleteCallback callback, void* callback_state,
		BlockNumber npages, HTAB* infomap,
		GistBDSItem* rescanstack, GistBDSItem* tail)
{
	BlockNumber blkno = GIST_ROOT_BLKNO;
	for (; blkno < npages; blkno++) {
		Buffer buffer;
		Page page;
		OffsetNumber i, maxoff;
		IndexTuple idxtuple;
		ItemId iid;
		OffsetNumber todelete[MaxOffsetNumber];
		int ntodelete = 0;
		GISTPageOpaque opaque;
		BlockNumber child;
		GistBDSItem *item;
		bool isNew;

		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL,
				info->strategy);
		LockBuffer(buffer, GIST_SHARE);
		gistcheckpage(rel, buffer);
		page = (Page) BufferGetPage(buffer);

		isNew = PageIsNew(page) || PageIsEmpty(page);
		opaque = GistPageGetOpaque(page);

		gistFillBlockInfo(infomap, blkno);

		gistMemorizeLeftLink(infomap, blkno, InvalidBlockNumber, false);

		if(opaque->rightlink != InvalidBlockNumber) {
			gistMemorizeLeftLink(infomap, opaque->rightlink, blkno, true);
		}
		if (GistPageIsLeaf(page)) {

			LockBuffer(buffer, GIST_UNLOCK);
			LockBuffer(buffer, GIST_EXCLUSIVE);

			maxoff = PageGetMaxOffsetNumber(page);
			for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i)) {
				iid = PageGetItemId(page, i);
				idxtuple = (IndexTuple) PageGetItem(page, iid);

				if (callback(&(idxtuple->t_tid), callback_state)) {
					todelete[ntodelete] = i - ntodelete;
					ntodelete++;
					stats->tuples_removed += 1;
				} else
					stats->num_index_tuples += 1;
			}
		} else {
			/*
			 * first scan
			 * */

			maxoff = PageGetMaxOffsetNumber(page);
			if (blkno != GIST_ROOT_BLKNO
					/*&& (GistFollowRight(page) || lastNSN < GistPageGetNSN(page)) */
					&& opaque->rightlink != InvalidBlockNumber) {
				/*
				 * build left link map. add to rescan later.
				 * */
				item = (GistBDSItem *) palloc(sizeof(GistBDSItem));

				item->isParent = false;
				item->blkno = opaque->rightlink;
				item->next = NULL;

				tail->next = item;
				tail = item;

			}
			for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i)) {
				iid = PageGetItemId(page, i);
				idxtuple = (IndexTuple) PageGetItem(page, iid);
				child = ItemPointerGetBlockNumber(&(idxtuple->t_tid));

				gistMemorizeParentTab(infomap, child, blkno);

				if (GistTupleIsInvalid(idxtuple))
					ereport(LOG,
							(errmsg("index \"%s\" contains an inner tuple marked as invalid", RelationGetRelationName(rel)), errdetail("This is caused by an incomplete page split at crash recovery before upgrading to PostgreSQL 9.1."), errhint("Please REINDEX it.")));
			}

		}
		if (ntodelete || isNew) {
			if ((maxoff == ntodelete) || isNew) {

				item = (GistBDSItem *) palloc(sizeof(GistBDSItem));
				item->isParent = true;
				item->blkno = blkno;
				item->next = NULL;

				tail->next = item;
				tail = item;


				gistMemorizeLinkToDelete(infomap, blkno, false);
			} else {
				START_CRIT_SECTION();

				MarkBufferDirty(buffer);

				for (i = 0; i < ntodelete; i++)
					PageIndexTupleDelete(page, todelete[i]);
				GistMarkTuplesDeleted(page);

				if (RelationNeedsWAL(rel)) {
					XLogRecPtr recptr;

					recptr = gistXLogUpdate(rel->rd_node, buffer, todelete,
							ntodelete,
							NULL, 0, InvalidBuffer);
					PageSetLSN(page, recptr);
				} else
					PageSetLSN(page, gistGetFakeLSN(rel));

				END_CRIT_SECTION();
			}
		}

		UnlockReleaseBuffer(buffer);
		vacuum_delay_point();
	}
}
static void
gistrescanvacuum(Relation rel, IndexVacuumInfo * info, IndexBulkDeleteResult * stats,
		IndexBulkDeleteCallback callback, void* callback_state,
		HTAB* infomap,
		GistBDSItem* rescanstack, GistBDSItem* tail)
{
	GistBDSItem * ptr;
	while (rescanstack != NULL) {
		Buffer buffer;
		Page page;
		OffsetNumber i, maxoff;
		IndexTuple idxtuple;
		ItemId iid;
		OffsetNumber todelete[MaxOffsetNumber];
		int ntodelete = 0;
		GISTPageOpaque opaque;
		BlockNumber blkno, child;
		Buffer childBuffer;
		GistBDSItem *item;
		bool isNew;
		bool isDeleted;

		BlockNumber setdeletedblkno[MaxOffsetNumber];

		blkno = rescanstack->blkno;
		if (gistGetParentTab(infomap, blkno) == InvalidBlockNumber && blkno != GIST_ROOT_BLKNO) {
			/*
			 * strange pages. it's maybe(pages without parent but not root).
			 * for example when last vacuum shut down and we can set this page as deleted
			 * repair that pages.
			 * how repaire: remove data if exists. rightlink repair. set-deleted
			 */
			gistvacuumrepairpage(rel, info, stats, callback, callback_state, infomap, blkno);

			ptr = rescanstack->next;
			pfree(rescanstack);
			rescanstack = ptr;

			vacuum_delay_point();
			continue;
		}
		if (rescanstack->isParent == true) {
			blkno = gistGetParentTab(infomap, blkno);
		}

		isDeleted = gistIsDeletedLink(infomap, blkno);

		if(isDeleted == true || blkno == InvalidBlockNumber) {

			ptr = rescanstack->next;
			pfree(rescanstack);
			rescanstack = ptr;

			vacuum_delay_point();
			continue;
		}
		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blkno,
				RBM_NORMAL, info->strategy);
		LockBuffer(buffer, GIST_SHARE);

		gistcheckpage(rel, buffer);
		page = (Page) BufferGetPage(buffer);

		opaque = GistPageGetOpaque(page);

		if (blkno != GIST_ROOT_BLKNO
				&& opaque->rightlink != InvalidBlockNumber) {
			item = (GistBDSItem *) palloc(sizeof(GistBDSItem));

			item->isParent = false;
			item->blkno = opaque->rightlink;
			item->next = rescanstack->next;

			rescanstack->next = item;
		}

		if (GistPageIsLeaf(page)) {
			/* usual procedure with leafs pages*/
			LockBuffer(buffer, GIST_UNLOCK);
			LockBuffer(buffer, GIST_EXCLUSIVE);

			maxoff = PageGetMaxOffsetNumber(page);
			for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i)) {
				iid = PageGetItemId(page, i);
				idxtuple = (IndexTuple) PageGetItem(page, iid);

				if (callback(&(idxtuple->t_tid), callback_state)) {
					todelete[ntodelete] = i - ntodelete;
					ntodelete++;
				}
			}
		} else {
			LockBuffer(buffer, GIST_UNLOCK);
			LockBuffer(buffer, GIST_EXCLUSIVE);
			maxoff = PageGetMaxOffsetNumber(page);
			for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i)) {
				bool delete;
				iid = PageGetItemId(page, i);
				idxtuple = (IndexTuple) PageGetItem(page, iid);

				child = ItemPointerGetBlockNumber(&(idxtuple->t_tid));

				delete = gistGetDeleteLink(infomap, child);
				/*
				 * leaf is needed to delete????
				 * */
				if (delete) {
					IndexTuple idxtuplechild;
					ItemId iidchild;
					OffsetNumber todeletechild[MaxOffsetNumber];
					int ntodeletechild = 0;
					OffsetNumber j, maxoffchild;
					Page childpage;
					bool childIsNew;

					childBuffer = ReadBufferExtended(rel, MAIN_FORKNUM, child,
							RBM_NORMAL, info->strategy);

					LockBuffer(childBuffer, GIST_EXCLUSIVE);

					childpage = (Page) BufferGetPage(childBuffer);
					childIsNew = PageIsNew(childpage) || PageIsEmpty(childpage);

					if (GistPageIsLeaf(childpage)) {
						maxoffchild = PageGetMaxOffsetNumber(childpage);
						for (j = FirstOffsetNumber; j <= maxoffchild; j =
								OffsetNumberNext(j)) {
							iidchild = PageGetItemId(childpage, j);
							idxtuplechild = (IndexTuple) PageGetItem(childpage,
									iidchild);

							if (callback(&(idxtuplechild->t_tid),
									callback_state)) {
								todeletechild[ntodeletechild] = j
										- ntodeletechild;
								ntodeletechild++;
							}
						}
						if (ntodeletechild || childIsNew) {
							START_CRIT_SECTION();

							MarkBufferDirty(childBuffer);

							for (j = 0; j < ntodeletechild; j++)
								PageIndexTupleDelete(childpage,
										todeletechild[j]);
							GistMarkTuplesDeleted(childpage);

							if (RelationNeedsWAL(rel)) {
								XLogRecPtr recptr;

								recptr = gistXLogUpdate(rel->rd_node,
										childBuffer, todeletechild,
										ntodeletechild,
										NULL, 0, InvalidBuffer);
								PageSetLSN(childpage, recptr);
							} else
								PageSetLSN(childpage, gistGetFakeLSN(rel));

							END_CRIT_SECTION();

							if ((ntodeletechild == maxoffchild) || childIsNew) {

								/*
								 *
								 * if there is right link on this page but not rightlink from this page. remove rightlink from left page.
								 * if there is right link on this page and there is a right link . right link of left page must be rightlink to rightlink of this page.
								 * */

								gistMemorizeLinkToDelete(infomap, child, true);
								todelete[ntodelete] = i - ntodelete;
								setdeletedblkno[ntodelete] = child;
								ntodelete++;

							}
						}
					}
					UnlockReleaseBuffer(childBuffer);
				}
			}
		}
		isNew = PageIsNew(page) || PageIsEmpty(page);
		if (ntodelete || isNew) {
			if(GistPageIsLeaf(page)) {
				item = (GistBDSItem *) palloc(sizeof(GistBDSItem));

				item->isParent = false;
				item->blkno = gistGetParentTab(infomap, blkno);
				item->next = rescanstack->next;
				rescanstack->next = item;
			} else {
				// could not remove symbolic link
				/*
				 * delete links to pages
				 * */
				if(ntodelete && (ntodelete == maxoff) ) {
					// save 1 link on inner page
					ntodelete--;
				}
				START_CRIT_SECTION();

				MarkBufferDirty(buffer);

				for (i = 0; i < ntodelete; i++)
					PageIndexTupleDelete(page, todelete[i]);
				GistMarkTuplesDeleted(page);

				if (RelationNeedsWAL(rel)) {
					XLogRecPtr recptr;

					recptr = gistXLogUpdate(rel->rd_node, buffer, todelete,
							ntodelete,
							NULL, 0, InvalidBuffer);
					PageSetLSN(page, recptr);
				} else
					PageSetLSN(page, gistGetFakeLSN(rel));
				END_CRIT_SECTION();

				/*
				 * ok. links has been deleted. and this in wal! now we can set deleted and repair rightlinks
				 * */
				for (i = 0; i < ntodelete; i++) {
					Buffer childBuffertodelete;
					Page childpagetodelete;
					PageHeader p;
					childBuffertodelete = ReadBufferExtended(rel, MAIN_FORKNUM, setdeletedblkno[i],
							RBM_NORMAL, info->strategy);

					LockBuffer(childBuffertodelete, GIST_EXCLUSIVE);

					childpagetodelete = (Page) BufferGetPage(childBuffertodelete);

					p = (PageHeader) childpagetodelete;

					p->pd_prune_xid = GetCurrentTransactionId();

					gistvacuumcheckrightlink(rel, info, infomap,
							setdeletedblkno[i], childpagetodelete);
					GistPageSetDeleted(childpagetodelete);
					MarkBufferDirty(childBuffertodelete);
					UnlockReleaseBuffer(childBuffertodelete);
					stats->pages_deleted++;
				}
			}
		}

		UnlockReleaseBuffer(buffer);

		ptr = rescanstack->next;
		pfree(rescanstack);
		rescanstack = ptr;

		vacuum_delay_point();
	}
}

Datum
gistbulkdelete(PG_FUNCTION_ARGS)
{
	IndexVacuumInfo *info = (IndexVacuumInfo *) PG_GETARG_POINTER(0);
	IndexBulkDeleteResult *stats = (IndexBulkDeleteResult *) PG_GETARG_POINTER(1);
	IndexBulkDeleteCallback callback = (IndexBulkDeleteCallback) PG_GETARG_POINTER(2);
	void	   *callback_state = (void *) PG_GETARG_POINTER(3);
	Relation	rel = info->index;
	GistBDSItem *rescanstack = NULL,
			   *tail = NULL;

	int memoryneeded = 0;

	BlockNumber npages;

	bool needLock;
	HTAB	   *infomap;
	HASHCTL		hashCtl;


	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
	stats->estimated_count = false;
	stats->num_index_tuples = 0;

	hashCtl.keysize = sizeof(BlockNumber);
	hashCtl.entrysize = sizeof(GistBlockInfo);
	hashCtl.hcxt = CurrentMemoryContext;

	needLock = !RELATION_IS_LOCAL(rel);

	if (needLock)
		LockRelationForExtension(rel, ExclusiveLock);
	npages = RelationGetNumberOfBlocks(rel);
	if (needLock)
		UnlockRelationForExtension(rel, ExclusiveLock);

	/*
	 * estimate memory limit
	 * if map more than maintance_mem_work use old version of vacuum
	 * */

	memoryneeded = npages * (sizeof(GistBlockInfo));
	if(memoryneeded > maintenance_work_mem * 1024) {
		return gistbulkdeletelogical(info, stats, callback, callback_state);
	}


	infomap = hash_create("gistvacuum info map",
										npages,
										&hashCtl,
									  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT );

	rescanstack = (GistBDSItem *) palloc(sizeof(GistBDSItem));

	rescanstack->isParent = false;
	rescanstack->blkno = GIST_ROOT_BLKNO;
	rescanstack->next = NULL;
	tail = rescanstack;

	/*
	 * this part of the vacuum use scan in physical order. Also this function fill hashmap `infomap`
	 * that stores information about parent, rightlinks and etc. Pages is needed to rescan will be pushed to tail of rescanstack.
	 * this function don't set flag gist_deleted.
	 * */
	gistphysicalvacuum(rel, info, stats, callback, callback_state, npages, infomap, rescanstack, tail);
	/*
	 * this part of the vacuum is not in physical order. It scans only pages from rescanstack.
	 * we get page if this page is leaf we use usual procedure, but if pages is inner that we scan
	 * it and delete links to childrens(but firstly recheck children and if all is ok).
	 * if any pages is empty or new after processing set flag gist_delete , store prune_xid number
	 * and etc. if all links from pages are deleted push parent of page to rescan stack to processing.
	 * special case is when all tuples are deleted from index. in this case root block will be setted in leaf.
	 * */
	gistrescanvacuum(rel, info, stats, callback, callback_state, infomap, rescanstack, tail);

	hash_destroy(infomap);
	PG_RETURN_POINTER(stats);
}
