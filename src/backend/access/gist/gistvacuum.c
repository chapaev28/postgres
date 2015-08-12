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
gistMemorizeParentTab(HTAB * map, BlockNumber child, BlockNumber parent)
{
	ParentMapEntry *entry;
	bool		found;

	entry = (ParentMapEntry *) hash_search(map,
										   (const void *) &child,
										   HASH_ENTER,
										   &found);
	//if (!found)
	//	elog(ERROR, "could not enter parent of block %d in lookup table", child);

	entry->parentblkno = parent;
}
static BlockNumber
gistGetParentTab(HTAB * map, BlockNumber child)
{
	ParentMapEntry *entry;
	bool		found;

	/* Find node buffer in hash table */
	entry = (ParentMapEntry *) hash_search(map,
										   (const void *) &child,
										   HASH_FIND,
										   &found);
	if (!found)
		elog(ERROR, "could not find parent of block %d in lookup table", child);

	return entry->parentblkno;
}
/*
static BlockNumber gistGetLeftLink(HTAB * map, BlockNumber right, bool* found)
{
	ParentMapEntry *entry;
	entry = (ParentMapEntry *) hash_search(map,
										   (const void *) &right,
										   HASH_FIND,
										   found);
	if (!found)
		return 0;
	return entry->parentblkno;
}
*/
/*
 * Bulk deletion of all index entries pointing to a set of heap tuples and
 * check invalid tuples left after upgrade.
 * The set of target tuples is specified via a callback routine that tells
 * whether any given heap tuple (identified by ItemPointer) is being deleted.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
/*
static void pushInStack(GistBDSItem* stack, GistBDSItem* item) {
	if(stack) {
		item->next = stack->next;
		stack->next = item;
	} else {
		stack = item;
	}
}
*/
typedef struct GistDelLinkItem
{
	BlockNumber blkno;
	bool toDelete;
	bool isDeleted;
} GistDelLinkItem;

typedef struct GistBlockXidItem
{
	BlockNumber blkno;
	TransactionId id;
} GistBlockXidItem;

static bool gistGetDeleteLink(HTAB* delLinkMap, BlockNumber blkno) {
	GistDelLinkItem *entry;
	bool		found;

	/* Find node buffer in hash table */
	entry = (GistDelLinkItem *) hash_search(delLinkMap,
										   (const void *) &blkno,
										   HASH_FIND,
										   &found);
	elog(LOG, "link to delete block %d %i", blkno, found ? entry->toDelete: false);

	if (!found)
		return false;

	return entry->toDelete;
}
static bool gistIsDeletedLink(HTAB* delLinkMap, BlockNumber blkno) {
	GistDelLinkItem *entry;
	bool		found;

	/* Find node buffer in hash table */
	entry = (GistDelLinkItem *) hash_search(delLinkMap,
										   (const void *) &blkno,
										   HASH_FIND,
										   &found);
	elog(LOG, "link is deleted block %d %i", blkno, entry ? entry->isDeleted: false);
	return entry ? entry->isDeleted: false;
}
static void gistMemorizeLinkToDelete(HTAB* delLinkMap, BlockNumber blkno, bool isDeleted) {
	GistDelLinkItem *entry;
	bool		found;
	entry = (GistDelLinkItem *) hash_search(delLinkMap,
										   (const void *) &blkno,
										   HASH_ENTER,
										   &found);
	/*if (!found)
		elog(LOG, "could not enter link to delete block %d %i in lookup table", blkno, isDeleted);
	else
		elog(LOG, "enter link to delete block %d %i in lookup table", blkno, isDeleted);
		*/
	entry->toDelete = true;
	entry->isDeleted = isDeleted;
}
static void gistRemoveLinkToDelete(HTAB* delLinkMap, BlockNumber blkno) {
	GistDelLinkItem *entry;
	bool		found;

	hash_search(delLinkMap, &blkno,
				HASH_REMOVE, &found);
}
static bool gistGetXidBlock(HTAB* xmap, BlockNumber blkno) {
	GistBlockXidItem *entry;
	bool		found;

	/* Find node buffer in hash table */
	entry = (GistBlockXidItem *) hash_search(xmap,
										   (const void *) &blkno,
										   HASH_FIND,
										   &found);
	if (!found)
		elog(ERROR, "could not find xid of block %d in lookup table", blkno);

	return entry->id;
}
static void gistMemorizeXidBlock(HTAB* xmap, BlockNumber blkno, TransactionId id) {
	GistBlockXidItem *entry;
	bool		found;

	entry = (GistBlockXidItem *) hash_search(xmap,
										   (const void *) &blkno,
										   HASH_ENTER,
										   &found);


	entry->id = id;

}
/*

static void vacuumPage(IndexVacuumInfo * info, Relation rel, BlockNumber blkno, Page page,
			Buffer buffer, GistNSN lastNSN, GistBDSItem* rescanstack,
			HTAB* parentMap, HTAB* delLinkMap, HTAB* rightLinkMap,
			HTAB* xmap,
			IndexBulkDeleteResult* stats, IndexBulkDeleteCallback callback, void* callback_state,
			bool fromRescan) {
}*/

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

Datum
gistbulkdelete(PG_FUNCTION_ARGS)
{
	IndexVacuumInfo *info = (IndexVacuumInfo *) PG_GETARG_POINTER(0);
	IndexBulkDeleteResult *stats = (IndexBulkDeleteResult *) PG_GETARG_POINTER(1);
	IndexBulkDeleteCallback callback = (IndexBulkDeleteCallback) PG_GETARG_POINTER(2);
	void	   *callback_state = (void *) PG_GETARG_POINTER(3);
	Relation	rel = info->index;
	GistBDSItem *rescanstack = NULL,
			   *ptr = NULL,
			   *tail = NULL;

	int memoryneeded = 0;

	BlockNumber npages,
				blkno;
	GistNSN lastNSN;

	bool needLock;
	HTAB	   *parentMap;
	HASHCTL		hashCtl;

	HTAB	   *deleteLinkMap;
	HASHCTL		hashCtlLinkMap;

	HTAB	   *rightLinkMap;
	HASHCTL		hashCtlRightLinkMap;

	HTAB	   *xidMap;
	HASHCTL		hashCtlxidMap;



	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
	stats->estimated_count = false;
	stats->num_index_tuples = 0;



	hashCtl.keysize = sizeof(BlockNumber);
	hashCtl.entrysize = sizeof(ParentMapEntry);
	hashCtl.hcxt = CurrentMemoryContext;


	hashCtlLinkMap.keysize = sizeof(BlockNumber);
	hashCtlLinkMap.entrysize = sizeof(GistDelLinkItem);
	hashCtlLinkMap.hcxt = CurrentMemoryContext;


	hashCtlRightLinkMap.keysize = sizeof(BlockNumber);
	hashCtlRightLinkMap.entrysize = sizeof(ParentMapEntry);
	hashCtlRightLinkMap.hcxt = CurrentMemoryContext;

	hashCtlxidMap.keysize = sizeof(BlockNumber);
	hashCtlxidMap.entrysize = sizeof(GistBlockXidItem);
	hashCtlxidMap.hcxt = CurrentMemoryContext;



	/* stopping truncate due to conflicting lock request */
	needLock = !RELATION_IS_LOCAL(rel);
	blkno = GIST_ROOT_BLKNO;

	/* try to find deleted pages */
	if (needLock)
		LockRelationForExtension(rel, ExclusiveLock);
	npages = RelationGetNumberOfBlocks(rel);
	if (needLock)
		UnlockRelationForExtension(rel, ExclusiveLock);

	/*
	 * estimate memory limit
	 * if parent map more than maintance_mem_work use old version of vacuum
	 * */

	memoryneeded = npages * (sizeof(ParentMapEntry) + sizeof(BlockNumber));
	if(memoryneeded > maintenance_work_mem * 1024) {
		return gistbulkdeletelogical(info, stats, callback, callback_state);
	}


	parentMap = hash_create("gistvacuum parent map",
										npages,
										&hashCtl,
									  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT );

	deleteLinkMap = hash_create("gistvacuum link map",
										npages,
										&hashCtlLinkMap,
									  HASH_ELEM | HASH_BLOBS  | HASH_CONTEXT);

/*

	rightLinkMap = hash_create("gistvacuum rightlink map",
										npages,
										&hashCtlRightLinkMap,
									  HASH_ELEM | HASH_BLOBS );
									  */
	xidMap = hash_create("gistvacuum xid map",
									npages,
									&hashCtlxidMap,
									HASH_ELEM | HASH_BLOBS );



	lastNSN = XactLastRecEnd;
	/*
	rescanstack = (GistBDSItem *) palloc(sizeof(GistBDSItem));
	rescanstack->isParent = false;
	rescanstack->blkno = blkno;
	rescanstack->next = NULL; */

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

		isNew = PageIsNew(page);

		if (GistPageIsLeaf(page)) {
			// push all information to maps
			TransactionId id = GetActiveSnapshot()->xmin;
			gistMemorizeXidBlock(xidMap, blkno, id);

			// check page to delete
			// scan page

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
		} else { /*
		 * first scan
		 * */
			maxoff = PageGetMaxOffsetNumber(page);
			opaque = GistPageGetOpaque(page);
			if (blkno != GIST_ROOT_BLKNO
					&& (GistFollowRight(page) || lastNSN < GistPageGetNSN(page))
					&& opaque->rightlink != InvalidBlockNumber) {
				//GistBDSItem *item = NULL;
				//BlockNumber left;
				//left = blkno;
				/*
				 * loop to rightlink . build left link map. add to rescan later.
				 * */
				item = (GistBDSItem *) palloc(sizeof(GistBDSItem));

				item->isParent = false;
				item->blkno = opaque->rightlink;
				item->next = NULL;

				if (rescanstack != NULL) {
					//item->next = rescanstack->next;
					//rescanstack->next = item;
					tail->next = item;
					tail = item;
				} else {
					rescanstack = item;
					tail = rescanstack;
				}

			}
			TransactionId id = GetActiveSnapshot()->xmin;
			gistMemorizeXidBlock(xidMap, blkno, id);

			for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i)) {
				iid = PageGetItemId(page, i);
				idxtuple = (IndexTuple) PageGetItem(page, iid);
				child = ItemPointerGetBlockNumber(&(idxtuple->t_tid));

				gistMemorizeParentTab(parentMap, child, blkno);

				if (GistTupleIsInvalid(idxtuple))
					ereport(LOG,
							(errmsg("index \"%s\" contains an inner tuple marked as invalid", RelationGetRelationName(rel)), errdetail("This is caused by an incomplete page split at crash recovery before upgrading to PostgreSQL 9.1."), errhint("Please REINDEX it.")));
			}

		}
		if (ntodelete || isNew) {
			if (maxoff == ntodelete || isNew) {
				item = (GistBDSItem *) palloc(sizeof(GistBDSItem));

				item->isParent = true;
				item->blkno = blkno;
				item->next = NULL;

				if (rescanstack != NULL) {
					//item->next = rescanstack->next;
					//rescanstack->next = item;
					tail->next = item;
					tail = item;
				} else {
					rescanstack = item;
					tail = rescanstack;
				}

				gistMemorizeLinkToDelete(deleteLinkMap, blkno, false);
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

//		vacuumPage(info, rel, blkno, page, buffer, lastNSN, rescanstack, parentMap, deleteLinkMap, rightLinkMap, xidMap, stats, callback, callback_state, false);

		UnlockReleaseBuffer(buffer);
		vacuum_delay_point();
	}

	while (rescanstack != NULL) {
		Buffer buffer;
		Page page;
		OffsetNumber i, maxoff;
		IndexTuple idxtuple;
		ItemId iid;
		OffsetNumber todelete[MaxOffsetNumber];
		int ntodelete = 0;
		GISTPageOpaque opaque;
		BlockNumber child;
		Buffer childBuffer;
		GistBDSItem *item;
		bool isNew;
		bool isDeleted;


		blkno = rescanstack->blkno;
		if (rescanstack->isParent == true) {
			blkno = gistGetParentTab(parentMap, rescanstack->blkno);
		}

		isDeleted = gistIsDeletedLink(deleteLinkMap, blkno);

		if(isDeleted == true) {

		//	elog(LOG, "skip page %d", blkno);

			ptr = rescanstack->next;
			pfree(rescanstack);
			rescanstack = ptr;

			vacuum_delay_point();
			continue;
		}
		//elog(LOG, "process page %d", blkno);

		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blkno,
				RBM_NORMAL, info->strategy);
		LockBuffer(buffer, GIST_SHARE);

		gistcheckpage(rel, buffer);
		page = (Page) BufferGetPage(buffer);

//		vacuumPage(info, rel, blkno, page, buffer, lastNSN, rescanstack, parentMap, deleteLinkMap, rightLinkMap, xidMap, stats, callback, callback_state, true);

		if (GistPageIsLeaf(page)) {
			// push all information to maps
			TransactionId id = GetActiveSnapshot()->xmin;
			gistMemorizeXidBlock(xidMap, blkno, id);

			// check page to delete
			// scan page

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
			/*
			 * delete leafs entry
			 * */
			TransactionId id;
			LockBuffer(buffer, GIST_UNLOCK);
			LockBuffer(buffer, GIST_EXCLUSIVE);

			maxoff = PageGetMaxOffsetNumber(page);
			opaque = GistPageGetOpaque(page);
			if (blkno != GIST_ROOT_BLKNO
					&& (GistFollowRight(page) || lastNSN < GistPageGetNSN(page))
					&& opaque->rightlink != InvalidBlockNumber) {
				item = (GistBDSItem *) palloc(sizeof(GistBDSItem));

				item->isParent = false;
				item->blkno = opaque->rightlink;
				item->next = NULL;

				if (rescanstack != NULL) {
					//item->next = rescanstack->next;
					//rescanstack->next = item;
					tail->next = item;
					tail = item;
				} else {
					rescanstack = item;
					tail = rescanstack;
				}
			}

			for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i)) {
				bool delete;
				iid = PageGetItemId(page, i);
				idxtuple = (IndexTuple) PageGetItem(page, iid);

				child = ItemPointerGetBlockNumber(&(idxtuple->t_tid));

				delete = gistGetDeleteLink(deleteLinkMap, child);
				/*
				 * leaf needed to delete????
				 * */
				if (delete) {
				/*	ereport(LOG,
							(errmsg("delete it  \"%s\" %d", RelationGetRelationName(rel), child), errdetail("This is caused by an incomplete page split at crash recovery before upgrading to PostgreSQL 9.1."), errhint("Please REINDEX it.")));
				*/
					id = gistGetXidBlock(xidMap, child);
					if (TransactionIdPrecedes(id, RecentGlobalDataXmin)) {
						// all data is visible is not held

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

						if(GistPageIsLeaf(childpage)) {
							// also check right links
							maxoffchild = PageGetMaxOffsetNumber(childpage);
							for (j = FirstOffsetNumber; j <= maxoffchild; j = OffsetNumberNext(j)) {
								iidchild = PageGetItemId(childpage, j);
								idxtuplechild = (IndexTuple) PageGetItem(childpage, iidchild);

								if (callback(&(idxtuplechild->t_tid), callback_state)) {
									todeletechild[ntodeletechild] = j - ntodeletechild;
									ntodeletechild++;

									gistRemoveLinkToDelete(deleteLinkMap, child);
									gistMemorizeLinkToDelete(deleteLinkMap, child, true);

								}
							}
							if(ntodeletechild || childIsNew) {
								START_CRIT_SECTION();

								MarkBufferDirty(childBuffer);

								for (j = 0; j < ntodeletechild; j++)
									PageIndexTupleDelete(childpage, todeletechild[j]);
								GistMarkTuplesDeleted(childpage);

								if (RelationNeedsWAL(rel)) {
									XLogRecPtr recptr;

									recptr = gistXLogUpdate(rel->rd_node, childBuffer, todeletechild,
											ntodeletechild,
											NULL, 0, InvalidBuffer);
									PageSetLSN(childpage, recptr);
								} else
									PageSetLSN(childpage, gistGetFakeLSN(rel));

								END_CRIT_SECTION();

								if((ntodeletechild == maxoffchild) || childIsNew) {

									GistPageSetDeleted(childpage);
									stats->pages_deleted++;
									todelete[ntodelete] = i - ntodelete;
									ntodelete++;
								}
							}
						} else {
							// child is inner page
							todelete[ntodelete] = i - ntodelete;
							ntodelete++;
							GistPageSetDeleted(childpage);
							stats->pages_deleted++;

						}
						UnlockReleaseBuffer(childBuffer);
					} else {
						// rescan later
						/*ereport(LOG,
								(errmsg("index \"%s\" dont precedes", RelationGetRelationName(rel)), errdetail("This is caused by an incomplete page split at crash recovery before upgrading to PostgreSQL 9.1."), errhint("Please REINDEX it.")));
						*/
					}
				}
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

			if ((maxoff == ntodelete) || isNew) {
				item = (GistBDSItem *) palloc(sizeof(GistBDSItem));

				if (blkno != GIST_ROOT_BLKNO) {

					item->isParent = false;
					item->blkno = gistGetParentTab(parentMap, blkno);
					item->next = NULL;

					//pushInStack(rescanstack, item);

					if (rescanstack != NULL) {
						tail->next = item;
						tail = item;
					} else {
						rescanstack = item;
						tail = rescanstack;
					}
					//gistRemoveLinkToDelete(deleteLinkMap, gistGetParentTab(parentMap, blkno));
					//gistMemorizeLinkToDelete(deleteLinkMap, gistGetParentTab(parentMap, blkno), false);


					/*
					 * its page is scaned. dont scan it later
					 * */

					/* link to blkno to delete and this page is processed*/
					gistRemoveLinkToDelete(deleteLinkMap, blkno);
					gistMemorizeLinkToDelete(deleteLinkMap, blkno, true);

					// GistPageGetOpaque(page)->flags |= F_LEAF;
/*
					ereport(LOG,
																(errmsg("add to rescan \"%s\" %d", RelationGetRelationName(rel), blkno), errdetail("This is caused by an incomplete page split at crash recovery before upgrading to PostgreSQL 9.1."), errhint("Please REINDEX it.")));
*/
					//gistMemorizeLinkToDelete(deleteLinkMap, blkno, false);
					//GistPageGetOpaque(page)->flags |= F_LEAF;
					/* get parent remove and add link to delete false!!!!!!!!! */

				} else {
					 GistPageGetOpaque(page)->flags |= F_LEAF;
				}
			}
		}

		UnlockReleaseBuffer(buffer);

		ptr = rescanstack->next;
		pfree(rescanstack);
		rescanstack = ptr;

		vacuum_delay_point();

	}

	hash_destroy(xidMap);
	hash_destroy(rightLinkMap);
	hash_destroy(deleteLinkMap);
	hash_destroy(parentMap);
	PG_RETURN_POINTER(stats);
}
