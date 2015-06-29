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
 * Bulk deletion of all index entries pointing to a set of heap tuples and
 * check invalid tuples left after upgrade.
 * The set of target tuples is specified via a callback routine that tells
 * whether any given heap tuple (identified by ItemPointer) is being deleted.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
void pushInStack(GistBDSItem* stack, GistBDSItem* item) {
	if(stack) {
		item->next = stack->next;
		stack->next = item;
	} else {
		stack = item;
	}
}
typedef struct GistDelLinkItem
{
	BlockNumber blkno;
	bool toDelete;
} GistDelLinkItem;

bool gistGetDeleteLink(HTAB* delLinkMap, BlockNumber child) {
	GistDelLinkItem *entry;
	bool		found;

	/* Find node buffer in hash table */
	entry = (ParentMapEntry *) hash_search(delLinkMap,
										   (const void *) &child,
										   HASH_FIND,
										   &found);
	if (!found)
		return true;

	return entry->toDelete;
}
void gistMemorizeLinkToDelete(HTAB* delLinkMap, BlockNumber blkno) {
	GistDelLinkItem *entry;
	bool		found;

	entry = (ParentMapEntry *) hash_search(delLinkMap,
										   (const void *) &blkno,
										   HASH_ENTER,
										   &found);
	entry->toDelete = true;

}
void vacuumPage(Relation rel, BlockNumber blkno, Page page,
			Buffer buffer, GistNSN lastNSN, GistBDSItem *rescanstack,
			HTAB* parentMap, HTAB* delLinkMap,
			IndexBulkDeleteResult* stats, IndexBulkDeleteCallback callback, void* callback_state,
			bool fromRescan, GistBDSItem* ptr){
	OffsetNumber i,
				maxoff;
	IndexTuple	idxtuple;
	ItemId		iid;
	OffsetNumber todelete[MaxOffsetNumber];
	int			ntodelete = 0;
	if (GistPageIsLeaf(page))
	{

		LockBuffer(buffer, GIST_UNLOCK);
		LockBuffer(buffer, GIST_EXCLUSIVE);

		page = (Page) BufferGetPage(buffer);
		if(!fromRescan) {
			if (blkno == GIST_ROOT_BLKNO && !GistPageIsLeaf(page))
			{
				UnlockReleaseBuffer(buffer);
				return;
			}
		} else {
			if (rescanstack->blkno == GIST_ROOT_BLKNO && !GistPageIsLeaf(page))
			{
				UnlockReleaseBuffer(buffer);
				ptr = rescanstack->next;
				pfree(rescanstack);
				rescanstack = ptr;
				return;
			}
		}
		GISTPageOpaque opaque = GistPageGetOpaque(page);
		if( blkno != GIST_ROOT_BLKNO &&
				(GistFollowRight(page) || lastNSN < GistPageGetNSN(page)) &&
				opaque->rightlink != InvalidBlockNumber ) {
			GistBDSItem *item = (GistBDSItem *) palloc(sizeof(GistBDSItem));

			item->blkno = opaque->rightlink;
			item->isParent = false;
			pushInStack(rescanstack, item);
		}
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

	}
	else {
		maxoff = PageGetMaxOffsetNumber(page);
		GISTPageOpaque opaque = GistPageGetOpaque(page);
		if( blkno != GIST_ROOT_BLKNO &&
				(GistFollowRight(page) || lastNSN < GistPageGetNSN(page)) &&
				opaque->rightlink != InvalidBlockNumber ) {
			GistBDSItem *item = (GistBDSItem *) palloc(sizeof(GistBDSItem));

			item->blkno = opaque->rightlink;
			item->isParent = false;
			pushInStack(rescanstack, item);
		}
		for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
		{
			iid = PageGetItemId(page, i);
			idxtuple = (IndexTuple) PageGetItem(page, iid);
			BlockNumber child;
			child = ItemPointerGetBlockNumber(&(idxtuple->t_tid));
			if(!fromRescan) {
				gistMemorizeParentTab(parentMap, child, blkno);
			} else {
				gistMemorizeParentTab(parentMap, child, rescanstack->blkno);
			}
			bool deleteLink = gistGetDeleteLink(delLinkMap, child);
			if(deleteLink) {
				todelete[ntodelete] = i - ntodelete;
				ntodelete++;
			}

			if (GistTupleIsInvalid(idxtuple))
				ereport(LOG,
						(errmsg("index \"%s\" contains an inner tuple marked as invalid",
								RelationGetRelationName(rel)),
						 errdetail("This is caused by an incomplete page split at crash recovery before upgrading to PostgreSQL 9.1."),
						 errhint("Please REINDEX it.")));
		}
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
		if (ntodelete == maxoff) {
			// set delete page and rescan parent page
			GistPageSetDeleted(page);
			stats->pages_deleted++;
			GistBDSItem *item = (GistBDSItem *) palloc(sizeof(GistBDSItem));
			item->isParent = true;
			item->blkno = blkno;
			pushInStack(rescanstack, item);
			gistMemorizeLinkToDelete(delLinkMap, blkno);
		}
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
	GistBDSItem *rescanstack,
			   *ptr;

	BlockNumber currentblk;
	BlockNumber npages,
				blkno;
	GistNSN lastNSN;
	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
	stats->estimated_count = false;
	stats->num_index_tuples = 0;
	bool needLock;
	HTAB	   *parentMap;
	HASHCTL		hashCtl;

	hashCtl.keysize = sizeof(BlockNumber);
	hashCtl.entrysize = sizeof(ParentMapEntry);
	hashCtl.hcxt = CurrentMemoryContext;


	HTAB	   *deleteLinkMap;
	HASHCTL		hashCtlLinkMap;

	hashCtlLinkMap.keysize = sizeof(BlockNumber);
	hashCtlLinkMap.entrysize = sizeof(bool);
	hashCtlLinkMap.hcxt = CurrentMemoryContext;





	int numberRescanedPage = 0;
//stopping truncate due to conflicting lock request
	needLock = !RELATION_IS_LOCAL(rel);
	blkno = GIST_ROOT_BLKNO;
rescan_physical:

	/* try to find deleted pages */
	if (needLock)
		LockRelationForExtension(rel, ExclusiveLock);
	npages = RelationGetNumberOfBlocks(rel);
	if (needLock)
		UnlockRelationForExtension(rel, ExclusiveLock);

	parentMap = hash_create("gistvacuum parent map",
										npages,
										&hashCtl,
									  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);


	deleteLinkMap = hash_create("gistvacuum link map",
										npages,
										&hashCtl,
									  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	lastNSN = XactLastRecEnd;
	for (; blkno < npages; blkno++)
	{
		Buffer		buffer;
		Page		page;

		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blkno,
									RBM_NORMAL, info->strategy);
		LockBuffer(buffer, GIST_SHARE);
		gistcheckpage(rel, buffer);
		page = (Page) BufferGetPage(buffer);

		vacuumPage(rel, blkno, page, buffer, lastNSN, rescanstack, parentMap, deleteLinkMap, stats, callback, callback_state, false, NULL);

		UnlockReleaseBuffer(buffer);
		vacuum_delay_point();
	}
rescan_stack:

	lastNSN = XactLastRecEnd;

	while (rescanstack)
	{
		Buffer		buffer;
		Page		page;
		OffsetNumber i,
					maxoff;
		IndexTuple	idxtuple;
		ItemId		iid;
		numberRescanedPage++;
		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, rescanstack->blkno,
									RBM_NORMAL, info->strategy);
		LockBuffer(buffer, GIST_SHARE);
		gistcheckpage(rel, buffer);
		page = (Page) BufferGetPage(buffer);
		if (rescanstack->isParent == true) {
			rescanstack->blkno = gistGetParentTab(parentMap, rescanstack->blkno);
		}
		vacuumPage(rel, blkno, page, buffer, lastNSN, rescanstack, parentMap, deleteLinkMap, stats, callback, callback_state, true, ptr);
		UnlockReleaseBuffer(buffer);

		ptr = rescanstack->next;
		pfree(rescanstack);
		rescanstack = ptr;
		vacuum_delay_point();
	}

	hash_destroy(parentMap);
	if (needLock)
		LockRelationForExtension(rel, ExclusiveLock);
	currentblk = RelationGetNumberOfBlocks(rel);
	if (needLock)
		UnlockRelationForExtension(rel, ExclusiveLock);
	if(currentblk > npages) {
		goto rescan_physical;
	}
	/*
	ereport(LOG,
			(errmsg("index \"%s\" rescaned page %d delete page %d",
					RelationGetRelationName(rel), numberRescanedPage, stats->pages_deleted),
			 errdetail("This is caused by an incomplete page split at crash recovery before upgrading to PostgreSQL 9.1."),
			 errhint("Please REINDEX it.")));
	*/
	PG_RETURN_POINTER(stats);
}
/*
Datum
gistbulkdelete(PG_FUNCTION_ARGS)
{
	IndexVacuumInfo *info = (IndexVacuumInfo *) PG_GETARG_POINTER(0);
	IndexBulkDeleteResult *stats = (IndexBulkDeleteResult *) PG_GETARG_POINTER(1);
	IndexBulkDeleteCallback callback = (IndexBulkDeleteCallback) PG_GETARG_POINTER(2);
	void	   *callback_state = (void *) PG_GETARG_POINTER(3);
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
*/
