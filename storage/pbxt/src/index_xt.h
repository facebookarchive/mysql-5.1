/* Copyright (c) 2005 PrimeBase Technologies GmbH
 *
 * PrimeBase XT
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * 2005-09-30	Paul McCullagh
 *
 * H&G2JCtL
 */
#ifndef __xt_index_h__
#define __xt_index_h__

#ifdef DRIZZLED
#include <drizzled/definitions.h>
#include <mysys/my_bitmap.h>
#else
#include <mysql_version.h>
#include <my_bitmap.h>
#endif
#include <time.h>

#include "thread_xt.h"
#include "linklist_xt.h"
#include "datalog_xt.h"
#include "datadic_xt.h"

#ifndef MYSQL_VERSION_ID
#error MYSQL_VERSION_ID must be defined!
#endif

//#define PRINT_IND_FLUSH_STATS

/* Define this to gather data on what area
 * of an index page is being written.
 */
#define IND_OPT_DATA_WRITTEN

#ifdef IND_OPT_DATA_WRITTEN
/* This is a debug switch that compares
 * the contents of a cached page
 * to the data just written to disk
 *
 * It requires information on what parts
 * of the cache page have been changed.
 */
//#define CHECK_IF_WRITE_WAS_OK
#endif

#ifdef IND_OPT_DATA_WRITTEN
/* Write only those parts of an index
 * page that have been modified.
 */
#define IND_WRITE_MIN_DATA
#endif

/*
 * Define this in order to complete write the
 * end of an index page (i.e. unused space),
 * if the next page to be written is the
 * one that follows this page.
 * Note that this does not work optimilly
 * with the option to write minimum data.
 */
//#define IND_FILL_BLOCK_TO_NEXT

/* Define this all writes to the index
 * file should be in block sizes and on
 * block boundaries.
 */
//#define IND_WRITE_IN_BLOCK_SIZES

/* This is the block size used to write the index: */
#define IND_WRITE_BLOCK_SIZE				XT_BLOCK_SIZE_FOR_DIRECT_IO
//#define IND_WRITE_BLOCK_SIZE				(1024*1)

/*
 * Define this to skew the split of nodes
 * when adding to the end of an index.
 */
#define IND_SKEW_SPLIT_ON_APPEND

/*
 * The maximum amount of data to write before
 * flushing the index file.
 */
#define IND_FLUSH_THRESHOLD					(512 * 1024 * 1024)

struct XTDictionary;
STRUCT_TABLE;
struct XTTable;
struct XTOpenTable;
struct XTIndex;
struct XTIndBlock;
struct XTTable;
struct XTThread;
class Field;

/*
 * INDEX ROLLBACK
 *
 * When a transaction is rolled back, the index entries are not
 * garbage collected!! Instead, the index entries are deleted
 * when the data record is garbage collected.
 *
 * When an index record is written, and this record replaces
 * some other record (i.e. a node is updated). The new record
 * references its predecessor.
 *
 * On cleanup (rollback or commit), the predecessor records
 * are garbage collected.
 *
 * NOTE: It is possible to loose memory if a crash occurs during
 * index modification. This can occur if a node is split and
 * we crash between writing the 2 new records.
 *
 */ 

/*
 * These flags influence the way the compare and search
 * routines function.
 *
 * The low-order 16 bits are reserved for the caller
 * (i.e. MySQL specific stuff).
 */
#define XT_SEARCH_WHOLE_KEY			0x10000000		/* This flag is used to search for an insertion point, or to find
													 * a particular slot that has already been inserted into the
													 * index. The compare includes the handle of the variation.
													 */
#define XT_SEARCH_AFTER_KEY			0x20000000		/* This flags searches for the position just after the given key.
													 * Even if the key is not found, success is possible if there
													 * is a value in the index that would be after the search key.
													 *
													 * If this flag is not set then we search for the first
													 * occurrence of the key in the index. If not found we 
													 * take the position just after the search key.
													 */
#define XT_SEARCH_FIRST_FLAG		0x40000000		/* Use this flags to find the first position in the index.
													 * When set, the actual key value is ignored.
													 */
#define XT_SEARCH_AFTER_LAST_FLAG	0x80000000		/* Search out the position after the last in the index.
													 * When set, the actual key value is ignored.
													 */

#define XT_INDEX_MAX_KEY_SIZE_MAX	2048			/* These are allocated on the stack, so this is the maximum! */

#define XT_INDEX_MAX_KEY_SIZE		((XT_INDEX_PAGE_SIZE >> 1) > XT_INDEX_MAX_KEY_SIZE_MAX ? XT_INDEX_MAX_KEY_SIZE_MAX : (XT_INDEX_PAGE_SIZE >> 1))

#define XT_IS_NODE_BIT				0x8000

#define XT_IS_NODE(x)				((x) & XT_IS_NODE_BIT)

#define XT_NODE_REF_SIZE			4
#define XT_GET_NODE_REF(t, x)		XT_RET_NODE_ID(XT_GET_DISK_4(x))
#define XT_SET_NODE_REF(t, x, y)	XT_SET_DISK_4((x), XT_NODE_ID(y))

#define XT_MAX_RECORD_REF_SIZE		8

#define XT_INDEX_PAGE_HEAD_SIZE		offsetof(XTIdxBranchDRec, tb_data)
#define XT_INDEX_PAGE_DATA_SIZE		(XT_INDEX_PAGE_SIZE - 2)				// XT_INDEX_PAGE_HEAD_SIZE == 2!

#define XT_MAKE_LEAF_SIZE(x)		((x) + XT_INDEX_PAGE_HEAD_SIZE)

#define XT_MAKE_NODE_SIZE(x)		(((x) + XT_INDEX_PAGE_HEAD_SIZE) | XT_IS_NODE_BIT)

#define XT_MAKE_BRANCH_SIZE(x, y)	(((x) + XT_INDEX_PAGE_HEAD_SIZE) | ((y) ? XT_IS_NODE_BIT : 0))

#define XT_GET_INDEX_BLOCK_LEN(x)	((x) & 0x7FFF)

#define XT_GET_BRANCH_DATA_SIZE(x)	(XT_GET_INDEX_BLOCK_LEN(x) - XT_INDEX_PAGE_HEAD_SIZE)

#define XT_DIRTY_BLOCK_LIST_SIZE	4096

typedef struct XTIndexHead {
	XTDiskValue4		tp_format_offset_4;	/* The offset of the format part of the header. */

	XTDiskValue4		tp_header_size_4;	/* The  size of the header. */
	XTDiskValue6		tp_not_used_6;

	XTDiskValue6		tp_ind_eof_6;
	XTDiskValue6		tp_ind_free_6;

	/* The index roots follow. Each is if_node_ref_size_1 size. */
	xtWord1				tp_data[XT_VAR_LENGTH];
} XTIndexHeadDRec, *XTIndexHeadDPtr;

typedef struct XTIndexFormat {
	XTDiskValue4		if_format_size_4;	/* The size of this structure (index format). */
	XTDiskValue2		if_tab_version_2;	/* The table version number. */
	XTDiskValue2		if_ind_version_2;	/* The index version number. */
	XTDiskValue1		if_node_ref_size_1;	/* This size of index node reference in indexes (default 4 bytes). */
	XTDiskValue1		if_rec_ref_size_1;	/* The size of record references in the indexes (default 4 bytes). */
	XTDiskValue4		if_page_size_4;
} XTIndexFormatDRec, *XTIndexFormatDPtr;

typedef struct XTIdxBranch {
	XTDiskValue2		tb_size_2;			/* No of bytes used below. */
	
	/* We enough space for 2 buffers when splitting! */
	xtWord1				tb_data[XT_INDEX_PAGE_DATA_SIZE];
} XTIdxBranchDRec, *XTIdxBranchDPtr;

typedef struct XTIdxItem {
	u_int				i_total_size;		/* Size of the data in the searched branch (excludes 2 byte header). */
	u_int				i_item_size;		/* Size of the item at this position. */
	u_int				i_node_ref_size;
	u_int				i_item_offset;		/* Item offset. */
} XTIdxItemRec, *XTIdxItemPtr;

typedef struct XTIdxResult {
	xtBool				sr_found;			/* TRUE if the key was found. */
	xtBool				sr_duplicate;		/* TRUE if the duplicate was found. */
	xtBool				sr_last_item;		/* TRUE if the last item was found. */
	xtRecordID			sr_rec_id;			/* Reference to the record of the found key. */
	xtRowID				sr_row_id;
	xtIndexNodeID		sr_branch;			/* Branch to follow when searching a node. */
	XTIdxItemRec		sr_item;
} XTIdxResultRec, *XTIdxResultPtr;

typedef struct XTIdxKeyValue {
	int					sv_flags;
	xtRecordID			sv_rec_id;
	xtRowID				sv_row_id;
	u_int				sv_length;
	xtWord1				*sv_key;
} XTIdxKeyValueRec, *XTIdxKeyValuePtr;

typedef struct XTIdxSearchKey {
	xtBool				sk_on_key;			/* TRUE if we are positioned on the search key. */
	XTIdxKeyValueRec	sk_key_value;		/* The value of the search key. */
	xtWord1				sk_key_buf[XT_INDEX_MAX_KEY_SIZE];
} XTIdxSearchKeyRec, *XTIdxSearchKeyPtr;

typedef void (*XTScanBranchFunc)(struct XTTable *tab, struct XTIndex *ind, XTIdxBranchDPtr branch, register XTIdxKeyValuePtr value, register XTIdxResultRec *result);
typedef void (*XTPrevItemFunc)(struct XTTable *tab, struct XTIndex *ind, XTIdxBranchDPtr branch, register XTIdxResultRec *result);
typedef void (*XTLastItemFunc)(struct XTTable *tab, struct XTIndex *ind, XTIdxBranchDPtr branch, register XTIdxResultRec *result);

typedef int (*XTSimpleCompFunc)(struct XTIndex *ind, u_int key_length, xtWord1 *key_value, xtWord1 *b_value);

struct charset_info_st;

typedef struct XTIndexSeg		/* Key-portion */
{
	u_int				col_idx;			/* The table column index of this component. */
	u_int				is_recs_in_range;	/* Value returned by records_in_range(). */
	u_int				is_selectivity;		/* The number of unique values per mi_select_total. */
	xtWord1				type;				/* Type of key (for sort) */
	xtWord1				language;
	xtWord1				null_bit;			/* bitmask to test for NULL */
	xtWord1				bit_start,bit_end;	/* if bit field */
	xtWord1				bit_pos,bit_length;	/* (not used in 4.1) */
	xtWord2				flag;
	xtWord2				length;				/* Keylength */
	xtWord4				start;				/* Start of key in record */
	xtWord4				null_pos;			/* position to NULL indicator */
	MX_CONST_CHARSET_INFO	*charset;
} XTIndexSegRec, *XTIndexSegPtr;

typedef struct XTIndFreeList {
	struct XTIndFreeList	*fl_next_list;				/* List of free pages for this index. */
	u_int					fl_start;					/* Start for allocating from the front of the list. */
	u_int					fl_free_count;				/* Total items in the free list. */
	xtIndexNodeID			fl_page_id[XT_VAR_LENGTH];	/* List of page ID's of the free pages. */
} XTIndFreeListRec, *XTIndFreeListPtr;

typedef struct XTIndDirtyBlocks {
	struct XTIndDirtyBlocks	*db_next;
	struct XTIndBlock		*db_blocks[XT_DIRTY_BLOCK_LIST_SIZE];
} XTIndDirtyBlocksRec, *XTIndDirtyBlocksPtr;

typedef struct XTIndDirtyListItor {
	u_int					dli_i;
	u_int					dli_count;
	XTIndDirtyBlocksPtr		dli_list;

	void dli_reset() {
		dli_i = 0;
		dli_count = 0;
		dli_list = NULL;
	}
} XTIndDirtyListItorRec, *XTIndDirtyListItorPtr;

typedef struct XTIndDirtyList {
	u_int					dl_total_blocks;			/* Count of the dirty blocks. */
	u_int					dl_list_usage;				/* The number of elements used in the first block list: */
	XTIndDirtyBlocksPtr		dl_block_lists;				/* A chain of dirty blocks. */

	xtBool	dl_add_block(struct XTIndBlock *block);
	void	dl_sort_blocks();
	void	dl_free_all();

	inline struct XTIndBlock *dl_next_block(XTIndDirtyListItorPtr it) {
		struct XTIndBlock *block;

		if (it->dli_i == it->dli_count) {
			if (it->dli_list) {
				it->dli_count = XT_DIRTY_BLOCK_LIST_SIZE;
				it->dli_list = it->dli_list->db_next;
			}
			else {
				it->dli_count = dl_list_usage;
				it->dli_list = dl_block_lists;
			}
			it->dli_i = 0;
			if (!it->dli_list)
				return NULL;
		}

		block = it->dli_list->db_blocks[it->dli_i];
		it->dli_i++;
		return block;
	}
} XTIndDirtyListRec, *XTIndDirtyListPtr;

/*
 * XT_INDEX_USE_PTHREAD_RW:
 * The stardard pthread RW lock is currently the fastest for INSERTs
 * in 32 threads on smalltab: runTest(SMALL_INSERT_TEST, 32, dbUrl)
 */
/*
 * XT_INDEX_USE_RWMUTEX:
 * But the RW mutex is a close second, if not just as fast.
 * If it is at least as fast, then it is better because read lock
 * overhead is then zero.
 *
 * If definitely does get in the way of the 
 */ 
/* XT_INDEX_USE_PTHREAD_RW:
 * But this is clearly better on Linux. 216682 instead of 169259
 * payment transactions (DBT2 in non-conflict transactions,
 * using only the customer table).
 *
 * 27.2.2009:
 * The story continues. I have now fixed a bug in RW MUTEX that
 * may have been slowing things down (see {RACE-WR_MUTEX}).
 *
 * So we will need to test "customer payment" again.
 *
 * 3.3.2009
 * Latest test show that RW mutex is slightly faster:
 * 127460 to 123574 payment transactions.
 */

#ifdef XT_NO_ATOMICS
#define XT_INDEX_USE_PTHREAD_RW
#else
//#define XT_INDEX_USE_PTHREAD_RW
#define XT_TAB_ROW_USE_XSMUTEX
//#define XT_INDEX_SPINXSLOCK
#endif

#if defined(XT_INDEX_USE_PTHREAD_RW)
#define XT_INDEX_LOCK_TYPE				xt_rwlock_type
#define XT_INDEX_INIT_LOCK(s, i)		xt_init_rwlock_with_autoname(s, &(i)->mi_rwlock)
#define XT_INDEX_FREE_LOCK(s, i)		xt_free_rwlock(&(i)->mi_rwlock)	
#define XT_INDEX_READ_LOCK(i, o)		xt_slock_rwlock_ns(&(i)->mi_rwlock)
#define XT_INDEX_WRITE_LOCK(i, o)		xt_xlock_rwlock_ns(&(i)->mi_rwlock)
#define XT_INDEX_UNLOCK(i, o)			xt_unlock_rwlock_ns(&(i)->mi_rwlock)
#define XT_INDEX_HAVE_XLOCK(i, o)		TRUE
#elif defined(XT_TAB_ROW_USE_XSMUTEX)
#define XT_INDEX_LOCK_TYPE				XTMutexXSLockRec
#define XT_INDEX_INIT_LOCK(s, i)		xt_xsmutex_init_with_autoname(s, &(i)->mi_rwlock)
#define XT_INDEX_FREE_LOCK(s, i)		xt_xsmutex_free(s, &(i)->mi_rwlock)	
#define XT_INDEX_READ_LOCK(i, o)		xt_xsmutex_slock(&(i)->mi_rwlock, (o)->ot_thread->t_id)
#define XT_INDEX_WRITE_LOCK(i, o)		xt_xsmutex_xlock(&(i)->mi_rwlock, (o)->ot_thread->t_id)
#define XT_INDEX_UNLOCK(i, o)			xt_xsmutex_unlock(&(i)->mi_rwlock, (o)->ot_thread->t_id)
#define XT_INDEX_HAVE_XLOCK(i, o)		((i)->sxs_xlocker == (o)->ot_thread->t_id)
#elif defined(XT_INDEX_SPINXSLOCK)
#define XT_INDEX_LOCK_TYPE				XTSpinXSLockRec
#define XT_INDEX_INIT_LOCK(s, i)		xt_spinxslock_init_with_autoname(s, &(i)->mi_rwlock)
#define XT_INDEX_FREE_LOCK(s, i)		xt_spinxslock_free(s, &(i)->mi_rwlock)	
#define XT_INDEX_READ_LOCK(i, o)		xt_spinxslock_slock(&(i)->mi_rwlock, (o)->ot_thread->t_id)
#define XT_INDEX_WRITE_LOCK(i, o)		xt_spinxslock_xlock(&(i)->mi_rwlock, FALSE, (o)->ot_thread->t_id)
#define XT_INDEX_UNLOCK(i, o)			xt_spinxslock_unlock(&(i)->mi_rwlock, (o)->ot_thread->t_id)
#define XT_INDEX_HAVE_XLOCK(i, o)		((i)->mi_rwlock.nrw_xlocker == (o)->ot_thread->t_id)
#else
#error Please define the lock type
#endif

/* The R/W lock on the index is used as follows:
 * Read Lock - used for operations on the index that are not of a structural nature.
 * This includes any read operation and update operations that change an index
 * node.
 * Write lock - used to change the structure of the index. This includes adding
 * and deleting pages.
 */
typedef struct XTIndex {
	u_int				mi_index_no;				/* The index number (used by MySQL). */

	/* Protected by the mi_rwlock lock: */
	XT_INDEX_LOCK_TYPE	mi_rwlock;					/* This lock protects the structure of the index.
													 * Read lock - structure may not change, but pages may change.
													 * Write lock - structure of index may be changed.
													 */
	xtIndexNodeID		mi_root;					/* The index root node. */
	XTIndFreeListPtr	mi_free_list;				/* List of free pages for this index. */
	
	/* Protected by the mi_dirty_lock: */
	XTSpinLockRec		mi_dirty_lock;				/* Spin lock protecting the dirty & free lists. */
	struct XTIndBlock	*mi_dirty_list;				/* List of dirty pages for this index. */
	u_int				mi_dirty_blocks;			/* Count of the dirty blocks. */

	/* Index contants: */
	u_int				mi_flags;
	u_int				mi_key_size;
	u_int				mi_max_items;				/* The maximum number of items that can fit in a leaf node. */
	xtBool				mi_low_byte_first;
	xtBool				mi_fix_key;
	xtBool				mi_lazy_delete;				/* TRUE if index entries are "lazy deleted". */
	u_int				mi_single_type;				/* Used when the index contains a single field. */
	u_int				mi_select_total;
	XTScanBranchFunc	mi_scan_branch;
	XTPrevItemFunc		mi_prev_item;
	XTLastItemFunc		mi_last_item;
	XTSimpleCompFunc	mi_simple_comp_key;
	MX_BITMAP			mi_col_map;					/* Bit-map of columns in the index. */
	u_int				mi_subset_of;				/* Indicates if this index is a complete subset of someother index. */
	u_int				mi_seg_count;
	XTIndexSegRec		mi_seg[200];
} XTIndexRec, *XTIndexPtr;

#define XT_INDEX_OK				0
#define XT_INDEX_TOO_OLD		1
#define XT_INDEX_TOO_NEW		2
#define XT_INDEX_BAD_BLOCK		3
#define XT_INDEX_CORRUPTED		4
#define XT_INDEX_MISSING		5
#define XT_INDEX_NOT_RECOVERED	6

typedef void (*XTFreeDicFunc)(struct XTThread *self, struct XTDictionary *dic);

typedef struct XTDictionary {
	XTDDTable			*dic_table;					/* XT table information. */

	/* Table binary information. */
	u_int				dic_mysql_buf_size;			/* This is the size of the MySQL buffer (row size + null bytes). */
	u_int				dic_mysql_rec_size;			/* This is the size of the fixed length MySQL row. */
	u_int				dic_rec_size;				/* This is the size of the handle data file record. */
	xtBool				dic_rec_fixed;				/* TRUE if the record has a fixed length size. */
	u_int				dic_tab_flags;				/* Table flags: XT_TF_MEMORY_TABLE, XT_TF_MEMORY_TABLE. */
	xtWord8				dic_min_auto_inc;			/* The minimum auto-increment value. */
	xtWord8				dic_min_row_size;
	xtWord8				dic_max_row_size;
	xtWord8				dic_ave_row_size;
	xtWord8				dic_def_ave_row_size;		/* Defined row size set by the user. */
	u_int				dic_no_of_cols;				/* Number of columns. */
	u_int				dic_fix_col_count;			/* The number of columns always in the fixed part of a extended record. */
	u_int				dic_ind_cols_req;			/* The number of columns required to build all indexes. */
	xtWord8				dic_ind_rec_len;			/* Length of the record part that is needed for all index columns! */

	/* BLOB columns: */
	u_int				dic_blob_cols_req;			/* The number of the columns required to load all LONGBLOB columns. */
	u_int				dic_blob_count;
	Field				**dic_blob_cols;

	/* MySQL related information. NULL when no tables are open from MySQL side! */
	xtBool				dic_no_lazy_delete;			/* FALSE if lazy delete is OK. */
	u_int				dic_disable_index;			/* Non-zero if the index cannot be used. */
	u_int				dic_index_ver;				/* The version of the index. */
	u_int				dic_key_count;
	XTIndexPtr			*dic_keys;					/* MySQL/PBXT key description */
	STRUCT_TABLE		*dic_my_table;				/* MySQL table */
} XTDictionaryRec, *XTDictionaryPtr;

#define XT_DT_LOG_HEAD				0
#define XT_DT_INDEX_PAGE			1
#define XT_DT_FREE_LIST				2
#define XT_DT_HEADER				3
#define XT_DT_SHORT_IND_PAGE		4
#define XT_DT_MOD_IND_PAGE			5
#define XT_DT_MOD_IND_PAGE_HEAD		6
#define XT_DT_SET_PAGE_HEAD			7
#define XT_DT_2_MOD_IND_PAGE		8
#define XT_DT_MOD_IND_PAGE_EOB		9
#define XT_DT_MOD_IND_PAGE_HEAD_EOB	10
#define XT_DT_2_MOD_IND_PAGE_EOB	11

typedef struct XTIndLogHead {
	xtWord1					ilh_data_type;				/* XT_DT_LOG_HEAD */
	XTDiskValue4			ilh_tab_id_4;
	XTDiskValue4			ilh_log_eof_4;				/* The entire size of the log (0 if invalid!) */
} XTIndLogHeadDRec, *XTIndLogHeadDPtr;

typedef struct XTIndPageData {
	xtWord1					ild_data_type;
	XTDiskValue4			ild_page_id_4;
	xtWord1					ild_data[XT_VAR_LENGTH];
} XTIndPageDataDRec, *XTIndPageDataDPtr;

typedef struct XTIndHeadData {
	xtWord1					ilh_data_type;
	XTDiskValue2			ilh_head_size_2;
	xtWord1					ilh_data[XT_VAR_LENGTH];
} XTIndHeadDataDRec, *XTIndHeadDataDPtr;

typedef struct XTIndSetPageHeadData {
	xtWord1					ild_data_type;				/* XT_DT_SET_PAGE_HEAD */
	XTDiskValue4			ild_page_id_4;
	XTDiskValue2			ild_page_head_2;			/* The page header (first 2 bytes) */
} XTIndSetPageHeadDataDRec, *XTIndSetPageHeadDataDPtr;

typedef struct XTIndShortPageData {
	xtWord1					ild_data_type;				/* XT_DT_SHORT_IND_PAGE */
	XTDiskValue4			ild_page_id_4;
	XTDiskValue2			ild_size_2;					/* Size of the data. */
	xtWord1					ild_data[XT_VAR_LENGTH];
} XTIndShortPageDataDRec, *XTIndShortPageDataDPtr;

typedef struct XTIndModPageData {
	xtWord1					ild_data_type;				/* XT_DT_MOD_IND_PAGE */
	XTDiskValue4			ild_page_id_4;
	XTDiskValue2			ild_size_2;					/* Size of the data. */				
	XTDiskValue2			ild_offset_2;				/* Offset into the page. */
	xtWord1					ild_data[XT_VAR_LENGTH];
} XTIndModPageDataDRec, *XTIndModPageDataDPtr;

typedef struct XTIndModPageHeadData {
	xtWord1					ild_data_type;				/* XT_DT_MOD_IND_PAGE_HEAD/XT_DT_MOD_IND_PAGE_HEAD_EOB */
	XTDiskValue4			ild_page_id_4;
	XTDiskValue2			ild_size_2;					/* Size of the data. */
	XTDiskValue2			ild_offset_2;				/* Offset into the page. */
	XTDiskValue2			ild_page_head_2;			/* The page header (first 2 bytes) */
	xtWord1					ild_data[XT_VAR_LENGTH];
} XTIndModPageHeadDataDRec, *XTIndModPageHeadDataDPtr;

typedef struct XTIndDoubleModPageData {
	xtWord1					dld_data_type;				/* XT_DT_2_MOD_IND_PAGE/XT_DT_2_MOD_IND_PAGE_EOB */
	XTDiskValue4			dld_page_id_4;
	XTDiskValue2			dld_size1_2;				/* Size of the first data block, offset 0. */				
	XTDiskValue2			dld_offset2_2;				/* Offset of second data block. */
	XTDiskValue2			dld_size2_2;				/* Size of the second data block. */				
	xtWord1					dld_data[XT_VAR_LENGTH];
} XTIndDoubleModPageDataDRec, *XTIndDoubleModPageDataDPtr;

typedef struct XTIndexLog {
	struct XTIndexLogPool	*il_pool;
	struct XTIndexLog		*il_next_in_pool;

	xtLogID					il_log_id;						/* The ID of the data log. */
	XTOpenFilePtr			il_of;
	size_t					il_buffer_size;
	xtWord1					*il_buffer;

	xtTableID				il_tab_id;
	off_t					il_log_eof;	
	size_t					il_buffer_len;
	off_t					il_buffer_offset;
	XTSpinLockRec			il_write_lock;
	size_t					il_bytes_written;

	xtBool					il_reset(struct XTOpenTable *ot);
	void					il_close(xtBool delete_it);
	void					il_release();
	xtBool					il_data_written();

	xtBool					il_write_byte(struct XTOpenTable *ot, xtWord1 val);
	xtBool					il_write_word4(struct XTOpenTable *ot, xtWord4 value);
	xtBool					il_write_block(struct XTOpenTable *ot, struct XTIndBlock *block);
	xtBool					il_write_free_list(struct XTOpenTable *ot, u_int free_count, XTIndFreeListPtr free_list);
	xtBool					il_require_space(size_t bytes, XTThreadPtr thread);
	xtBool					il_write_header(struct XTOpenTable *ot, size_t head_size, xtWord1 *head_data);
	xtBool					il_flush(struct XTOpenTable *ot);
	xtBool					il_apply_log_write(struct XTOpenTable *ot);
	xtBool					il_apply_log_flush(struct XTOpenTable *ot);
	inline xtBool			il_pwrite_file(struct XTOpenTable *ot, off_t offs, size_t siz, void *dat);
	inline xtBool			il_flush_file(struct XTOpenTable *ot);
	
	xtBool					il_open_table(struct XTOpenTable **ot);
	void					il_close_table(struct XTOpenTable *ot);
} XTIndexLogRec, *XTIndexLogPtr;

typedef struct XTIndexLogPool {
	struct XTDatabase		*ilp_db;
	size_t					ilp_log_buffer_size;
	u_int					il_pool_count;
	XTIndexLogPtr			ilp_log_pool;
	xt_mutex_type			ilp_lock;						/* The public pool lock. */
	xtLogID					ilp_next_log_id;

	void					ilp_init(struct XTThread *self, struct XTDatabase *db, size_t log_buffer_size);
	void					ilp_close(struct XTThread *self, xtBool lock);
	void					ilp_exit(struct XTThread *self);
	void					ilp_name(size_t size, char *path, xtLogID log_id);

	xtBool					ilp_open_log(XTIndexLogPtr *il, xtLogID log_id, xtBool excl, XTThreadPtr thread);

	xtBool					ilp_get_log(XTIndexLogPtr *il, XTThreadPtr thread);
	void					ilp_release_log(XTIndexLogPtr il);
} XTIndexLogPoolRec, *XTIndexLogPoolPtr;

class XTFlushIndexTask : public XTLockTask {
	public:
	XTFlushIndexTask() : XTLockTask(),
		fit_table(NULL),
		fit_checkpoint(FALSE),
		fit_dirty_blocks(0),
		fit_blocks_flushed(0)
	{ }

	virtual xtBool	tk_task(XTThreadPtr thread);
	virtual void	tk_reference();
	virtual void	tk_release();

	struct XTTable		*fit_table;
	xtBool				fit_checkpoint;
	u_int				fit_dirty_blocks;			/* The number of dirty blocks being flushed! */
	u_int				fit_blocks_flushed;
};

/* A record reference consists of a record ID and a row ID: */
inline void xt_get_record_ref(register xtWord1 *item, xtRecordID *rec_id, xtRowID *row_id) {
	*rec_id = XT_GET_DISK_4(item);
	item += 4;
	*row_id = XT_GET_DISK_4(item);
}

inline void xt_get_res_record_ref(register xtWord1 *item, register XTIdxResultRec *result) {
	result->sr_rec_id = XT_GET_DISK_4(item);
	item += 4;
	result->sr_row_id = XT_GET_DISK_4(item);
}

inline void xt_set_record_ref(register xtWord1 *item, xtRecordID rec_id, xtRowID row_id) {
	XT_SET_DISK_4(item, rec_id);
	item += 4;
	XT_SET_DISK_4(item, row_id);
}

inline void xt_set_val_record_ref(register xtWord1 *item, register XTIdxKeyValuePtr value) {
	XT_SET_DISK_4(item, value->sv_rec_id);
	item += 4;
	XT_SET_DISK_4(item, value->sv_row_id);
}

xtBool	xt_idx_insert(struct XTOpenTable *ot, struct XTIndex *ind, xtRowID row_id, xtRecordID rec_id, xtWord1 *rec_buf, xtWord1 *bef_buf, xtBool allow_dups);
xtBool	xt_idx_delete(struct XTOpenTable *ot, struct XTIndex *ind, xtRecordID rec_id, xtWord1 *rec_buf);
xtBool	xt_idx_update_row_id(struct XTOpenTable *ot, struct XTIndex *ind, xtRecordID rec_id, xtRowID row_id, xtWord1 *rec_buf);
void	xt_idx_prep_key(struct XTIndex *ind, register XTIdxSearchKeyPtr search_key, int flags, xtWord1 *in_key_buf, size_t in_key_length);
xtBool	xt_idx_research(struct XTOpenTable *ot, struct XTIndex *ind);
xtBool	xt_idx_search(struct XTOpenTable *ot, struct XTIndex *ind, register XTIdxSearchKeyPtr search_key);
xtBool	xt_idx_search_prev(struct XTOpenTable *ot, struct XTIndex *ind, register XTIdxSearchKeyPtr search_key);
xtBool	xt_idx_next(register struct XTOpenTable *ot, register struct XTIndex *ind, register XTIdxSearchKeyPtr search_key);
xtBool	xt_idx_prev(register struct XTOpenTable *ot, register struct XTIndex *ind, register XTIdxSearchKeyPtr search_key);
xtBool	xt_idx_read(struct XTOpenTable *ot, struct XTIndex *ind, xtWord1 *rec_buf);
void	xt_ind_set_index_selectivity(struct XTOpenTable *ot, XTThreadPtr thread);
void	xt_check_indices(struct XTOpenTable *ot);
void	xt_load_indices(XTThreadPtr self, struct XTOpenTable *ot);
void	xt_ind_count_deleted_items(struct XTTable *ot, struct XTIndex *ind, struct XTIndBlock *block);
xtBool	xt_async_flush_indices(struct XTTable *tab, xtBool notify_complete, xtBool notify_before_write, struct XTThread *thread);
xtBool	xt_flush_indices(struct XTOpenTable *ot, off_t *bytes_flushed, xtBool have_table_lock, XTFlushIndexTask *ft);
void	xt_ind_track_dump_block(struct XTTable *tab, xtIndexNodeID address);

#define XT_S_MODE_MATCH		0
#define XT_S_MODE_NEXT		1
#define XT_S_MODE_PREV		2
xtBool	xt_idx_match_search(struct XTOpenTable *ot, struct XTIndex *ind, register XTIdxSearchKeyPtr search_key, xtWord1 *buf, int mode);

int		xt_compare_2_int4(XTIndexPtr ind, uint key_length, xtWord1 *key_value, xtWord1 *b_value);
int		xt_compare_3_int4(XTIndexPtr ind, uint key_length, xtWord1 *key_value, xtWord1 *b_value);
void	xt_scan_branch_single(struct XTTable *tab, XTIndexPtr ind, XTIdxBranchDPtr branch, register XTIdxKeyValuePtr value, register XTIdxResultRec *result);
void	xt_scan_branch_fix(struct XTTable *tab, XTIndexPtr ind, XTIdxBranchDPtr branch, register XTIdxKeyValuePtr value, register XTIdxResultRec *result);
void	xt_scan_branch_fix_simple(struct XTTable *tab, XTIndexPtr ind, XTIdxBranchDPtr branch, register XTIdxKeyValuePtr value, register XTIdxResultRec *result);
void	xt_scan_branch_var(struct XTTable *tab, XTIndexPtr ind, XTIdxBranchDPtr branch, register XTIdxKeyValuePtr value, register XTIdxResultRec *result);

void	xt_prev_branch_item_fix(struct XTTable *tab, XTIndexPtr ind, XTIdxBranchDPtr branch, register XTIdxResultRec *result);
void	xt_prev_branch_item_var(struct XTTable *tab, XTIndexPtr ind, XTIdxBranchDPtr branch, register XTIdxResultRec *result);

void	xt_last_branch_item_fix(struct XTTable *tab, XTIndexPtr ind, XTIdxBranchDPtr branch, register XTIdxResultPtr result);
void	xt_last_branch_item_var(struct XTTable *tab, XTIndexPtr ind, XTIdxBranchDPtr branch, register XTIdxResultPtr result);
xtBool	xt_idx_lazy_delete_on_leaf(XTIndexPtr ind, struct XTIndBlock *block, xtWord2 branch_size);

//#define TRACK_ACTIVITY
#ifdef TRACK_ACTIVITY

#define TRACK_BLOCK_ALLOC(x)	track_work(xt_ind_offset_to_node(tab, x), "A")
#define TRACK_BLOCK_FREE(x)		track_work(xt_ind_offset_to_node(ot->ot_table, x), "-")
#define TRACK_BLOCK_SPLIT(x)	track_work(xt_ind_offset_to_node(ot->ot_table, x), "/")
#define TRACK_BLOCK_WRITE(x)	track_work(xt_ind_offset_to_node(ot->ot_table, x), "w")
#define TRACK_BLOCK_FLUSH_N(x)	track_work(x, "F")
#define TRACK_BLOCK_TO_FLUSH(x)	track_work(x, "f")

xtPublic void track_work(u_int block, char *what);
#else

#define TRACK_BLOCK_ALLOC(x)
#define TRACK_BLOCK_FREE(x)
#define TRACK_BLOCK_SPLIT(x)
#define TRACK_BLOCK_WRITE(x)
#define TRACK_BLOCK_FLUSH_N(x)
#define TRACK_BLOCK_TO_FLUSH(x)

#endif

#endif

