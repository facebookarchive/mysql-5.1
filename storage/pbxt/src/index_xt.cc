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

#include "xt_config.h"

#ifdef DRIZZLED
#include <bitset>
#endif

#include <string.h>
#include <stdio.h>
#include <stddef.h>
#ifndef XT_WIN
#include <strings.h>
#endif
#include <zlib.h>
#include <bzlib.h>

#ifdef DRIZZLED
#include <drizzled/base.h>
#else
#include "mysql_priv.h"
#endif

#include "pthread_xt.h"
#include "memory_xt.h"
#include "index_xt.h"
#include "heap_xt.h"
#include "database_xt.h"
#include "strutil_xt.h"
#include "cache_xt.h"
#include "myxt_xt.h"
#include "trace_xt.h"
#include "table_xt.h"

#ifdef DEBUG
#define MAX_SEARCH_DEPTH			32
//#define CHECK_AND_PRINT
//#define CHECK_NODE_REFERENCE
//#define TRACE_FLUSH_INDEX
//#define CHECK_PRINTS_RECORD_REFERENCES
//#define DO_COMP_TEST
#define DUMP_INDEX
#else
#define MAX_SEARCH_DEPTH			100
#endif

//#define TRACE_FLUSH_TIMES

typedef struct IdxStackItem {
	XTIdxItemRec			i_pos;
	xtIndexNodeID			i_branch;
} IdxStackItemRec, *IdxStackItemPtr;

typedef struct IdxBranchStack {
	int						s_top;
	IdxStackItemRec			s_elements[MAX_SEARCH_DEPTH];
} IdxBranchStackRec, *IdxBranchStackPtr;

#ifdef DEBUG
#ifdef TEST_CODE
static void idx_check_on_key(XTOpenTablePtr ot);
#endif
static u_int idx_check_index(XTOpenTablePtr ot, XTIndexPtr ind, xtBool with_lock);
#endif

static xtBool idx_insert_node(XTOpenTablePtr ot, XTIndexPtr ind, IdxBranchStackPtr stack, xtBool last_item, XTIdxKeyValuePtr key_value, xtIndexNodeID branch);
static xtBool idx_remove_lazy_deleted_item_in_node(XTOpenTablePtr ot, XTIndexPtr ind, xtIndexNodeID current, XTIndReferencePtr iref, XTIdxKeyValuePtr key_value);

#ifdef XT_TRACK_INDEX_UPDATES

static xtBool ind_track_write(struct XTOpenTable *ot, struct XTIndex *ind, xtIndexNodeID offset, size_t size, xtWord1 *data)
{
	ot->ot_ind_reads++;
	return xt_ind_write(ot, ind, offset, size, data);
}

#define XT_IND_WRITE					ind_track_write

#else

#define XT_IND_WRITE					xt_ind_write

#endif


#ifdef CHECK_NODE_REFERENCE
#define IDX_GET_NODE_REF(t, x, o)		idx_get_node_ref(t, x, o)
#else
#define IDX_GET_NODE_REF(t, x, o)		XT_GET_NODE_REF(t, (x) - (o))
#endif

/*
 * -----------------------------------------------------------------------
 * DEBUG ACTIVITY
 */

//#define TRACK_ACTIVITY

#ifdef TRACK_ACTIVITY
#define TRACK_MAX_BLOCKS			2000

typedef struct TrackBlock {
	xtWord1				exists;
	char				*activity;
} TrackBlockRec, *TrackBlockPtr;

TrackBlockRec		blocks[TRACK_MAX_BLOCKS];

xtPublic void track_work(u_int block, char *what)
{
	int len = 0, len2;

	ASSERT_NS(block > 0 && block <= TRACK_MAX_BLOCKS);
	block--;
	if (blocks[block].activity)
		len = strlen(blocks[block].activity);
	len2 = strlen(what);
	xt_realloc_ns((void **) &blocks[block].activity, len + len2 + 1);
	memcpy(blocks[block].activity + len, what, len2 + 1);
}

static void track_block_exists(xtIndexNodeID block)
{
	if (XT_NODE_ID(block) > 0 && XT_NODE_ID(block) <= TRACK_MAX_BLOCKS)
		blocks[XT_NODE_ID(block)-1].exists = TRUE;
}

static void track_reset_missing()
{
	for (u_int i=0; i<TRACK_MAX_BLOCKS; i++)
		blocks[i].exists = FALSE;
}

static void track_dump_missing(xtIndexNodeID eof_block)
{
	for (u_int i=0; i<XT_NODE_ID(eof_block)-1; i++) {
		if (!blocks[i].exists)
			printf("block missing = %04d %s\n", i+1, blocks[i].activity);
	}
}

static void track_dump_all(u_int max_block)
{
	for (u_int i=0; i<max_block; i++) {
		if (blocks[i].exists)
			printf(" %04d %s\n", i+1, blocks[i].activity);
		else
			printf("-%04d %s\n", i+1, blocks[i].activity);
	}
}

#endif

xtPublic void xt_ind_track_dump_block(XTTableHPtr XT_UNUSED(tab), xtIndexNodeID XT_UNUSED(address))
{
#ifdef TRACK_ACTIVITY
	u_int i = XT_NODE_ID(address)-1;

	printf("BLOCK %04d %s\n", i+1, blocks[i].activity);
#endif
}

#ifdef CHECK_NODE_REFERENCE
static xtIndexNodeID idx_get_node_ref(XTTableHPtr tab, xtWord1 *ref, u_int node_ref_size)
{
	xtIndexNodeID node;

	/* Node is invalid by default: */
	XT_NODE_ID(node) = 0xFFFFEEEE;
	if (node_ref_size) {
		ref -= node_ref_size;
		node = XT_RET_NODE_ID(XT_GET_DISK_4(ref));
		if (node >= tab->tab_ind_eof) {
			xt_register_taberr(XT_REG_CONTEXT, XT_ERR_INDEX_CORRUPTED, tab->tab_name);
		}
	}
	return node;
}
#endif

/*
 * -----------------------------------------------------------------------
 * Stack functions
 */

static void idx_newstack(IdxBranchStackPtr stack)
{
	stack->s_top = 0;
}

static xtBool idx_push(IdxBranchStackPtr stack, xtIndexNodeID n, XTIdxItemPtr pos)
{
	if (stack->s_top == MAX_SEARCH_DEPTH) {
		xt_register_error(XT_REG_CONTEXT, XT_ERR_STACK_OVERFLOW, 0, "Index node stack overflow");
		return FAILED;
	}
	stack->s_elements[stack->s_top].i_branch = n;
	if (pos)
		stack->s_elements[stack->s_top].i_pos = *pos;
	stack->s_top++;
	return OK;
}

static IdxStackItemPtr idx_pop(IdxBranchStackPtr stack)
{
	if (stack->s_top == 0)
		return NULL;
	stack->s_top--;
	return &stack->s_elements[stack->s_top];
}

static IdxStackItemPtr idx_top(IdxBranchStackPtr stack)
{
	if (stack->s_top == 0)
		return NULL;
	return &stack->s_elements[stack->s_top-1];
}

/*
 * -----------------------------------------------------------------------
 * Allocation of nodes
 */

/*
 * Allocating and freeing blocks for an index is safe because this is a structural
 * change which requires an exclusive lock on the index!
 */
static xtBool idx_new_branch(XTOpenTablePtr ot, XTIndexPtr ind, xtIndexNodeID *address)
{
	register XTTableHPtr	tab;
	xtIndexNodeID			wrote_pos;
	XTIndFreeBlockRec		free_block;
	XTIndFreeListPtr		list_ptr;

	tab = ot->ot_table;

	//ASSERT_NS(XT_INDEX_HAVE_XLOCK(ind, ot));
	if (ind->mi_free_list && ind->mi_free_list->fl_free_count) {
		ind->mi_free_list->fl_free_count--;
		*address = ind->mi_free_list->fl_page_id[ind->mi_free_list->fl_free_count];
		TRACK_BLOCK_ALLOC(*address);
		return OK;
	}

	xt_lock_mutex_ns(&tab->tab_ind_lock);

	/* Check the cached free list: */
	while ((list_ptr = tab->tab_ind_free_list)) {
		if (list_ptr->fl_start < list_ptr->fl_free_count) {
			wrote_pos = list_ptr->fl_page_id[list_ptr->fl_start];
			list_ptr->fl_start++;
			xt_unlock_mutex_ns(&tab->tab_ind_lock);
			*address = wrote_pos;
			TRACK_BLOCK_ALLOC(wrote_pos);
			return OK;
		}
		tab->tab_ind_free_list = list_ptr->fl_next_list;
		xt_free_ns(list_ptr);
	}

	if ((XT_NODE_ID(wrote_pos) = XT_NODE_ID(tab->tab_ind_free))) {
		/* Use the block on the free list: */
		if (!xt_ind_read_bytes(ot, ind, wrote_pos, sizeof(XTIndFreeBlockRec), (xtWord1 *) &free_block))
			goto failed;
		XT_NODE_ID(tab->tab_ind_free) = (xtIndexNodeID) XT_GET_DISK_8(free_block.if_next_block_8);
		xt_unlock_mutex_ns(&tab->tab_ind_lock);
		*address = wrote_pos;
		TRACK_BLOCK_ALLOC(wrote_pos);
		return OK;
	}

	/* PMC - Dont allow overflow! */
	if (XT_NODE_ID(tab->tab_ind_eof) >= 0xFFFFFFF) {
		xt_register_ixterr(XT_REG_CONTEXT, XT_ERR_INDEX_FILE_TO_LARGE, xt_file_path(ot->ot_ind_file));
		goto failed;
	}
	*address = tab->tab_ind_eof;
	XT_NODE_ID(tab->tab_ind_eof)++;
	xt_unlock_mutex_ns(&tab->tab_ind_lock);
	TRACK_BLOCK_ALLOC(*address);
	return OK;

	failed:
	xt_unlock_mutex_ns(&tab->tab_ind_lock);
	return FAILED;
}

/* Add the block to the private free list of the index.
 * On flush, this list will be transfered to the global list.
 */
static xtBool idx_free_branch(XTOpenTablePtr ot, XTIndexPtr ind, xtIndexNodeID node_id)
{
	register u_int		count;
	register u_int		i;
	register u_int		guess;

	TRACK_BLOCK_FREE(node_id);
	//ASSERT_NS(XT_INDEX_HAVE_XLOCK(ind, ot));
	if (!ind->mi_free_list) {
		count = 0;
		if (!(ind->mi_free_list = (XTIndFreeListPtr) xt_calloc_ns(offsetof(XTIndFreeListRec, fl_page_id) + 10 * sizeof(xtIndexNodeID))))
			return FAILED;
	}
	else {
		count = ind->mi_free_list->fl_free_count;
		if (!xt_realloc_ns((void **) &ind->mi_free_list, offsetof(XTIndFreeListRec, fl_page_id) + (count + 1) * sizeof(xtIndexNodeID)))
			return FAILED;
	}
 
	i = 0;
	while (i < count) {
		guess = (i + count - 1) >> 1;
		if (XT_NODE_ID(node_id) == XT_NODE_ID(ind->mi_free_list->fl_page_id[guess])) {
			// Should not happen...
			ASSERT_NS(FALSE);
			return OK;
		}
		if (XT_NODE_ID(node_id) < XT_NODE_ID(ind->mi_free_list->fl_page_id[guess]))
			count = guess;
		else
			i = guess + 1;
	}

	/* Insert at position i */
	memmove(ind->mi_free_list->fl_page_id + i + 1, ind->mi_free_list->fl_page_id + i, (ind->mi_free_list->fl_free_count - i) * sizeof(xtIndexNodeID));
	ind->mi_free_list->fl_page_id[i] = node_id;
	ind->mi_free_list->fl_free_count++;

	/* Set the cache page to clean: */
	return xt_ind_free_block(ot, ind, node_id);
}

/*
 * -----------------------------------------------------------------------
 * Simple compare functions
 */

xtPublic int xt_compare_2_int4(XTIndexPtr XT_UNUSED(ind), uint key_length, xtWord1 *key_value, xtWord1 *b_value)
{
	int r;

	ASSERT_NS(key_length == 4 || key_length == 8);
	r = (xtInt4) XT_GET_DISK_4(key_value) - (xtInt4) XT_GET_DISK_4(b_value);
	if (r == 0 && key_length > 4) {
		key_value += 4;
		b_value += 4;
		r = (xtInt4) XT_GET_DISK_4(key_value) - (xtInt4) XT_GET_DISK_4(b_value);
	}
	return r;
}

xtPublic int xt_compare_3_int4(XTIndexPtr XT_UNUSED(ind), uint key_length, xtWord1 *key_value, xtWord1 *b_value)
{
	int r;

	ASSERT_NS(key_length == 4 || key_length == 8 || key_length == 12);
	r = (xtInt4) XT_GET_DISK_4(key_value) - (xtInt4) XT_GET_DISK_4(b_value);
	if (r == 0 && key_length > 4) {
		key_value += 4;
		b_value += 4;
		r = (xtInt4) XT_GET_DISK_4(key_value) - (xtInt4) XT_GET_DISK_4(b_value);
		if (r == 0 && key_length > 8) {
			key_value += 4;
			b_value += 4;
			r = (xtInt4) XT_GET_DISK_4(key_value) - (xtInt4) XT_GET_DISK_4(b_value);
		}
	}
	return r;
}

/*
 * -----------------------------------------------------------------------
 * Tree branch sanning (searching nodes and leaves)
 */

xtPublic void xt_scan_branch_single(struct XTTable *XT_UNUSED(tab), XTIndexPtr ind, XTIdxBranchDPtr branch, register XTIdxKeyValuePtr value, register XTIdxResultRec *result)
{
	XT_NODE_TEMP;
	u_int				branch_size;
	u_int				node_ref_size;
	u_int				full_item_size;
	int					search_flags;
	register xtWord1	*base;
	register u_int		i;
	register xtWord1	*bitem;
	u_int				total_count;

	branch_size = XT_GET_DISK_2(branch->tb_size_2);
	node_ref_size = XT_IS_NODE(branch_size) ? XT_NODE_REF_SIZE : 0;

	result->sr_found = FALSE;
	result->sr_duplicate = FALSE;
	result->sr_item.i_total_size = XT_GET_BRANCH_DATA_SIZE(branch_size);
	ASSERT_NS((int) result->sr_item.i_total_size >= 0 && result->sr_item.i_total_size <= XT_INDEX_PAGE_SIZE-2);

	result->sr_item.i_item_size = ind->mi_key_size + XT_RECORD_REF_SIZE;
	full_item_size = result->sr_item.i_item_size + node_ref_size;
	result->sr_item.i_node_ref_size = node_ref_size;

	search_flags = value->sv_flags;
	base = branch->tb_data + node_ref_size;
	total_count = (result->sr_item.i_total_size - node_ref_size) / full_item_size;
	if (search_flags & XT_SEARCH_FIRST_FLAG)
		i = 0;
	else if (search_flags & XT_SEARCH_AFTER_LAST_FLAG)
		i = total_count;
	else {
		register u_int		guess;
		register u_int		count;
		register xtInt4		r;
		xtRecordID			key_record;

		key_record = value->sv_rec_id;
		count = total_count;

		ASSERT_NS(ind);
		i = 0;
		while (i < count) {
			guess = (i + count - 1) >> 1;

			bitem = base + guess * full_item_size;

			switch (ind->mi_single_type) {
				case HA_KEYTYPE_LONG_INT: {
					register xtInt4 a, b;
					
					a = XT_GET_DISK_4(value->sv_key);
					b = XT_GET_DISK_4(bitem);
					r = (a < b) ? -1 : (a == b ? 0 : 1);
					break;
				}
				case HA_KEYTYPE_ULONG_INT: {
					register xtWord4 a, b;
					
					a = XT_GET_DISK_4(value->sv_key);
					b = XT_GET_DISK_4(bitem);
					r = (a < b) ? -1 : (a == b ? 0 : 1);
					break;
				}
				default:
					/* Should not happen: */
					r = 1;
					break;
			}
			if (r == 0) {
				if (search_flags & XT_SEARCH_WHOLE_KEY) {
					xtRecordID	item_record;
					xtRowID		row_id;
					
					xt_get_record_ref(bitem + ind->mi_key_size, &item_record, &row_id);

					/* This should not happen because we should never
					 * try to insert the same record twice into the 
					 * index!
					 */
					result->sr_duplicate = TRUE;
					if (key_record == item_record) {
						result->sr_found = TRUE;
						result->sr_rec_id = item_record;
						result->sr_row_id = row_id;
						result->sr_branch = IDX_GET_NODE_REF(tab, bitem, node_ref_size);
						result->sr_item.i_item_offset = node_ref_size + guess * full_item_size;
						return;
					}
					if (key_record < item_record)
						r = -1;
					else
						r = 1;
				}
				else {
					result->sr_found = TRUE;
					/* -1 causes a search to the beginning of the duplicate list of keys.
					 * 1 causes a search to just after the key.
				 	*/
					if (search_flags & XT_SEARCH_AFTER_KEY)
						r = 1;
					else
						r = -1;
				}
			}

			if (r < 0)
				count = guess;
			else
				i = guess + 1;
		}
	}

	bitem = base + i * full_item_size;
	xt_get_res_record_ref(bitem + ind->mi_key_size, result);
	result->sr_branch = IDX_GET_NODE_REF(tab, bitem, node_ref_size);			/* Only valid if this is a node. */
	result->sr_item.i_item_offset = node_ref_size + i * full_item_size;
#ifdef IND_SKEW_SPLIT_ON_APPEND
	if (i != total_count)
		result->sr_last_item = FALSE;
#endif
}

/*
 * We use a special binary search here. It basically assumes that the values
 * in the index are not unique.
 *
 * Even if they are unique, when we search for part of a key, then it is
 * effectively the case.
 *
 * So in the situation where we find duplicates in the index we usually
 * want to position ourselves at the beginning of the duplicate list.
 *
 * Alternatively a search can find the position just after a given key.
 *
 * To achieve this we make the following modifications:
 * - The result of the comparison is always returns 1 or -1. We only stop
 *   the search early in the case an exact match when inserting (but this
 *   should not happen anyway).
 * - The search never actually fails, but sets 'found' to TRUE if it
 *   sees the search key in the index.
 *
 * If the search value exists in the index we know that
 * this method will take us to the first occurrence of the key in the
 * index (in the case of -1) or to the first value after the
 * the search key in the case of 1.
 */
xtPublic void xt_scan_branch_fix(struct XTTable *XT_UNUSED(tab), XTIndexPtr ind, XTIdxBranchDPtr branch, register XTIdxKeyValuePtr value, register XTIdxResultRec *result)
{
	XT_NODE_TEMP;
	u_int				branch_size;
	u_int				node_ref_size;
	u_int				full_item_size;
	int					search_flags;
	xtWord1				*base;
	register u_int		i;
	xtWord1				*bitem;
	u_int				total_count;

	branch_size = XT_GET_DISK_2(branch->tb_size_2);
	node_ref_size = XT_IS_NODE(branch_size) ? XT_NODE_REF_SIZE : 0;

	result->sr_found = FALSE;
	result->sr_duplicate = FALSE;
	result->sr_item.i_total_size = XT_GET_BRANCH_DATA_SIZE(branch_size);
	ASSERT_NS((int) result->sr_item.i_total_size >= 0 && result->sr_item.i_total_size <= XT_INDEX_PAGE_SIZE-2);

	result->sr_item.i_item_size = ind->mi_key_size + XT_RECORD_REF_SIZE;
	full_item_size = result->sr_item.i_item_size + node_ref_size;
	result->sr_item.i_node_ref_size = node_ref_size;

	search_flags = value->sv_flags;
	base = branch->tb_data + node_ref_size;
	total_count = (result->sr_item.i_total_size - node_ref_size) / full_item_size;
	if (search_flags & XT_SEARCH_FIRST_FLAG)
		i = 0;
	else if (search_flags & XT_SEARCH_AFTER_LAST_FLAG)
		i = total_count;
	else {
		register u_int		guess;
		register u_int		count;
		xtRecordID			key_record;
		int					r;

		key_record = value->sv_rec_id;
		count = total_count;

		ASSERT_NS(ind);
		i = 0;
		while (i < count) {
			guess = (i + count - 1) >> 1;

			bitem = base + guess * full_item_size;

			r = myxt_compare_key(ind, search_flags, value->sv_length, value->sv_key, bitem);

			if (r == 0) {
				if (search_flags & XT_SEARCH_WHOLE_KEY) {
					xtRecordID	item_record;
					xtRowID		row_id;

					xt_get_record_ref(bitem + ind->mi_key_size, &item_record, &row_id);

					/* This should not happen because we should never
					 * try to insert the same record twice into the 
					 * index!
					 */
					result->sr_duplicate = TRUE;
					if (key_record == item_record) {
						result->sr_found = TRUE;
						result->sr_rec_id = item_record;
						result->sr_row_id = row_id;
						result->sr_branch = IDX_GET_NODE_REF(tab, bitem, node_ref_size);
						result->sr_item.i_item_offset = node_ref_size + guess * full_item_size;
						return;
					}
					if (key_record < item_record)
						r = -1;
					else
						r = 1;
				}
				else {
					result->sr_found = TRUE;
					/* -1 causes a search to the beginning of the duplicate list of keys.
					 * 1 causes a search to just after the key.
				 	*/
					if (search_flags & XT_SEARCH_AFTER_KEY)
						r = 1;
					else
						r = -1;
				}
			}

			if (r < 0)
				count = guess;
			else
				i = guess + 1;
		}
	}

	bitem = base + i * full_item_size;
	xt_get_res_record_ref(bitem + ind->mi_key_size, result);
	result->sr_branch = IDX_GET_NODE_REF(tab, bitem, node_ref_size);			/* Only valid if this is a node. */
	result->sr_item.i_item_offset = node_ref_size + i * full_item_size;
#ifdef IND_SKEW_SPLIT_ON_APPEND
	if (i != total_count)
		result->sr_last_item = FALSE;
#endif
}

xtPublic void xt_scan_branch_fix_simple(struct XTTable *XT_UNUSED(tab), XTIndexPtr ind, XTIdxBranchDPtr branch, register XTIdxKeyValuePtr value, register XTIdxResultRec *result)
{
	XT_NODE_TEMP;
	u_int				branch_size;
	u_int				node_ref_size;
	u_int				full_item_size;
	int					search_flags;
	xtWord1				*base;
	register u_int		i;
	xtWord1				*bitem;
	u_int				total_count;

	branch_size = XT_GET_DISK_2(branch->tb_size_2);
	node_ref_size = XT_IS_NODE(branch_size) ? XT_NODE_REF_SIZE : 0;

	result->sr_found = FALSE;
	result->sr_duplicate = FALSE;
	result->sr_item.i_total_size = XT_GET_BRANCH_DATA_SIZE(branch_size);
	ASSERT_NS((int) result->sr_item.i_total_size >= 0 && result->sr_item.i_total_size <= XT_INDEX_PAGE_SIZE-2);

	result->sr_item.i_item_size = ind->mi_key_size + XT_RECORD_REF_SIZE;
	full_item_size = result->sr_item.i_item_size + node_ref_size;
	result->sr_item.i_node_ref_size = node_ref_size;

	search_flags = value->sv_flags;
	base = branch->tb_data + node_ref_size;
	total_count = (result->sr_item.i_total_size - node_ref_size) / full_item_size;
	if (search_flags & XT_SEARCH_FIRST_FLAG)
		i = 0;
	else if (search_flags & XT_SEARCH_AFTER_LAST_FLAG)
		i = total_count;
	else {
		register u_int		guess;
		register u_int		count;
		xtRecordID			key_record;
		int					r;

		key_record = value->sv_rec_id;
		count = total_count;

		ASSERT_NS(ind);
		i = 0;
		while (i < count) {
			guess = (i + count - 1) >> 1;

			bitem = base + guess * full_item_size;

			r = ind->mi_simple_comp_key(ind, value->sv_length, value->sv_key, bitem);

			if (r == 0) {
				if (search_flags & XT_SEARCH_WHOLE_KEY) {
					xtRecordID	item_record;
					xtRowID		row_id;

					xt_get_record_ref(bitem + ind->mi_key_size, &item_record, &row_id);

					/* This should not happen because we should never
					 * try to insert the same record twice into the 
					 * index!
					 */
					result->sr_duplicate = TRUE;
					if (key_record == item_record) {
						result->sr_found = TRUE;
						result->sr_rec_id = item_record;
						result->sr_row_id = row_id;
						result->sr_branch = IDX_GET_NODE_REF(tab, bitem, node_ref_size);
						result->sr_item.i_item_offset = node_ref_size + guess * full_item_size;
						return;
					}
					if (key_record < item_record)
						r = -1;
					else
						r = 1;
				}
				else {
					result->sr_found = TRUE;
					/* -1 causes a search to the beginning of the duplicate list of keys.
					 * 1 causes a search to just after the key.
				 	*/
					if (search_flags & XT_SEARCH_AFTER_KEY)
						r = 1;
					else
						r = -1;
				}
			}

			if (r < 0)
				count = guess;
			else
				i = guess + 1;
		}
	}

	bitem = base + i * full_item_size;
	xt_get_res_record_ref(bitem + ind->mi_key_size, result);
	result->sr_branch = IDX_GET_NODE_REF(tab, bitem, node_ref_size);			/* Only valid if this is a node. */
	result->sr_item.i_item_offset = node_ref_size + i * full_item_size;
#ifdef IND_SKEW_SPLIT_ON_APPEND
	if (i != total_count)
		result->sr_last_item = FALSE;
#endif
}

/*
 * Variable length key values are stored as a sorted list. Since each list item has a variable length, we
 * must scan the list sequentially in order to find a key.
 */
xtPublic void xt_scan_branch_var(struct XTTable *XT_UNUSED(tab), XTIndexPtr ind, XTIdxBranchDPtr branch, register XTIdxKeyValuePtr value, register XTIdxResultRec *result)
{
	XT_NODE_TEMP;
	u_int			branch_size;
	u_int			node_ref_size;
	int				search_flags;
	xtWord1			*base;
	xtWord1			*bitem;
	u_int			ilen;
	xtWord1			*bend;

	branch_size = XT_GET_DISK_2(branch->tb_size_2);
	node_ref_size = XT_IS_NODE(branch_size) ? XT_NODE_REF_SIZE : 0;

	result->sr_found = FALSE;
	result->sr_duplicate = FALSE;
	result->sr_item.i_total_size = XT_GET_BRANCH_DATA_SIZE(branch_size);
	ASSERT_NS((int) result->sr_item.i_total_size >= 0 && result->sr_item.i_total_size <= XT_INDEX_PAGE_SIZE-2);

	result->sr_item.i_node_ref_size = node_ref_size;

	search_flags = value->sv_flags;
	base = branch->tb_data + node_ref_size;
	bitem = base;
	bend = &branch->tb_data[result->sr_item.i_total_size];
	ilen = 0;
	if (bitem >= bend)
		goto done_ok;

	if (search_flags & XT_SEARCH_FIRST_FLAG)
		ilen = myxt_get_key_length(ind, bitem);
	else if (search_flags & XT_SEARCH_AFTER_LAST_FLAG) {
		bitem = bend;
		ilen = 0;
	}
	else {
		xtRecordID	key_record;
		int			r;

		key_record = value->sv_rec_id;

		ASSERT_NS(ind);
		while (bitem < bend) {
			ilen = myxt_get_key_length(ind, bitem);
			r = myxt_compare_key(ind, search_flags, value->sv_length, value->sv_key, bitem);
			if (r == 0) {
				if (search_flags & XT_SEARCH_WHOLE_KEY) {
					xtRecordID	item_record;
					xtRowID		row_id;

					xt_get_record_ref(bitem + ilen, &item_record, &row_id);

					/* This should not happen because we should never
					 * try to insert the same record twice into the 
					 * index!
					 */
					result->sr_duplicate = TRUE;
					if (key_record == item_record) {
						result->sr_found = TRUE;
						result->sr_item.i_item_size = ilen + XT_RECORD_REF_SIZE;
						result->sr_rec_id = item_record;
						result->sr_row_id = row_id;
						result->sr_branch = IDX_GET_NODE_REF(tab, bitem, node_ref_size);
						result->sr_item.i_item_offset = bitem - branch->tb_data;
						return;
					}
					if (key_record < item_record)
						r = -1;
					else
						r = 1;
				}
				else {
					result->sr_found = TRUE;
					/* -1 causes a search to the beginning of the duplicate list of keys.
					 * 1 causes a search to just after the key.
				 	*/
					if (search_flags & XT_SEARCH_AFTER_KEY)
						r = 1;
					else
						r = -1;
				}
			}
			if (r <= 0)
				break;
			bitem += ilen + XT_RECORD_REF_SIZE + node_ref_size;
		}
	}

	done_ok:
	result->sr_item.i_item_size = ilen + XT_RECORD_REF_SIZE;
	xt_get_res_record_ref(bitem + ilen, result);
	result->sr_branch = IDX_GET_NODE_REF(tab, bitem, node_ref_size);			/* Only valid if this is a node. */
	result->sr_item.i_item_offset = bitem - branch->tb_data;
#ifdef IND_SKEW_SPLIT_ON_APPEND
	if (bitem != bend)
		result->sr_last_item = FALSE;
#endif
}

/* Go to the next item in the node. */
static void idx_next_branch_item(XTTableHPtr XT_UNUSED(tab), XTIndexPtr ind, XTIdxBranchDPtr branch, register XTIdxResultRec *result)
{
	XT_NODE_TEMP;
	xtWord1	*bitem;
	u_int	ilen;

	result->sr_item.i_item_offset += result->sr_item.i_item_size + result->sr_item.i_node_ref_size;
	bitem = branch->tb_data + result->sr_item.i_item_offset;
	if (result->sr_item.i_item_offset < result->sr_item.i_total_size) {
		if (ind->mi_fix_key)
			ilen = result->sr_item.i_item_size;
		else {
			ilen = myxt_get_key_length(ind, bitem) + XT_RECORD_REF_SIZE;
			result->sr_item.i_item_size = ilen;
		}
		xt_get_res_record_ref(bitem + ilen - XT_RECORD_REF_SIZE, result); /* (Only valid if i_item_offset < i_total_size) */
	}
	else {
		result->sr_item.i_item_size = 0;
		result->sr_rec_id = 0;
		result->sr_row_id = 0;
	}
	if (result->sr_item.i_node_ref_size)
		/* IDX_GET_NODE_REF() loads the branch reference to the LEFT of the item. */
		result->sr_branch = IDX_GET_NODE_REF(tab, bitem, result->sr_item.i_node_ref_size);
	else
		result->sr_branch = 0;
}

xtPublic void xt_prev_branch_item_fix(XTTableHPtr XT_UNUSED(tab), XTIndexPtr XT_UNUSED(ind), XTIdxBranchDPtr branch, register XTIdxResultRec *result)
{
	XT_NODE_TEMP;
	ASSERT_NS(result->sr_item.i_item_offset >= result->sr_item.i_item_size + result->sr_item.i_node_ref_size + result->sr_item.i_node_ref_size);
	result->sr_item.i_item_offset -= (result->sr_item.i_item_size + result->sr_item.i_node_ref_size);
	xt_get_res_record_ref(branch->tb_data + result->sr_item.i_item_offset + result->sr_item.i_item_size - XT_RECORD_REF_SIZE, result); /* (Only valid if i_item_offset < i_total_size) */
	result->sr_branch = IDX_GET_NODE_REF(tab, branch->tb_data + result->sr_item.i_item_offset, result->sr_item.i_node_ref_size);
}

xtPublic void xt_prev_branch_item_var(XTTableHPtr XT_UNUSED(tab), XTIndexPtr ind, XTIdxBranchDPtr branch, register XTIdxResultRec *result)
{
	XT_NODE_TEMP;
	xtWord1	*bitem;
	xtWord1	*bend;
	u_int	ilen;

	bitem = branch->tb_data + result->sr_item.i_node_ref_size;
	bend = &branch->tb_data[result->sr_item.i_item_offset];
	for (;;) {
		ilen = myxt_get_key_length(ind, bitem);
		if (bitem + ilen + XT_RECORD_REF_SIZE + result->sr_item.i_node_ref_size >= bend)
			break;
		bitem += ilen + XT_RECORD_REF_SIZE + result->sr_item.i_node_ref_size;
	}

	result->sr_item.i_item_size = ilen + XT_RECORD_REF_SIZE;
	xt_get_res_record_ref(bitem + ilen, result); /* (Only valid if i_item_offset < i_total_size) */
	result->sr_branch = IDX_GET_NODE_REF(tab, bitem, result->sr_item.i_node_ref_size);
	result->sr_item.i_item_offset = bitem - branch->tb_data;
}

static void idx_reload_item_fix(XTIndexPtr XT_NDEBUG_UNUSED(ind), XTIdxBranchDPtr branch, register XTIdxResultPtr result)
{
	u_int branch_size;

	branch_size = XT_GET_DISK_2(branch->tb_size_2);
	ASSERT_NS(result->sr_item.i_node_ref_size == (XT_IS_NODE(branch_size) ? XT_NODE_REF_SIZE : 0));
	ASSERT_NS(result->sr_item.i_item_size == ind->mi_key_size + XT_RECORD_REF_SIZE);
	result->sr_item.i_total_size = XT_GET_BRANCH_DATA_SIZE(branch_size);
	if (result->sr_item.i_item_offset > result->sr_item.i_total_size)
		result->sr_item.i_item_offset = result->sr_item.i_total_size;
	xt_get_res_record_ref(&branch->tb_data[result->sr_item.i_item_offset + result->sr_item.i_item_size - XT_RECORD_REF_SIZE], result); 
}

static void idx_first_branch_item(XTTableHPtr XT_UNUSED(tab), XTIndexPtr ind, XTIdxBranchDPtr branch, register XTIdxResultPtr result)
{
	XT_NODE_TEMP;
	u_int branch_size;
	u_int node_ref_size;
	u_int key_data_size;

	branch_size = XT_GET_DISK_2(branch->tb_size_2);
	node_ref_size = XT_IS_NODE(branch_size) ? XT_NODE_REF_SIZE : 0;

	result->sr_found = FALSE;
	result->sr_duplicate = FALSE;
	result->sr_item.i_total_size = XT_GET_BRANCH_DATA_SIZE(branch_size);
	ASSERT_NS((int) result->sr_item.i_total_size >= 0 && result->sr_item.i_total_size <= XT_INDEX_PAGE_SIZE-2);

	if (ind->mi_fix_key)
		key_data_size = ind->mi_key_size;
	else {
		xtWord1 *bitem;

		bitem = branch->tb_data + node_ref_size;
		if (bitem < &branch->tb_data[result->sr_item.i_total_size])
			key_data_size = myxt_get_key_length(ind, bitem);
		else
			key_data_size = 0;
	}

	result->sr_item.i_item_size = key_data_size + XT_RECORD_REF_SIZE;
	result->sr_item.i_node_ref_size = node_ref_size;

	xt_get_res_record_ref(branch->tb_data + node_ref_size + key_data_size, result);
	result->sr_branch = IDX_GET_NODE_REF(tab, branch->tb_data + node_ref_size, node_ref_size); /* Only valid if this is a node. */
	result->sr_item.i_item_offset = node_ref_size;
}

/*
 * Last means different things for leaf or node!
 */
xtPublic void xt_last_branch_item_fix(XTTableHPtr XT_UNUSED(tab), XTIndexPtr ind, XTIdxBranchDPtr branch, register XTIdxResultPtr result)
{
	XT_NODE_TEMP;
	u_int branch_size;
	u_int node_ref_size;

	branch_size = XT_GET_DISK_2(branch->tb_size_2);
	node_ref_size = XT_IS_NODE(branch_size) ? XT_NODE_REF_SIZE : 0;

	result->sr_found = FALSE;
	result->sr_duplicate = FALSE;
	result->sr_item.i_total_size = XT_GET_BRANCH_DATA_SIZE(branch_size);
	ASSERT_NS((int) result->sr_item.i_total_size >= 0 && result->sr_item.i_total_size <= XT_INDEX_PAGE_SIZE-2);

	result->sr_item.i_item_size = ind->mi_key_size + XT_RECORD_REF_SIZE;
	result->sr_item.i_node_ref_size = node_ref_size;

	if (node_ref_size) {
		result->sr_item.i_item_offset = result->sr_item.i_total_size;
		result->sr_branch = IDX_GET_NODE_REF(tab, branch->tb_data + result->sr_item.i_item_offset, node_ref_size);
	}
	else {
		if (result->sr_item.i_total_size) {
			result->sr_item.i_item_offset = result->sr_item.i_total_size - result->sr_item.i_item_size;
			xt_get_res_record_ref(branch->tb_data + result->sr_item.i_item_offset + ind->mi_key_size, result);
		}
		else
			/* Leaf is empty: */
			result->sr_item.i_item_offset = 0;
	}
}

xtPublic void xt_last_branch_item_var(XTTableHPtr XT_UNUSED(tab), XTIndexPtr ind, XTIdxBranchDPtr branch, register XTIdxResultPtr result)
{
	XT_NODE_TEMP;
	u_int	branch_size;
	u_int	node_ref_size;

	branch_size = XT_GET_DISK_2(branch->tb_size_2);
	node_ref_size = XT_IS_NODE(branch_size) ? XT_NODE_REF_SIZE : 0;

	result->sr_found = FALSE;
	result->sr_duplicate = FALSE;
	result->sr_item.i_total_size = XT_GET_BRANCH_DATA_SIZE(branch_size);
	ASSERT_NS((int) result->sr_item.i_total_size >= 0 && result->sr_item.i_total_size <= XT_INDEX_PAGE_SIZE-2);

	result->sr_item.i_node_ref_size = node_ref_size;

	if (node_ref_size) {
		result->sr_item.i_item_offset = result->sr_item.i_total_size;
		result->sr_branch = IDX_GET_NODE_REF(tab, branch->tb_data + result->sr_item.i_item_offset, node_ref_size);
		result->sr_item.i_item_size = 0;
	}
	else {
		if (result->sr_item.i_total_size) {
			xtWord1	*bitem;
			u_int	ilen;
			xtWord1	*bend;

			bitem = branch->tb_data + node_ref_size;;
			bend = &branch->tb_data[result->sr_item.i_total_size];
			ilen = 0;
			if (bitem < bend) {
				for (;;) {
					ilen = myxt_get_key_length(ind, bitem);
					if (bitem + ilen + XT_RECORD_REF_SIZE + node_ref_size >= bend)
						break;
					bitem += ilen + XT_RECORD_REF_SIZE + node_ref_size;
				}
			}

			result->sr_item.i_item_offset = bitem - branch->tb_data;
			xt_get_res_record_ref(bitem + ilen, result);
			result->sr_item.i_item_size = ilen + XT_RECORD_REF_SIZE;
		}
		else {
			/* Leaf is empty: */
			result->sr_item.i_item_offset = 0;
			result->sr_item.i_item_size = 0;
		}
	}
}

xtPublic xtBool xt_idx_lazy_delete_on_leaf(XTIndexPtr ind, XTIndBlockPtr block, xtWord2 branch_size)
{
	ASSERT_NS(ind->mi_fix_key);
	
	/* Compact the leaf if more than half the items that fit on the page
	 * are deleted: */
	if (block->cp_del_count >= ind->mi_max_items/2)
		return FALSE;

	/* Compact the page if there is only 1 (or less) valid item left: */
	if ((u_int) block->cp_del_count+1 >= ((u_int) branch_size - 2)/(ind->mi_key_size + XT_RECORD_REF_SIZE))
		return FALSE;

	return OK;
}

static xtBool idx_lazy_delete_on_node(XTIndexPtr ind, XTIndBlockPtr block, register XTIdxItemPtr item)
{
	ASSERT_NS(ind->mi_fix_key);
	
	/* Compact the node if more than 1/4 of the items that fit on the page
	 * are deleted: */
	if (block->cp_del_count >= ind->mi_max_items/4)
		return FALSE;

	/* Compact the page if there is only 1 (or less) valid item left: */
	if ((u_int) block->cp_del_count+1 >= (item->i_total_size - item->i_node_ref_size)/(item->i_item_size + item->i_node_ref_size))
		return FALSE;

	return OK;
}

inline static xtBool idx_cmp_item_key_fix(XTIndReferencePtr iref, register XTIdxItemPtr item, XTIdxKeyValuePtr value)
{
	xtWord1 *data;

	data = &iref->ir_branch->tb_data[item->i_item_offset];
	return memcmp(data, value->sv_key, value->sv_length) == 0;
}

inline static void idx_set_item_key_fix(XTIndReferencePtr iref, register XTIdxItemPtr item, XTIdxKeyValuePtr value)
{
	xtWord1 *data;

	data = &iref->ir_branch->tb_data[item->i_item_offset];
	memcpy(data, value->sv_key, value->sv_length);
	xt_set_val_record_ref(data + value->sv_length, value);
#ifdef IND_OPT_DATA_WRITTEN
	if (item->i_item_offset < iref->ir_block->cb_min_pos)
		iref->ir_block->cb_min_pos = item->i_item_offset;
	if (item->i_item_offset + value->sv_length > iref->ir_block->cb_max_pos)
		iref->ir_block->cb_max_pos = item->i_item_offset + value->sv_length;
	ASSERT_NS(iref->ir_block->cb_max_pos <= XT_INDEX_PAGE_SIZE-2);
	ASSERT_NS(iref->ir_block->cb_min_pos <= iref->ir_block->cb_max_pos);
#endif
	iref->ir_updated = TRUE;
}

static xtBool idx_set_item_row_id(XTOpenTablePtr ot, XTIndexPtr ind, XTIndReferencePtr iref, register XTIdxItemPtr item, xtRowID row_id)
{
	register XTIndBlockPtr	block = iref->ir_block;
	size_t					offset;
	xtWord1					*data;

	if (block->cb_state == IDX_CAC_BLOCK_FLUSHING) {
		ASSERT_NS(ot->ot_table->tab_ind_flush_ilog);
		if (!ot->ot_table->tab_ind_flush_ilog->il_write_block(ot, block)) {
			xt_ind_release(ot, ind, iref->ir_xlock ? XT_UNLOCK_WRITE : XT_UNLOCK_READ, iref);
			return FAILED;
		}
	}

	offset = 
		/* This is the offset of the reference in the item we found: */
		item->i_item_offset +item->i_item_size - XT_RECORD_REF_SIZE +
		/* This is the offset of the row id in the reference: */
		XT_RECORD_ID_SIZE;
	data = &iref->ir_branch->tb_data[offset];

	/* This update does not change the structure of page, so we do it without
	 * copying the page before we write.
	 */
	XT_SET_DISK_4(data, row_id);
#ifdef IND_OPT_DATA_WRITTEN
	if (offset < block->cb_min_pos)
		block->cb_min_pos = offset;
	if (offset + XT_ROW_ID_SIZE > block->cb_max_pos)
		block->cb_max_pos = offset + XT_ROW_ID_SIZE;
	ASSERT_NS(block->cb_max_pos <= XT_INDEX_PAGE_SIZE-2);
	ASSERT_NS(block->cb_min_pos <= iref->ir_block->cb_max_pos);
#endif
	iref->ir_updated = TRUE;
	return OK;
}

inline static xtBool idx_is_item_deleted(register XTIdxBranchDPtr branch, register XTIdxItemPtr item)
{
	xtWord1	*data;

	data = &branch->tb_data[item->i_item_offset + item->i_item_size - XT_RECORD_REF_SIZE + XT_RECORD_ID_SIZE];
	return XT_GET_DISK_4(data) == (xtRowID) -1;
}

static xtBool idx_set_item_deleted(XTOpenTablePtr ot, XTIndexPtr ind, XTIndReferencePtr iref, register XTIdxItemPtr item)
{
	if (!idx_set_item_row_id(ot, ind, iref, item, (xtRowID) -1))
		return FAILED;
	
	/* This should be safe because there is only one thread,
	 * the sweeper, that does this!
	 *
	 * Threads that decrement this value have an xlock on
	 * the page, or the index.
	 */
	iref->ir_block->cp_del_count++;
	return OK;
}

/*
 * {LAZY-DEL-INDEX-ITEMS}
 * Do a lazy delete of an item by just setting the Row ID
 * to the delete indicator: row ID -1.
 */
static xtBool idx_lazy_delete_branch_item(XTOpenTablePtr ot, XTIndexPtr ind, XTIndReferencePtr iref, register XTIdxItemPtr item)
{
	if (!idx_set_item_deleted(ot, ind, iref, item))
		return FAILED;
	xt_ind_release(ot, ind, iref->ir_xlock ? XT_UNLOCK_W_UPDATE : XT_UNLOCK_R_UPDATE, iref);
	return OK;
}

/*
 * This function compacts the leaf, but preserves the
 * position of the item.
 */
static xtBool idx_compact_leaf(XTOpenTablePtr ot, XTIndexPtr ind, XTIndReferencePtr iref, register XTIdxItemPtr item)
{
	register XTIndBlockPtr		block = iref->ir_block;
	register XTIdxBranchDPtr	branch = iref->ir_branch;
	int		item_idx, count, i, idx;
	u_int	size;
	xtWord1	*s_data;
	xtWord1	*d_data;
	xtWord1	*data;
	xtRowID	row_id;

	if (block->cb_state == IDX_CAC_BLOCK_FLUSHING) {
		ASSERT_NS(ot->ot_table->tab_ind_flush_ilog);
		if (!ot->ot_table->tab_ind_flush_ilog->il_write_block(ot, block)) {
			xt_ind_release(ot, ind, iref->ir_xlock ? XT_UNLOCK_WRITE : XT_UNLOCK_READ, iref);
			return FAILED;
		}
	}

	if (block->cb_handle_count) {
		if (!xt_ind_copy_on_write(iref)) {
			xt_ind_release(ot, ind, iref->ir_xlock ? XT_UNLOCK_WRITE : XT_UNLOCK_READ, iref);
			return FAILED;
		}
	}

	ASSERT_NS(!item->i_node_ref_size);
	ASSERT_NS(ind->mi_fix_key);
	size = item->i_item_size;
	count = item->i_total_size / size;
	item_idx = item->i_item_offset / size;
	s_data = d_data = branch->tb_data;
	idx = 0;
	for (i=0; i<count; i++) {
		data = s_data + item->i_item_size - XT_RECORD_REF_SIZE + XT_RECORD_ID_SIZE;
		row_id = XT_GET_DISK_4(data);
		if (row_id == (xtRowID) -1) {
			if (idx < item_idx)
				item_idx--;
		}
		else {
			if (d_data != s_data)
				memcpy(d_data, s_data, size);
			d_data += size;
			idx++;
		}
		s_data += size;
	}
	block->cp_del_count = 0;
	item->i_total_size = d_data - branch->tb_data;
	ASSERT_NS(idx * size == item->i_total_size);
	item->i_item_offset = item_idx * size;
	XT_SET_DISK_2(branch->tb_size_2, XT_MAKE_BRANCH_SIZE(item->i_total_size, 0));
#ifdef IND_OPT_DATA_WRITTEN
	block->cb_header = TRUE;
	block->cb_min_pos = 0;
	block->cb_max_pos = item->i_total_size;
	ASSERT_NS(block->cb_max_pos <= XT_INDEX_PAGE_SIZE-2);
	ASSERT_NS(block->cb_min_pos <= iref->ir_block->cb_max_pos);
#endif
	iref->ir_updated = TRUE;
	return OK;
}

static xtBool idx_lazy_remove_leaf_item_right(XTOpenTablePtr ot, XTIndexPtr ind, XTIndReferencePtr iref, register XTIdxItemPtr item)
{
	register XTIndBlockPtr		block = iref->ir_block;
	register XTIdxBranchDPtr	branch = iref->ir_branch;
	int		item_idx, count, i;
	u_int	size;
	xtWord1	*s_data;
	xtWord1	*d_data;
	xtWord1	*data;
	xtRowID	row_id;

	ASSERT_NS(!item->i_node_ref_size);

	if (block->cb_state == IDX_CAC_BLOCK_FLUSHING) {
		ASSERT_NS(ot->ot_table->tab_ind_flush_ilog);
		if (!ot->ot_table->tab_ind_flush_ilog->il_write_block(ot, block)) {
			xt_ind_release(ot, ind, iref->ir_xlock ? XT_UNLOCK_WRITE : XT_UNLOCK_READ, iref);
			return FAILED;
		}
	}

	if (block->cb_handle_count) {
		if (!xt_ind_copy_on_write(iref)) {
			xt_ind_release(ot, ind, XT_UNLOCK_WRITE, iref);
			return FAILED;
		}
	}

	ASSERT_NS(ind->mi_fix_key);
	size = item->i_item_size;
	count = item->i_total_size / size;
	item_idx = item->i_item_offset / size;
	s_data = d_data = branch->tb_data;
	for (i=0; i<count; i++) {
		if (i == item_idx)
			item->i_item_offset = d_data - branch->tb_data;
		else {
			data = s_data + item->i_item_size - XT_RECORD_REF_SIZE + XT_RECORD_ID_SIZE;
			row_id = XT_GET_DISK_4(data);
			if (row_id != (xtRowID) -1) {
				if (d_data != s_data)
					memcpy(d_data, s_data, size);
				d_data += size;
			}
		}
		s_data += size;
	}
	block->cp_del_count = 0;
	item->i_total_size = d_data - branch->tb_data;
	XT_SET_DISK_2(branch->tb_size_2, XT_MAKE_BRANCH_SIZE(item->i_total_size, 0));
#ifdef IND_OPT_DATA_WRITTEN
	block->cb_header = TRUE;
	block->cb_min_pos = 0;
	block->cb_max_pos = item->i_total_size;
	ASSERT_NS(block->cb_max_pos <= XT_INDEX_PAGE_SIZE-2);
	ASSERT_NS(block->cb_min_pos <= iref->ir_block->cb_max_pos);
#endif
	iref->ir_updated = TRUE;
	xt_ind_release(ot, ind, XT_UNLOCK_W_UPDATE, iref);
	return OK;
}

/*
 * Remove an item and save to disk.
 */
static xtBool idx_remove_branch_item_right(XTOpenTablePtr ot, XTIndexPtr ind, xtIndexNodeID, XTIndReferencePtr iref, register XTIdxItemPtr item)
{
	register XTIndBlockPtr		block = iref->ir_block;
	register XTIdxBranchDPtr	branch = iref->ir_branch;
	u_int size = item->i_item_size + item->i_node_ref_size;

	if (block->cb_state == IDX_CAC_BLOCK_FLUSHING) {
		ASSERT_NS(ot->ot_table->tab_ind_flush_ilog);
		if (!ot->ot_table->tab_ind_flush_ilog->il_write_block(ot, block)) {
			xt_ind_release(ot, ind, iref->ir_xlock ? XT_UNLOCK_WRITE : XT_UNLOCK_READ, iref);
			return FAILED;
		}
	}

	/* {HANDLE-COUNT-USAGE}
	 * This access is safe because we have the right to update
	 * the page, so no other thread can modify the page.
	 *
	 * This means:
	 * We either have an Xlock on the index, or we have
	 * an Xlock on the cache block.
	 */
	if (block->cb_handle_count) {
		if (!xt_ind_copy_on_write(iref)) {
			xt_ind_release(ot, ind, item->i_node_ref_size ? XT_UNLOCK_READ : XT_UNLOCK_WRITE, iref);
			return FAILED;
		}
	}
	if (ind->mi_lazy_delete) {
		if (idx_is_item_deleted(branch, item))
			block->cp_del_count--;
	}
	/* Remove the node reference to the left of the item: */
	memmove(&branch->tb_data[item->i_item_offset],
		&branch->tb_data[item->i_item_offset + size],
		item->i_total_size - item->i_item_offset - size);
	item->i_total_size -= size;
	XT_SET_DISK_2(branch->tb_size_2, XT_MAKE_BRANCH_SIZE(item->i_total_size, item->i_node_ref_size));
	IDX_TRACE("%d-> %x\n", (int) XT_NODE_ID(address), (int) XT_GET_DISK_2(branch->tb_size_2));
#ifdef IND_OPT_DATA_WRITTEN
	block->cb_header = TRUE;
	if (item->i_item_offset < block->cb_min_pos)
		block->cb_min_pos = item->i_item_offset;
	block->cb_max_pos = item->i_total_size;
	ASSERT_NS(block->cb_max_pos <= XT_INDEX_PAGE_SIZE-2);
	ASSERT_NS(block->cb_min_pos <= block->cb_max_pos);
#endif
	iref->ir_updated = TRUE;
	xt_ind_release(ot, ind, item->i_node_ref_size ? XT_UNLOCK_R_UPDATE : XT_UNLOCK_W_UPDATE, iref);
	return OK;
}

static xtBool idx_remove_branch_item_left(XTOpenTablePtr ot, XTIndexPtr ind, xtIndexNodeID, XTIndReferencePtr iref, register XTIdxItemPtr item, xtBool *lazy_delete_cleanup_required)
{
	register XTIndBlockPtr		block = iref->ir_block;
	register XTIdxBranchDPtr	branch = iref->ir_branch;
	u_int size = item->i_item_size + item->i_node_ref_size;

	if (block->cb_state == IDX_CAC_BLOCK_FLUSHING) {
		ASSERT_NS(ot->ot_table->tab_ind_flush_ilog);
		if (!ot->ot_table->tab_ind_flush_ilog->il_write_block(ot, block)) {
			xt_ind_release(ot, ind, iref->ir_xlock ? XT_UNLOCK_WRITE : XT_UNLOCK_READ, iref);
			return FAILED;
		}
	}

	ASSERT_NS(item->i_node_ref_size);
	if (block->cb_handle_count) {
		if (!xt_ind_copy_on_write(iref)) {
			xt_ind_release(ot, ind, item->i_node_ref_size ? XT_UNLOCK_READ : XT_UNLOCK_WRITE, iref);
			return FAILED;
		}
	}
	if (ind->mi_lazy_delete) {
		if (idx_is_item_deleted(branch, item))
			block->cp_del_count--;
		if (lazy_delete_cleanup_required)
			*lazy_delete_cleanup_required = idx_lazy_delete_on_node(ind, block, item);
	}
	/* Remove the node reference to the left of the item: */
	memmove(&branch->tb_data[item->i_item_offset - item->i_node_ref_size],
		&branch->tb_data[item->i_item_offset + item->i_item_size],
		item->i_total_size - item->i_item_offset - item->i_item_size);
	item->i_total_size -= size;
	XT_SET_DISK_2(branch->tb_size_2, XT_MAKE_BRANCH_SIZE(item->i_total_size, item->i_node_ref_size));
	IDX_TRACE("%d-> %x\n", (int) XT_NODE_ID(address), (int) XT_GET_DISK_2(branch->tb_size_2));
#ifdef IND_OPT_DATA_WRITTEN
	block->cb_header = TRUE;
	if (item->i_item_offset - item->i_node_ref_size < block->cb_min_pos)
		block->cb_min_pos = item->i_item_offset - item->i_node_ref_size;
	block->cb_max_pos = item->i_total_size;
	ASSERT_NS(block->cb_max_pos <= XT_INDEX_PAGE_SIZE-2);
	ASSERT_NS(block->cb_min_pos <= block->cb_max_pos);
#endif
	iref->ir_updated = TRUE;
	xt_ind_release(ot, ind, item->i_node_ref_size ? XT_UNLOCK_R_UPDATE : XT_UNLOCK_W_UPDATE, iref);
	return OK;
}

static void idx_insert_leaf_item(XTIndexPtr XT_UNUSED(ind), XTIdxBranchDPtr leaf, XTIdxKeyValuePtr value, XTIdxResultPtr result)
{
	xtWord1 *item;

	/* This will ensure we do not overwrite the end of the buffer: */
	ASSERT_NS(value->sv_length <= XT_INDEX_MAX_KEY_SIZE);
	memmove(&leaf->tb_data[result->sr_item.i_item_offset + value->sv_length + XT_RECORD_REF_SIZE],
		&leaf->tb_data[result->sr_item.i_item_offset],
		result->sr_item.i_total_size - result->sr_item.i_item_offset);
	item = &leaf->tb_data[result->sr_item.i_item_offset];
	memcpy(item, value->sv_key, value->sv_length);
	xt_set_val_record_ref(item + value->sv_length, value);
	result->sr_item.i_total_size += value->sv_length + XT_RECORD_REF_SIZE;
	XT_SET_DISK_2(leaf->tb_size_2, XT_MAKE_LEAF_SIZE(result->sr_item.i_total_size));
}

static void idx_insert_node_item(XTTableHPtr XT_UNUSED(tab), XTIndexPtr XT_UNUSED(ind), XTIdxBranchDPtr leaf, XTIdxKeyValuePtr value, XTIdxResultPtr result, xtIndexNodeID branch)
{
	xtWord1 *item;

	/* This will ensure we do not overwrite the end of the buffer: */
	ASSERT_NS(value->sv_length <= XT_INDEX_MAX_KEY_SIZE);
	memmove(&leaf->tb_data[result->sr_item.i_item_offset + value->sv_length + XT_RECORD_REF_SIZE + result->sr_item.i_node_ref_size],
		&leaf->tb_data[result->sr_item.i_item_offset],
		result->sr_item.i_total_size - result->sr_item.i_item_offset);
	item = &leaf->tb_data[result->sr_item.i_item_offset];
	memcpy(item, value->sv_key, value->sv_length);
	xt_set_val_record_ref(item + value->sv_length, value);
	XT_SET_NODE_REF(tab, item + value->sv_length + XT_RECORD_REF_SIZE, branch);
	result->sr_item.i_total_size += value->sv_length + XT_RECORD_REF_SIZE + result->sr_item.i_node_ref_size;
	XT_SET_DISK_2(leaf->tb_size_2, XT_MAKE_NODE_SIZE(result->sr_item.i_total_size));
}

static xtBool idx_get_middle_branch_item(XTOpenTablePtr ot, XTIndexPtr ind, XTIdxBranchDPtr branch, XTIdxKeyValuePtr value, XTIdxResultPtr result)
{
	xtWord1	*bitem;

	ASSERT_NS(result->sr_item.i_node_ref_size == 0 || result->sr_item.i_node_ref_size == XT_NODE_REF_SIZE);
	ASSERT_NS((int) result->sr_item.i_total_size >= 0 && result->sr_item.i_total_size <= XT_INDEX_PAGE_SIZE*2);
	if (ind->mi_fix_key) {
		u_int full_item_size = result->sr_item.i_item_size + result->sr_item.i_node_ref_size;

		result->sr_item.i_item_offset = ((result->sr_item.i_total_size - result->sr_item.i_node_ref_size)
			/ full_item_size / 2 * full_item_size) + result->sr_item.i_node_ref_size;
#ifdef IND_SKEW_SPLIT_ON_APPEND
		if (result->sr_last_item) {
			u_int offset;
			
			offset = result->sr_item.i_total_size - full_item_size * 2;
			/* We actually split at the item before last! */
			if (offset > result->sr_item.i_item_offset)
				result->sr_item.i_item_offset = offset;
		}
#endif

		bitem = &branch->tb_data[result->sr_item.i_item_offset];
		value->sv_flags = XT_SEARCH_WHOLE_KEY;
		value->sv_length = result->sr_item.i_item_size - XT_RECORD_REF_SIZE;
		xt_get_record_ref(bitem + value->sv_length, &value->sv_rec_id, &value->sv_row_id);
		memcpy(value->sv_key, bitem, value->sv_length);
	}
	else {
		u_int	node_ref_size;
		u_int	ilen, tlen;
		xtWord1	*bend;

		node_ref_size = result->sr_item.i_node_ref_size;
		bitem = branch->tb_data + node_ref_size;
		bend = &branch->tb_data[(result->sr_item.i_total_size - node_ref_size) / 2 + node_ref_size];
#ifdef IND_SKEW_SPLIT_ON_APPEND
		if (result->sr_last_item)
			bend = &branch->tb_data[XT_INDEX_PAGE_DATA_SIZE];

		u_int	prev_ilen = 0;
		xtWord1	*prev_bitem = NULL;
#endif
		ilen = 0;
		if (bitem < bend) {
			tlen = 0;
			for (;;) {
				ilen = myxt_get_key_length(ind, bitem);
				tlen += ilen + XT_RECORD_REF_SIZE + node_ref_size;
				if (bitem + ilen + XT_RECORD_REF_SIZE + node_ref_size >= bend) {
					if (ilen > XT_INDEX_PAGE_SIZE || tlen > result->sr_item.i_total_size) {
						xt_register_taberr(XT_REG_CONTEXT, XT_ERR_INDEX_CORRUPTED, ot->ot_table->tab_name);
						return FAILED;
					}
					break;
				}
#ifdef IND_SKEW_SPLIT_ON_APPEND
				prev_ilen = ilen;
				prev_bitem = bitem;
#endif
				bitem += ilen + XT_RECORD_REF_SIZE + node_ref_size;
			}
		}

#ifdef IND_SKEW_SPLIT_ON_APPEND
		/* We actully want the item before last! */
		if (result->sr_last_item && prev_bitem) {
			bitem = prev_bitem;
			ilen = prev_ilen;
		}
#endif
		result->sr_item.i_item_offset = bitem - branch->tb_data;
		result->sr_item.i_item_size = ilen + XT_RECORD_REF_SIZE;

		value->sv_flags = XT_SEARCH_WHOLE_KEY;
		value->sv_length = ilen;
		xt_get_record_ref(bitem + ilen, &value->sv_rec_id, &value->sv_row_id);
		memcpy(value->sv_key, bitem, value->sv_length);
	}
	return OK;
}

static size_t idx_write_branch_item(XTIndexPtr XT_UNUSED(ind), xtWord1 *item, XTIdxKeyValuePtr value)
{
	memcpy(item, value->sv_key, value->sv_length);
	xt_set_val_record_ref(item + value->sv_length, value);
	return value->sv_length + XT_RECORD_REF_SIZE;
}

static xtBool idx_replace_node_key(XTOpenTablePtr ot, XTIndexPtr ind, IdxStackItemPtr item, IdxBranchStackPtr stack, u_int item_size, xtWord1 *item_buf)
{
	XTIndReferenceRec	iref;
	xtIndexNodeID		new_branch;
	XTIdxResultRec		result;
	xtIndexNodeID		current = item->i_branch;
	u_int				new_size;
	XTIdxBranchDPtr		new_branch_ptr;
	XTIdxKeyValueRec	key_value;
	xtWord1				key_buf[XT_INDEX_MAX_KEY_SIZE];

#ifdef DEBUG
	iref.ir_xlock = 2;
	iref.ir_updated = 2;
#endif
	if (!xt_ind_fetch(ot, ind, current, XT_LOCK_WRITE, &iref))
		return FAILED;

	if (iref.ir_block->cb_state == IDX_CAC_BLOCK_FLUSHING) {
		ASSERT_NS(ot->ot_table->tab_ind_flush_ilog);
		if (!ot->ot_table->tab_ind_flush_ilog->il_write_block(ot, iref.ir_block))
			goto failed_1;
	}

	if (iref.ir_block->cb_handle_count) {
		if (!xt_ind_copy_on_write(&iref))
			goto failed_1;
	}

	if (ind->mi_lazy_delete) {
		ASSERT_NS(item_size == item->i_pos.i_item_size);
		if (idx_is_item_deleted(iref.ir_branch, &item->i_pos))
			iref.ir_block->cp_del_count--;
	}
	memmove(&iref.ir_branch->tb_data[item->i_pos.i_item_offset + item_size],
		&iref.ir_branch->tb_data[item->i_pos.i_item_offset + item->i_pos.i_item_size],
		item->i_pos.i_total_size - item->i_pos.i_item_offset - item->i_pos.i_item_size);
	memcpy(&iref.ir_branch->tb_data[item->i_pos.i_item_offset],
		item_buf, item_size);
	if (ind->mi_lazy_delete) {
		if (idx_is_item_deleted(iref.ir_branch, &item->i_pos))
			iref.ir_block->cp_del_count++;
	}
	item->i_pos.i_total_size = item->i_pos.i_total_size + item_size - item->i_pos.i_item_size;
	XT_SET_DISK_2(iref.ir_branch->tb_size_2, XT_MAKE_NODE_SIZE(item->i_pos.i_total_size));
	IDX_TRACE("%d-> %x\n", (int) XT_NODE_ID(current), (int) XT_GET_DISK_2(iref.ir_branch->tb_size_2));
#ifdef IND_OPT_DATA_WRITTEN
	iref.ir_block->cb_header = TRUE;
	if (item->i_pos.i_item_offset < iref.ir_block->cb_min_pos)
		iref.ir_block->cb_min_pos = item->i_pos.i_item_offset;
	iref.ir_block->cb_max_pos = item->i_pos.i_total_size;
	ASSERT_NS(iref.ir_block->cb_min_pos <= iref.ir_block->cb_max_pos);
#endif
	iref.ir_updated = TRUE;

#ifdef DEBUG
	if (ind->mi_lazy_delete)
		ASSERT_NS(item->i_pos.i_total_size <= XT_INDEX_PAGE_DATA_SIZE);
#endif
	if (item->i_pos.i_total_size <= XT_INDEX_PAGE_DATA_SIZE)
		return xt_ind_release(ot, ind, XT_UNLOCK_W_UPDATE, &iref);

	/* The node has overflowed!! */
#ifdef IND_SKEW_SPLIT_ON_APPEND
	result.sr_last_item = FALSE;
#endif
	result.sr_item = item->i_pos;

	/* Adjust the stack (we want the parents of the delete node): */
	for (;;) {
		if (idx_pop(stack) == item)
			break;
	}		

	/* We assume that value can be overwritten (which is the case) */
	key_value.sv_flags = XT_SEARCH_WHOLE_KEY;
	key_value.sv_key = key_buf;
	if (!idx_get_middle_branch_item(ot, ind, iref.ir_branch, &key_value, &result))
		goto failed_1;

	if (!idx_new_branch(ot, ind, &new_branch))
		goto failed_1;

	/* Split the node: */
	new_size = result.sr_item.i_total_size - result.sr_item.i_item_offset - result.sr_item.i_item_size;
	// TODO: Are 2 buffers now required?
	new_branch_ptr = (XTIdxBranchDPtr) &ot->ot_ind_wbuf.tb_data[XT_INDEX_PAGE_DATA_SIZE];
	memmove(new_branch_ptr->tb_data, &iref.ir_branch->tb_data[result.sr_item.i_item_offset + result.sr_item.i_item_size], new_size);

	XT_SET_DISK_2(new_branch_ptr->tb_size_2, XT_MAKE_NODE_SIZE(new_size));
	IDX_TRACE("%d-> %x\n", (int) XT_NODE_ID(new_branch), (int) XT_GET_DISK_2(new_branch_ptr->tb_size_2));
	if (!xt_ind_write(ot, ind, new_branch, offsetof(XTIdxBranchDRec, tb_data) + new_size, (xtWord1 *) new_branch_ptr))
		goto failed_2;

	/* Change the size of the old branch: */
	XT_SET_DISK_2(iref.ir_branch->tb_size_2, XT_MAKE_NODE_SIZE(result.sr_item.i_item_offset));
	IDX_TRACE("%d-> %x\n", (int) XT_NODE_ID(current), (int) XT_GET_DISK_2(iref.ir_branch->tb_size_2));
#ifdef IND_OPT_DATA_WRITTEN
	iref.ir_block->cb_header = TRUE;
	if (result.sr_item.i_item_offset < iref.ir_block->cb_min_pos)
		iref.ir_block->cb_min_pos = result.sr_item.i_item_offset;
	iref.ir_block->cb_max_pos = result.sr_item.i_item_offset;
	ASSERT_NS(iref.ir_block->cb_max_pos <= XT_INDEX_PAGE_DATA_SIZE);
	ASSERT_NS(iref.ir_block->cb_min_pos <= iref.ir_block->cb_max_pos);
#endif
	iref.ir_updated = TRUE;
	xt_ind_release(ot, ind, XT_UNLOCK_W_UPDATE, &iref);

	/* Insert the new branch into the parent node, using the new middle key value: */
	if (!idx_insert_node(ot, ind, stack, FALSE, &key_value, new_branch)) {
		/* 
		 * TODO: Mark the index as corrupt.
		 * This should not fail because everything has been
		 * preallocated.
		 * However, if it does fail the index
		 * will be corrupt.
		 * I could modify and release the branch above,
		 * after this point.
		 * But that would mean holding the lock longer,
		 * and also may not help because idx_insert_node()
		 * is recursive.
		 */
		idx_free_branch(ot, ind, new_branch);
		return FAILED;
	}

	return OK;

	failed_2:
	idx_free_branch(ot, ind, new_branch);

	failed_1:
	xt_ind_release(ot, ind, XT_UNLOCK_WRITE, &iref);

	return FAILED;
}

/*ot_ind_wbuf
 * -----------------------------------------------------------------------
 * Standard b-tree insert
 */

/*
 * Insert the given branch into the node on the top of the stack. If the stack
 * is empty we need to add a new root.
 */
static xtBool idx_insert_node(XTOpenTablePtr ot, XTIndexPtr ind, IdxBranchStackPtr stack, xtBool last_item, XTIdxKeyValuePtr key_value, xtIndexNodeID branch)
{
	IdxStackItemPtr		stack_item;
	xtIndexNodeID		new_branch;
	size_t				size;
	xtIndexNodeID		current;
	XTIndReferenceRec	iref;
	XTIdxResultRec		result;
	u_int				new_size;
	XTIdxBranchDPtr		new_branch_ptr;
#ifdef IND_OPT_DATA_WRITTEN
	u_int				new_min_pos;
#endif

#ifdef DEBUG
	iref.ir_xlock = 2;
	iref.ir_updated = 2;
#endif
	/* Insert a new branch (key, data)... */
	if (!(stack_item = idx_pop(stack))) {
		xtWord1 *ditem;

		/* New root */
		if (!idx_new_branch(ot, ind, &new_branch))
			goto failed;

		ditem = ot->ot_ind_wbuf.tb_data;
		XT_SET_NODE_REF(ot->ot_table, ditem, ind->mi_root);
		ditem += XT_NODE_REF_SIZE;
		ditem += idx_write_branch_item(ind, ditem, key_value);
		XT_SET_NODE_REF(ot->ot_table, ditem, branch);
		ditem += XT_NODE_REF_SIZE;
		size = ditem - ot->ot_ind_wbuf.tb_data;
		XT_SET_DISK_2(ot->ot_ind_wbuf.tb_size_2, XT_MAKE_NODE_SIZE(size));
		IDX_TRACE("%d-> %x\n", (int) XT_NODE_ID(new_branch), (int) XT_GET_DISK_2(ot->ot_ind_wbuf.tb_size_2));
		if (!xt_ind_write(ot, ind, new_branch, offsetof(XTIdxBranchDRec, tb_data) + size, (xtWord1 *) &ot->ot_ind_wbuf))
			goto failed_2;
		ind->mi_root = new_branch;
		goto done_ok;
	}

	current = stack_item->i_branch;
	/* This read does not count (towards ot_ind_reads), because we are only
	 * counting each loaded page once. We assume that the page is in
	 * cache, and will remain in cache when we read again below for the
	 * purpose of update.
	 */
	if (!xt_ind_fetch(ot, ind, current, XT_LOCK_READ, &iref))
		goto failed;
	ASSERT_NS(XT_IS_NODE(XT_GET_DISK_2(iref.ir_branch->tb_size_2)));
#ifdef IND_SKEW_SPLIT_ON_APPEND
	result.sr_last_item = last_item;
#endif
	ind->mi_scan_branch(ot->ot_table, ind, iref.ir_branch, key_value, &result);

	if (result.sr_item.i_total_size + key_value->sv_length + XT_RECORD_REF_SIZE + result.sr_item.i_node_ref_size <= XT_INDEX_PAGE_DATA_SIZE) {
		if (iref.ir_block->cb_state == IDX_CAC_BLOCK_FLUSHING) {
			ASSERT_NS(ot->ot_table->tab_ind_flush_ilog);
			if (!ot->ot_table->tab_ind_flush_ilog->il_write_block(ot, iref.ir_block))
				goto failed_1;
		}

		if (iref.ir_block->cb_handle_count) {
			if (!xt_ind_copy_on_write(&iref))
				goto failed_1;
		}

		idx_insert_node_item(ot->ot_table, ind, iref.ir_branch, key_value, &result, branch);
		IDX_TRACE("%d-> %x\n", (int) XT_NODE_ID(current), (int) XT_GET_DISK_2(ot->ot_ind_wbuf.tb_size_2));
#ifdef IND_OPT_DATA_WRITTEN
		iref.ir_block->cb_header = TRUE;
		if (result.sr_item.i_item_offset < iref.ir_block->cb_min_pos)
			iref.ir_block->cb_min_pos = result.sr_item.i_item_offset;
		iref.ir_block->cb_max_pos = result.sr_item.i_total_size;
		ASSERT_NS(iref.ir_block->cb_max_pos <= XT_INDEX_PAGE_DATA_SIZE);
	ASSERT_NS(iref.ir_block->cb_min_pos <= iref.ir_block->cb_max_pos);
#endif
		iref.ir_updated = TRUE;
		ASSERT_NS(result.sr_item.i_total_size <= XT_INDEX_PAGE_DATA_SIZE);
		xt_ind_release(ot, ind, XT_UNLOCK_R_UPDATE, &iref);
		goto done_ok;
	}

	memcpy(&ot->ot_ind_wbuf, iref.ir_branch, offsetof(XTIdxBranchDRec, tb_data) + result.sr_item.i_total_size);
	idx_insert_node_item(ot->ot_table, ind, &ot->ot_ind_wbuf, key_value, &result, branch);
	IDX_TRACE("%d-> %x\n", (int) XT_NODE_ID(current), (int) XT_GET_DISK_2(ot->ot_ind_wbuf.tb_size_2));
	ASSERT_NS(result.sr_item.i_total_size > XT_INDEX_PAGE_DATA_SIZE);
#ifdef IND_OPT_DATA_WRITTEN
	new_min_pos = result.sr_item.i_item_offset;
#endif

	/* We assume that value can be overwritten (which is the case) */
	if (!idx_get_middle_branch_item(ot, ind, &ot->ot_ind_wbuf, key_value, &result))
		goto failed_1;

	if (!idx_new_branch(ot, ind, &new_branch))
		goto failed_1;

	/* Split the node: */
	new_size = result.sr_item.i_total_size - result.sr_item.i_item_offset - result.sr_item.i_item_size;
	new_branch_ptr = (XTIdxBranchDPtr) &ot->ot_ind_wbuf.tb_data[XT_INDEX_PAGE_DATA_SIZE];
	memmove(new_branch_ptr->tb_data, &ot->ot_ind_wbuf.tb_data[result.sr_item.i_item_offset + result.sr_item.i_item_size], new_size);

	XT_SET_DISK_2(new_branch_ptr->tb_size_2, XT_MAKE_NODE_SIZE(new_size));
	IDX_TRACE("%d-> %x\n", (int) XT_NODE_ID(new_branch), (int) XT_GET_DISK_2(new_branch_ptr->tb_size_2));
	if (!xt_ind_write(ot, ind, new_branch, offsetof(XTIdxBranchDRec, tb_data) + new_size, (xtWord1 *) new_branch_ptr))
		goto failed_2;

	/* Change the size of the old branch: */
	XT_SET_DISK_2(ot->ot_ind_wbuf.tb_size_2, XT_MAKE_NODE_SIZE(result.sr_item.i_item_offset));
	IDX_TRACE("%d-> %x\n", (int) XT_NODE_ID(current), (int) XT_GET_DISK_2(ot->ot_ind_wbuf.tb_size_2));

	if (iref.ir_block->cb_state == IDX_CAC_BLOCK_FLUSHING) {
		ASSERT_NS(ot->ot_table->tab_ind_flush_ilog);
		if (!ot->ot_table->tab_ind_flush_ilog->il_write_block(ot, iref.ir_block))
			goto failed_2;
	}

	if (iref.ir_block->cb_handle_count) {
		if (!xt_ind_copy_on_write(&iref))
			goto failed_2;
	}

#ifdef IND_OPT_DATA_WRITTEN
	if (result.sr_item.i_item_offset < new_min_pos)
		new_min_pos = result.sr_item.i_item_offset;
#endif
	memcpy(iref.ir_branch, &ot->ot_ind_wbuf, offsetof(XTIdxBranchDRec, tb_data) + result.sr_item.i_item_offset);
#ifdef IND_OPT_DATA_WRITTEN
	iref.ir_block->cb_header = TRUE;
	if (new_min_pos < iref.ir_block->cb_min_pos)
		iref.ir_block->cb_min_pos = new_min_pos;
	iref.ir_block->cb_max_pos = result.sr_item.i_item_offset;
	ASSERT_NS(iref.ir_block->cb_max_pos <= XT_INDEX_PAGE_DATA_SIZE);
	ASSERT_NS(iref.ir_block->cb_min_pos <= iref.ir_block->cb_max_pos);
#endif
	iref.ir_updated = TRUE;
	xt_ind_release(ot, ind, XT_UNLOCK_R_UPDATE, &iref);

	/* Insert the new branch into the parent node, using the new middle key value: */
	if (!idx_insert_node(ot, ind, stack, last_item, key_value, new_branch)) {
		// Index may be inconsistant now...
		idx_free_branch(ot, ind, new_branch);
		goto failed;
	}

	done_ok:
	return OK;

	failed_2:
	idx_free_branch(ot, ind, new_branch);

	failed_1:
	xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);

	failed:
	return FAILED;
}

#define IDX_MAX_INDEX_FLUSH_COUNT		10

struct IdxTableItem {
	xtTableID		ti_tab_id;
	u_int			ti_dirty_blocks;
};

inline u_int idx_dirty_blocks(XTTableHPtr tab)
{
	XTIndexPtr	*indp;
	XTIndexPtr	ind;
	u_int		dirty_blocks;

	dirty_blocks = 0;
	indp = tab->tab_dic.dic_keys;
	for (int i=0; i<tab->tab_dic.dic_key_count; i++, indp++) {
		ind = *indp;
		dirty_blocks += ind->mi_dirty_blocks;
	}
	return dirty_blocks;
}

static xtBool idx_out_of_memory_failure(XTOpenTablePtr ot)
{
#ifdef XT_TRACK_INDEX_UPDATES
	/* If the index has been changed when we run out of memory, we
	 * will corrupt the index!
	 */
	ASSERT_NS(ot->ot_ind_changed == 0);
#endif
	if (ot->ot_thread->t_exception.e_xt_err == XT_ERR_NO_INDEX_CACHE) {
		u_int block_total = xt_ind_get_blocks();

		/* Flush index and retry! */
		xt_clear_exception(ot->ot_thread);

		if (idx_dirty_blocks(ot->ot_table) >= block_total / 4) {
			if (!xt_async_flush_indices(ot->ot_table, FALSE, TRUE, ot->ot_thread))
				return FAILED;
			if (!xt_wait_for_async_task_results(ot->ot_thread))
				return FAILED;
		}
		else {
			XTDatabaseHPtr	db = ot->ot_table->tab_db;
			IdxTableItem	table_list[IDX_MAX_INDEX_FLUSH_COUNT];
			int				item_count = 0;
			int				i;
			u_int			edx;
			XTTableEntryPtr	tab_ptr;
			u_int			dirty_blocks;
			u_int			dirty_total = 0;

			xt_ht_lock(NULL, db->db_tables);
			xt_enum_tables_init(&edx);
			while ((tab_ptr = xt_enum_tables_next(NULL, db, &edx))) {
				if (tab_ptr->te_table) {
					if (tab_ptr->te_table->tab_ind_flush_task->tk_is_running()) {
						if (!(dirty_blocks = tab_ptr->te_table->tab_ind_flush_task->fit_dirty_blocks))
							dirty_blocks = idx_dirty_blocks(tab_ptr->te_table);
					}
					else
						dirty_blocks = idx_dirty_blocks(tab_ptr->te_table);
					dirty_total += dirty_blocks;
					if (dirty_blocks) {
						for (i=0; i<item_count; i++) {
							if (table_list[i].ti_dirty_blocks < dirty_blocks)
								break;
						}
						if (i < IDX_MAX_INDEX_FLUSH_COUNT) {
							int cnt;
							
							if (item_count < IDX_MAX_INDEX_FLUSH_COUNT) {
								cnt = item_count - i;
								item_count++;
							}
							else
								cnt = item_count - i - 1;
							memmove(&table_list[i], &table_list[i+1], sizeof(IdxTableItem) * cnt);
							table_list[i].ti_tab_id = tab_ptr->te_table->tab_id;
							table_list[i].ti_dirty_blocks = dirty_blocks;
						}
					}
					if (dirty_total >= block_total / 4)
						break;
				}
			}
			xt_ht_unlock(NULL, db->db_tables);
			if (dirty_total >= block_total / 4) {
				for (i=0; i<item_count; i++) {
					if (table_list[i].ti_tab_id == ot->ot_table->tab_id) {
						if (!xt_async_flush_indices(ot->ot_table, FALSE, TRUE, ot->ot_thread))
							return FAILED;
					}
					else {
						XTTableHPtr tab;
						xtBool		ok;

						if ((tab = xt_use_table_by_id_ns(db, table_list[i].ti_tab_id))) {
							ok = xt_async_flush_indices(tab, FALSE, TRUE, ot->ot_thread);
							xt_heap_release_ns(tab);
						}
					}
				}
				if (!xt_wait_for_async_task_results(ot->ot_thread))
					return FAILED;
			}
		}

		return TRUE;
	}
	return FALSE;
}

/*
 * Check all the duplicate variation in an index.
 * If one of them is visible, then we have a duplicate key
 * error.
 *
 * GOTCHA: This routine must use the write index buffer!
 */
static xtBool idx_check_duplicates(XTOpenTablePtr ot, XTIndexPtr ind, XTIdxKeyValuePtr key_value)
{
	IdxBranchStackRec	stack;
	xtIndexNodeID		current;
	XTIndReferenceRec	iref;
	XTIdxResultRec		result;
	xtBool				on_key = FALSE;
	xtXactID			xn_id;
	int					save_flags;				
	XTXactWaitRec		xw;

#ifdef DEBUG
	iref.ir_xlock = 2;
	iref.ir_updated = 2;
#endif
	retry:
	idx_newstack(&stack);

	if (!(XT_NODE_ID(current) = XT_NODE_ID(ind->mi_root)))
		return OK;

	save_flags = key_value->sv_flags;
	key_value->sv_flags = 0;

	while (XT_NODE_ID(current)) {
		if (!xt_ind_fetch(ot, ind, current, XT_LOCK_READ, &iref)) {
			key_value->sv_flags = save_flags;
			return FAILED;
		}
		ind->mi_scan_branch(ot->ot_table, ind, iref.ir_branch, key_value, &result);
		if (result.sr_found)
			/* If we have found the key in a node: */
			on_key = TRUE;
		if (!result.sr_item.i_node_ref_size)
			break;
		xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
		if (!idx_push(&stack, current, &result.sr_item)) {
			key_value->sv_flags = save_flags;
			return FAILED;
		}
		current = result.sr_branch;
	}

	key_value->sv_flags = save_flags;

	if (!on_key) {
		xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
		return OK;
	}

	for (;;) {
		if (result.sr_item.i_item_offset == result.sr_item.i_total_size) {
			IdxStackItemPtr node;

			/* We are at the end of a leaf node.
			 * Go up the stack to find the start position of the next key.
			 * If we find none, then we are the end of the index.
			 */
			xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
			while ((node = idx_pop(&stack))) {
				if (node->i_pos.i_item_offset < node->i_pos.i_total_size) {
					current = node->i_branch;
					if (!xt_ind_fetch(ot, ind, current, XT_LOCK_READ, &iref))
						return FAILED;
					xt_get_res_record_ref(&iref.ir_branch->tb_data[node->i_pos.i_item_offset + node->i_pos.i_item_size - XT_RECORD_REF_SIZE], &result);
					result.sr_item = node->i_pos;
					goto check_value;
				}
			}
			break;
		}

		check_value:
		/* Quit the loop if the key is no longer matched! */
		if (myxt_compare_key(ind, 0, key_value->sv_length, key_value->sv_key, &iref.ir_branch->tb_data[result.sr_item.i_item_offset]) != 0) {
			xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
			break;
		}

		if (ind->mi_lazy_delete) {
			if (result.sr_row_id == (xtRowID) -1)
				goto next_item;
		}

		switch (xt_tab_maybe_committed(ot, result.sr_rec_id, &xn_id, NULL, NULL)) {
			case XT_MAYBE:
				/* Record is not committed, wait for the transaction. */
				xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
				XT_INDEX_UNLOCK(ind, ot);				
				xw.xw_xn_id = xn_id;
				if (!xt_xn_wait_for_xact(ot->ot_thread, &xw, NULL)) {
					XT_INDEX_WRITE_LOCK(ind, ot);
					return FAILED;
				}
				XT_INDEX_WRITE_LOCK(ind, ot);
				goto retry;			
			case XT_ERR:
				/* Error while reading... */
				goto failed;
			case TRUE:
				/* Record is committed or belongs to me, duplicate key: */
				XT_DEBUG_TRACE(("DUPLICATE KEY tx=%d rec=%d\n", (int) ot->ot_thread->st_xact_data->xd_start_xn_id, (int) result.sr_rec_id));
				xt_register_xterr(XT_REG_CONTEXT, XT_ERR_DUPLICATE_KEY);
				goto failed;
			case FALSE:
				/* Record is deleted or rolled-back: */
				break;
		}

		next_item:
		idx_next_branch_item(ot->ot_table, ind, iref.ir_branch, &result);

		if (result.sr_item.i_node_ref_size) {
			/* Go down to the bottom: */
			while (XT_NODE_ID(current)) {
				xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
				if (!idx_push(&stack, current, &result.sr_item))
					return FAILED;
				current = result.sr_branch;
				if (!xt_ind_fetch(ot, ind, current, XT_LOCK_READ, &iref))
					return FAILED;
				idx_first_branch_item(ot->ot_table, ind, iref.ir_branch, &result);
				if (!result.sr_item.i_node_ref_size)
					break;
			}
		}
	}

	return OK;
	
	failed:
	xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
	return FAILED;
}

inline static void idx_still_on_key(XTIndexPtr ind, register XTIdxSearchKeyPtr search_key, register XTIdxBranchDPtr branch, register XTIdxItemPtr item)
{
	if (search_key && search_key->sk_on_key) {
		search_key->sk_on_key = myxt_compare_key(ind, search_key->sk_key_value.sv_flags, search_key->sk_key_value.sv_length,
			search_key->sk_key_value.sv_key, &branch->tb_data[item->i_item_offset]) == 0;
	}
}

/*
 * Insert a value into the given index. Return FALSE if an error occurs.
 */
xtPublic xtBool xt_idx_insert(XTOpenTablePtr ot, XTIndexPtr ind, xtRowID row_id, xtRecordID rec_id, xtWord1 *rec_buf, xtWord1 *bef_buf, xtBool allow_dups)
{
	XTIdxKeyValueRec	key_value;
	xtWord1				key_buf[XT_INDEX_MAX_KEY_SIZE];
	IdxBranchStackRec	stack;
	xtIndexNodeID		current;
	XTIndReferenceRec	iref;
	xtIndexNodeID		new_branch;
	XTIdxBranchDPtr		new_branch_ptr;
	size_t				size;
	XTIdxResultRec		result;
	size_t				new_size;
	xtBool				check_for_dups = ind->mi_flags & (HA_UNIQUE_CHECK | HA_NOSAME) && !allow_dups;
	xtBool				lock_structure = FALSE;
	xtBool				updated = FALSE;
#ifdef IND_OPT_DATA_WRITTEN
	u_int				new_min_pos;
#endif

#ifdef DEBUG
	iref.ir_xlock = 2;
	iref.ir_updated = 2;
#endif
#ifdef CHECK_AND_PRINT
	//idx_check_index(ot, ind, TRUE);
#endif

	retry_after_oom:
#ifdef XT_TRACK_INDEX_UPDATES
	ot->ot_ind_changed = 0;
#endif
	key_value.sv_flags = XT_SEARCH_WHOLE_KEY;
	key_value.sv_rec_id = rec_id;
	key_value.sv_row_id = row_id;		/* Should always be zero on insert (will be update by sweeper later). 
										 * Non-zero only during recovery, assuming that sweeper will process such records right after recovery.
										 */
	key_value.sv_key = key_buf;
	key_value.sv_length = myxt_create_key_from_row(ind, key_buf, rec_buf, &check_for_dups);

	if (bef_buf && check_for_dups) {
		/* If we have a before image, and we are required to check for duplicates.
		 * then compare the before image key with the after image key.
		 */
		xtWord1	bef_key_buf[XT_INDEX_MAX_KEY_SIZE];
		u_int	len;
		xtBool	has_no_null = TRUE;

		len = myxt_create_key_from_row(ind, bef_key_buf, bef_buf, &has_no_null);
		if (has_no_null) {
			/* If the before key has no null values, then compare with the after key value.
			 * We only have to check for duplicates if the key has changed!
			 */
			check_for_dups = myxt_compare_key(ind, 0, len, bef_key_buf, key_buf) != 0;
		}
	}

	/* The index appears to have no root: */
	if (!XT_NODE_ID(ind->mi_root))
		lock_structure = TRUE;

	lock_and_retry:
	idx_newstack(&stack);

	/* A write lock is only required if we are going to change the
	 * strcuture of the index!
	 */
	if (lock_structure)
		XT_INDEX_WRITE_LOCK(ind, ot);
	else
		XT_INDEX_READ_LOCK(ind, ot);

	retry:
	/* Create a root node if required: */
	if (!(XT_NODE_ID(current) = XT_NODE_ID(ind->mi_root))) {
		/* Index is empty, create a new one: */
		ASSERT_NS(lock_structure);
		if (!xt_ind_reserve(ot, 1, NULL))
			goto failed;
		if (!idx_new_branch(ot, ind, &new_branch))
			goto failed;
		size = idx_write_branch_item(ind, ot->ot_ind_wbuf.tb_data, &key_value);
		XT_SET_DISK_2(ot->ot_ind_wbuf.tb_size_2, XT_MAKE_LEAF_SIZE(size));
		IDX_TRACE("%d-> %x\n", (int) new_branch, (int) XT_GET_DISK_2(ot->ot_ind_wbuf.tb_size_2));
		if (!xt_ind_write(ot, ind, new_branch, offsetof(XTIdxBranchDRec, tb_data) + size, (xtWord1 *) &ot->ot_ind_wbuf))
			goto failed_2;
		ind->mi_root = new_branch;
		goto done_ok;
	}

	/* Search down the tree for the insertion point. */
#ifdef IND_SKEW_SPLIT_ON_APPEND
	result.sr_last_item = TRUE;
#endif
	while (XT_NODE_ID(current)) {
		if (!xt_ind_fetch(ot, ind, current, XT_XLOCK_LEAF, &iref))
			goto failed;
		ind->mi_scan_branch(ot->ot_table, ind, iref.ir_branch, &key_value, &result);
		if (result.sr_duplicate) {
			if (check_for_dups) {
				/* Duplicates are not allowed, at least one has been
				 * found...
				 */

				/* Leaf nodes (i_node_ref_size == 0) are write locked,
				 * non-leaf nodes are read locked.
				 */
				xt_ind_release(ot, ind, result.sr_item.i_node_ref_size ? XT_UNLOCK_READ : XT_UNLOCK_WRITE, &iref);

				if (!idx_check_duplicates(ot, ind, &key_value))
					goto failed;
				/* We have checked all the "duplicate" variations. None of them are
				 * relevant. So this will cause a correct insert.
				 */
				check_for_dups = FALSE;
				idx_newstack(&stack);
				goto retry;
			}
		}
		if (result.sr_found) {
			/* Node found, can happen during recovery of indexes! 
			 * We have found an exact match of both key and record.
			 */
			XTPageUnlockType	utype;
			xtBool				overwrite = FALSE;

			/* {LAZY-DEL-INDEX-ITEMS}
			 * If the item has been lazy deleted, then just overwrite!
			 */ 
			if (result.sr_row_id == (xtRowID) -1) {
				xtWord2 del_count;
	
				/* This is safe because we have an xlock on the leaf. */
				if ((del_count = iref.ir_block->cp_del_count))
					iref.ir_block->cp_del_count = del_count-1;
				overwrite = TRUE;
			}

			if (!result.sr_row_id && row_id) {
				/* {INDEX-RECOV_ROWID} Set the row-id
				 * during recovery, even if the index entry
				 * is not committed.
				 * It will be removed later by the sweeper.
				 */
				overwrite = TRUE;
			}

			if (overwrite) {
				if (!idx_set_item_row_id(ot, ind, &iref, &result.sr_item, row_id))
					goto failed;
				utype = result.sr_item.i_node_ref_size ? XT_UNLOCK_R_UPDATE : XT_UNLOCK_W_UPDATE;
			}
			else
				utype = result.sr_item.i_node_ref_size ? XT_UNLOCK_READ : XT_UNLOCK_WRITE;
			xt_ind_release(ot, ind, utype, &iref);
			goto done_ok;
		}
		/* Stop when we get to a leaf: */
		if (!result.sr_item.i_node_ref_size)
			break;
		xt_ind_release(ot, ind, result.sr_item.i_node_ref_size ? XT_UNLOCK_READ : XT_UNLOCK_WRITE, &iref);
		if (!idx_push(&stack, current, NULL))
			goto failed;
		current = result.sr_branch;
	}
	ASSERT_NS(XT_NODE_ID(current));
	
	/* Must be a leaf!: */
	ASSERT_NS(!result.sr_item.i_node_ref_size);

	updated = FALSE;
	if (ind->mi_lazy_delete && iref.ir_block->cp_del_count) {
		/* There are a number of possibilities:
		 * - We could just replace a lazy deleted slot.
		 * - We could compact and insert.
		 * - We could just insert
		 */

		if (result.sr_item.i_item_offset > 0) {
			/* Check if it can go into the previous node: */
			XTIdxResultRec	t_res;

			t_res.sr_item = result.sr_item;
			xt_prev_branch_item_fix(ot->ot_table, ind, iref.ir_branch, &t_res);
			if (t_res.sr_row_id != (xtRowID) -1)
				goto try_current;

			/* Yup, it can, but first check to see if it would be 
			 * better to put it in the current node.
			 * This is the case if the previous node key is not the
			 * same as the key we are adding...
			 */
			if (result.sr_item.i_item_offset < result.sr_item.i_total_size &&
				result.sr_row_id == (xtRowID) -1) {
				if (!idx_cmp_item_key_fix(&iref, &t_res.sr_item, &key_value))
					goto try_current;
			}

			idx_set_item_key_fix(&iref, &t_res.sr_item, &key_value);
			iref.ir_block->cp_del_count--;
			xt_ind_release(ot, ind, XT_UNLOCK_W_UPDATE, &iref);
			goto done_ok;
		}

		try_current:
		if (result.sr_item.i_item_offset < result.sr_item.i_total_size) {
			if (result.sr_row_id == (xtRowID) -1) {
				idx_set_item_key_fix(&iref, &result.sr_item, &key_value);
				iref.ir_block->cp_del_count--;
				xt_ind_release(ot, ind, XT_UNLOCK_W_UPDATE, &iref);
				goto done_ok;
			}
		}

		/* Check if we must compact... 
		 * It makes no sense to split as long as there are lazy deleted items
		 * in the page. So, delete them if a split would otherwise be required!
		 */
		ASSERT_NS(key_value.sv_length + XT_RECORD_REF_SIZE == result.sr_item.i_item_size);
		if (result.sr_item.i_total_size + key_value.sv_length + XT_RECORD_REF_SIZE > XT_INDEX_PAGE_DATA_SIZE) {
			if (!idx_compact_leaf(ot, ind, &iref, &result.sr_item))
				goto failed;
			updated = TRUE;
		}
		
		/* Fall through to the insert code... */
		/* NOTE: if there were no lazy deleted items in the leaf, then
		 * idx_compact_leaf is a NOP. This is the only case in which it may not
		 * fall through and do the insert below.
		 *
		 * Normally, if the cp_del_count is correct then the insert
		 * will work below, and the assertion here will not fail.
		 *
		 * In this case, the xt_ind_release() will correctly indicate an update.
		 */
		ASSERT_NS(result.sr_item.i_total_size + key_value.sv_length + XT_RECORD_REF_SIZE <= XT_INDEX_PAGE_DATA_SIZE);
	}

	if (result.sr_item.i_total_size + key_value.sv_length + XT_RECORD_REF_SIZE <= XT_INDEX_PAGE_DATA_SIZE) {
		if (iref.ir_block->cb_state == IDX_CAC_BLOCK_FLUSHING) {
			ASSERT_NS(ot->ot_table->tab_ind_flush_ilog);
			if (!ot->ot_table->tab_ind_flush_ilog->il_write_block(ot, iref.ir_block))
				goto failed_1;
		}

		if (iref.ir_block->cb_handle_count) {
			if (!xt_ind_copy_on_write(&iref))
				goto failed_1;
		}

		idx_insert_leaf_item(ind, iref.ir_branch, &key_value, &result);
		IDX_TRACE("%d-> %x\n", (int) XT_NODE_ID(current), (int) XT_GET_DISK_2(ot->ot_ind_wbuf.tb_size_2));
		ASSERT_NS(result.sr_item.i_total_size <= XT_INDEX_PAGE_DATA_SIZE);
#ifdef IND_OPT_DATA_WRITTEN
		iref.ir_block->cb_header = TRUE;
		if (result.sr_item.i_item_offset < iref.ir_block->cb_min_pos)
			iref.ir_block->cb_min_pos = result.sr_item.i_item_offset;
		iref.ir_block->cb_max_pos = result.sr_item.i_total_size;
		ASSERT_NS(iref.ir_block->cb_max_pos <= XT_INDEX_PAGE_DATA_SIZE);
	ASSERT_NS(iref.ir_block->cb_min_pos <= iref.ir_block->cb_max_pos);
#endif
		iref.ir_updated = TRUE;
		xt_ind_release(ot, ind, XT_UNLOCK_W_UPDATE, &iref);
		goto done_ok;
	}

	/* Key does not fit. Must split the node.
	 * Make sure we have a structural lock:
	 */
	if (!lock_structure) {
		xt_ind_release(ot, ind, updated ? XT_UNLOCK_W_UPDATE : XT_UNLOCK_WRITE, &iref);
		XT_INDEX_UNLOCK(ind, ot);
		lock_structure = TRUE;
		goto lock_and_retry;
	}

	memcpy(&ot->ot_ind_wbuf, iref.ir_branch, offsetof(XTIdxBranchDRec, tb_data) + result.sr_item.i_total_size);
	idx_insert_leaf_item(ind, &ot->ot_ind_wbuf, &key_value, &result);
	IDX_TRACE("%d-> %x\n", (int) XT_NODE_ID(current), (int) XT_GET_DISK_2(ot->ot_ind_wbuf.tb_size_2));
	ASSERT_NS(result.sr_item.i_total_size > XT_INDEX_PAGE_DATA_SIZE && result.sr_item.i_total_size <= XT_INDEX_PAGE_DATA_SIZE*2);
#ifdef IND_OPT_DATA_WRITTEN
	new_min_pos = result.sr_item.i_item_offset;
#endif

	/* This is the number of potential writes. In other words, the total number
	 * of blocks that may be accessed.
	 *
	 * Note that this assume if a block is read and written soon after that the block
	 * will not be freed in-between (a safe assumption?)
	 */
	if (!xt_ind_reserve(ot, stack.s_top * 2 + 3, iref.ir_branch))
		goto failed_1;

	/* Key does not fit, must split... */
	if (!idx_get_middle_branch_item(ot, ind, &ot->ot_ind_wbuf, &key_value, &result))
		goto failed_1;

	if (!idx_new_branch(ot, ind, &new_branch))
		goto failed_1;

	/* Copy and write the rest of the data to the new node: */
	new_size = result.sr_item.i_total_size - result.sr_item.i_item_offset - result.sr_item.i_item_size;
	new_branch_ptr = (XTIdxBranchDPtr) &ot->ot_ind_wbuf.tb_data[XT_INDEX_PAGE_DATA_SIZE];
	memmove(new_branch_ptr->tb_data, &ot->ot_ind_wbuf.tb_data[result.sr_item.i_item_offset + result.sr_item.i_item_size], new_size);

	XT_SET_DISK_2(new_branch_ptr->tb_size_2, XT_MAKE_LEAF_SIZE(new_size));
	IDX_TRACE("%d-> %x\n", (int) XT_NODE_ID(new_branch), (int) XT_GET_DISK_2(new_branch_ptr->tb_size_2));
	if (!xt_ind_write(ot, ind, new_branch, offsetof(XTIdxBranchDRec, tb_data) + new_size, (xtWord1 *) new_branch_ptr))
		goto failed_2;

	/* Modify the first node: */
	XT_SET_DISK_2(ot->ot_ind_wbuf.tb_size_2, XT_MAKE_LEAF_SIZE(result.sr_item.i_item_offset));
	IDX_TRACE("%d-> %x\n", (int) XT_NODE_ID(current), (int) XT_GET_DISK_2(ot->ot_ind_wbuf.tb_size_2));

	if (iref.ir_block->cb_state == IDX_CAC_BLOCK_FLUSHING) {
		ASSERT_NS(ot->ot_table->tab_ind_flush_ilog);
		if (!ot->ot_table->tab_ind_flush_ilog->il_write_block(ot, iref.ir_block))
			goto failed_2;
	}

	if (iref.ir_block->cb_handle_count) {
		if (!xt_ind_copy_on_write(&iref))
			goto failed_2;
	}
#ifdef IND_OPT_DATA_WRITTEN
	if (result.sr_item.i_item_offset < new_min_pos)
		new_min_pos = result.sr_item.i_item_offset;
#endif
	memcpy(iref.ir_branch, &ot->ot_ind_wbuf, offsetof(XTIdxBranchDRec, tb_data) + result.sr_item.i_item_offset);
#ifdef IND_OPT_DATA_WRITTEN
	iref.ir_block->cb_header = TRUE;
	if (new_min_pos < iref.ir_block->cb_min_pos)
		iref.ir_block->cb_min_pos = new_min_pos;
	iref.ir_block->cb_max_pos = result.sr_item.i_item_offset;
	ASSERT_NS(iref.ir_block->cb_max_pos <= XT_INDEX_PAGE_DATA_SIZE);
	ASSERT_NS(iref.ir_block->cb_min_pos <= iref.ir_block->cb_max_pos);
#endif
	iref.ir_updated = TRUE;
	xt_ind_release(ot, ind, XT_UNLOCK_W_UPDATE, &iref);

	/* Insert the new branch into the parent node, using the new middle key value: */
	if (!idx_insert_node(ot, ind, &stack, result.sr_last_item, &key_value, new_branch)) {
		// Index may be inconsistant now...
		idx_free_branch(ot, ind, new_branch);
		goto failed;
	}

#ifdef XT_TRACK_INDEX_UPDATES
	ASSERT_NS(ot->ot_ind_reserved >= ot->ot_ind_reads);
#endif

	done_ok:
	XT_INDEX_UNLOCK(ind, ot);

#ifdef DEBUG
	//printf("INSERT OK\n");
	//idx_check_index(ot, ind, TRUE);
#endif
	xt_ind_unreserve(ot);
	return OK;

	failed_2:
	idx_free_branch(ot, ind, new_branch);

	failed_1:
	xt_ind_release(ot, ind, updated ? XT_UNLOCK_W_UPDATE : XT_UNLOCK_WRITE, &iref);

	failed:
	XT_INDEX_UNLOCK(ind, ot);
	if (idx_out_of_memory_failure(ot))
		goto retry_after_oom;

#ifdef DEBUG
	//printf("INSERT FAILED\n");
	//idx_check_index(ot, ind, TRUE);
#endif
	xt_ind_unreserve(ot);
	return FAILED;
}


/* Remove the given item in the node.
 * This is done by going down the tree to find a replacement
 * for the deleted item!
 */
static xtBool idx_remove_item_in_node(XTOpenTablePtr ot, XTIndexPtr ind, IdxBranchStackPtr stack, XTIndReferencePtr iref, XTIdxKeyValuePtr key_value)
{
	IdxStackItemPtr		delete_node;
	XTIdxResultRec		result;
	xtIndexNodeID		current;
	xtBool				lazy_delete_cleanup_required = FALSE;
	IdxStackItemPtr		current_top;

	delete_node = idx_top(stack);
	current = delete_node->i_branch;
	result.sr_item = delete_node->i_pos;

	/* Follow the branch after this item: */
	idx_next_branch_item(ot->ot_table, ind, iref->ir_branch, &result);
	xt_ind_release(ot, ind, iref->ir_updated ? XT_UNLOCK_R_UPDATE : XT_UNLOCK_READ, iref);

	/* Go down the left-hand side until we reach a leaf: */
	while (XT_NODE_ID(current)) {
		current = result.sr_branch;
		if (!xt_ind_fetch(ot, ind, current, XT_XLOCK_LEAF, iref))
			return FAILED;
		idx_first_branch_item(ot->ot_table, ind, iref->ir_branch, &result);
		if (!result.sr_item.i_node_ref_size)
			break;
		xt_ind_release(ot, ind, XT_UNLOCK_READ, iref);
		if (!idx_push(stack, current, &result.sr_item))
			return FAILED;
	}

	ASSERT_NS(XT_NODE_ID(current));
	ASSERT_NS(!result.sr_item.i_node_ref_size);

	if (!xt_ind_reserve(ot, stack->s_top + 2, iref->ir_branch)) {
		xt_ind_release(ot, ind, XT_UNLOCK_WRITE, iref);
		return FAILED;
	}
	
	/* This code removes lazy deleted items from the leaf,
	 * before we promote an item to a leaf.
	 * This is not essential, but prevents lazy deleted
	 * items from being propogated up the tree.
	 */
	if (ind->mi_lazy_delete) {
		if (iref->ir_block->cp_del_count) {
			if (!idx_compact_leaf(ot, ind, iref, &result.sr_item))
				return FAILED;
		}
	}

	/* Crawl back up the stack trace, looking for a key
	 * that can be used to replace the deleted key.
	 *
	 * Any empty nodes on the way up can be removed!
	 */
	if (result.sr_item.i_total_size > 0) {
		/* There is a key in the leaf, extract it, and put it in the node: */
		memcpy(key_value->sv_key, &iref->ir_branch->tb_data[result.sr_item.i_item_offset], result.sr_item.i_item_size);
		/* This call also frees the iref.ir_branch page! */
		if (!idx_remove_branch_item_right(ot, ind, current, iref, &result.sr_item))
			return FAILED;
		if (!idx_replace_node_key(ot, ind, delete_node, stack, result.sr_item.i_item_size, key_value->sv_key))
			return FAILED;
		goto done_ok;
	}

	xt_ind_release(ot, ind, iref->ir_updated ? XT_UNLOCK_W_UPDATE : XT_UNLOCK_WRITE, iref);

	for (;;) {
		/* The current node/leaf is empty, remove it: */
		idx_free_branch(ot, ind, current);

		current_top = idx_pop(stack);
		current = current_top->i_branch;
		if (!xt_ind_fetch(ot, ind, current, XT_XLOCK_LEAF, iref))
			return FAILED;
		
		if (current_top == delete_node) {
			/* All children have been removed. Delete the key and done: */
			if (!idx_remove_branch_item_right(ot, ind, current, iref, &current_top->i_pos))
				return FAILED;
			goto done_ok;
		}

		if (current_top->i_pos.i_total_size > current_top->i_pos.i_node_ref_size) {
			/* Save the key: */
			memcpy(key_value->sv_key, &iref->ir_branch->tb_data[current_top->i_pos.i_item_offset], current_top->i_pos.i_item_size);
			/* This function also frees the cache page: */
			if (!idx_remove_branch_item_left(ot, ind, current, iref, &current_top->i_pos, &lazy_delete_cleanup_required))
				return FAILED;
			if (!idx_replace_node_key(ot, ind, delete_node, stack, current_top->i_pos.i_item_size, key_value->sv_key))
				return FAILED;
			/* */
			if (lazy_delete_cleanup_required) {
				if (!xt_ind_fetch(ot, ind, current, XT_LOCK_READ, iref))
					return FAILED;
				if (!idx_remove_lazy_deleted_item_in_node(ot, ind, current, iref, key_value))
					return FAILED;
			}
			goto done_ok;
		}
		xt_ind_release(ot, ind, current_top->i_pos.i_node_ref_size ? XT_UNLOCK_READ : XT_UNLOCK_WRITE, iref);
	}

	done_ok:
#ifdef XT_TRACK_INDEX_UPDATES
	ASSERT_NS(ot->ot_ind_reserved >= ot->ot_ind_reads);
#endif
	return OK;
}

/*
 * This function assumes we have a lock on the structure of the index.
 */
static xtBool idx_remove_lazy_deleted_item_in_node(XTOpenTablePtr ot, XTIndexPtr ind, xtIndexNodeID current, XTIndReferencePtr iref, XTIdxKeyValuePtr key_value)
{
	IdxBranchStackRec	stack;
	XTIdxResultRec		result;

	/* Now remove all lazy deleted items in this node.... */
	idx_first_branch_item(ot->ot_table, ind, (XTIdxBranchDPtr) iref->ir_block->cb_data, &result);

	for (;;) {
		while (result.sr_item.i_item_offset < result.sr_item.i_total_size) {
			if (result.sr_row_id == (xtRowID) -1)
				goto remove_item;
			idx_next_branch_item(ot->ot_table, ind, (XTIdxBranchDPtr) iref->ir_block->cb_data, &result);
		}
		break;

		remove_item:

		idx_newstack(&stack);
		if (!idx_push(&stack, current, &result.sr_item)) {
			xt_ind_release(ot, ind, iref->ir_updated ? XT_UNLOCK_R_UPDATE : XT_UNLOCK_READ, iref);
			return FAILED;
		}

		if (!idx_remove_item_in_node(ot, ind, &stack, iref, key_value))
			return FAILED;

		/* Go back up to the node we are trying to
		 * free of things.
		 */
		if (!xt_ind_fetch(ot, ind, current, XT_LOCK_READ, iref))
			return FAILED;
		/* Load the data again: */
		idx_reload_item_fix(ind, iref->ir_branch, &result);
	}

	xt_ind_release(ot, ind, iref->ir_updated ? XT_UNLOCK_R_UPDATE : XT_UNLOCK_READ, iref);
	return OK;
}

static xtBool idx_delete(XTOpenTablePtr ot, XTIndexPtr ind, XTIdxKeyValuePtr key_value)
{
	IdxBranchStackRec	stack;
	xtIndexNodeID		current;
	XTIndReferenceRec	iref;
	XTIdxResultRec		result;
	xtBool				lock_structure = FALSE;

#ifdef DEBUG
	iref.ir_xlock = 2;
	iref.ir_updated = 2;
#endif
	/* The index appears to have no root: */
	if (!XT_NODE_ID(ind->mi_root))
		lock_structure = TRUE;

	lock_and_retry:
	idx_newstack(&stack);

	if (lock_structure)
		XT_INDEX_WRITE_LOCK(ind, ot);
	else
		XT_INDEX_READ_LOCK(ind, ot);

	if (!(XT_NODE_ID(current) = XT_NODE_ID(ind->mi_root)))
		goto done_ok;

	while (XT_NODE_ID(current)) {
		if (!xt_ind_fetch(ot, ind, current, XT_XLOCK_DEL_LEAF, &iref))
			goto failed;
		ind->mi_scan_branch(ot->ot_table, ind, iref.ir_branch, key_value, &result);
		if (!result.sr_item.i_node_ref_size) {
			/* A leaf... */
			if (result.sr_found) {
				if (ind->mi_lazy_delete) {
					/* If the we have a W lock, then fetch decided that we
					 * need to compact the page.
					 * The decision is made by xt_idx_lazy_delete_on_leaf() 
					 */
					if (!iref.ir_xlock) {
						if (!idx_lazy_delete_branch_item(ot, ind, &iref, &result.sr_item))
							goto failed;
					}
					else {
						if (!iref.ir_block->cp_del_count) {
							if (!idx_remove_branch_item_right(ot, ind, current, &iref, &result.sr_item))
								goto failed;
						}
						else {
							if (!idx_lazy_remove_leaf_item_right(ot, ind, &iref, &result.sr_item))
								goto failed;
						}
					}
				}
				else {
					if (!idx_remove_branch_item_right(ot, ind, current, &iref, &result.sr_item))
						goto failed;
				}
			}
			else
				xt_ind_release(ot, ind, iref.ir_xlock ? XT_UNLOCK_WRITE : XT_UNLOCK_READ, &iref);
			goto done_ok;
		}
		if (!idx_push(&stack, current, &result.sr_item)) {
			xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
			goto failed;
		}
		if (result.sr_found)
			/* If we have found the key in a node: */
			break;
		xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
		current = result.sr_branch;
	}

	/* Must be a non-leaf!: */
	ASSERT_NS(result.sr_item.i_node_ref_size);

	if (ind->mi_lazy_delete) {
		if (!idx_lazy_delete_on_node(ind, iref.ir_block, &result.sr_item)) {
			/* We need to remove some items from this node: */

			if (!lock_structure) {
				xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
				XT_INDEX_UNLOCK(ind, ot);
				lock_structure = TRUE;
				goto lock_and_retry;
			}

			if (!idx_set_item_deleted(ot, ind, &iref, &result.sr_item))
				goto failed;
			if (!idx_remove_lazy_deleted_item_in_node(ot, ind, current, &iref, key_value))
				goto failed;
			goto done_ok;
		}

		if (!ot->ot_table->tab_dic.dic_no_lazy_delete) {
			/* {LAZY-DEL-INDEX-ITEMS}
			 * We just set item to deleted, this is a significant time
			 * saver.
			 * But this item can only be cleaned up when all
			 * items on the node below are deleted.
			 */
			if (!idx_lazy_delete_branch_item(ot, ind, &iref, &result.sr_item))
				goto failed;
			goto done_ok;
		}
	}

	/* We will have to remove the key from a non-leaf node,
	 * which means we are changing the structure of the index.
	 * Make sure we have a structural lock:
	 */
	if (!lock_structure) {
		xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
		XT_INDEX_UNLOCK(ind, ot);
		lock_structure = TRUE;
		goto lock_and_retry;
	}

	/* This is the item we will have to replace: */
	if (!idx_remove_item_in_node(ot, ind, &stack, &iref, key_value))
		goto failed;

	done_ok:
	XT_INDEX_UNLOCK(ind, ot);

#ifdef DEBUG
	//printf("DELETE OK\n");
	//idx_check_index(ot, ind, TRUE);
#endif
	xt_ind_unreserve(ot);
	return OK;

	failed:
	XT_INDEX_UNLOCK(ind, ot);
	xt_ind_unreserve(ot);
	return FAILED;
}

xtPublic xtBool xt_idx_delete(XTOpenTablePtr ot, XTIndexPtr ind, xtRecordID rec_id, xtWord1 *rec_buf)
{
	XTIdxKeyValueRec	key_value;
	xtWord1				key_buf[XT_INDEX_MAX_KEY_SIZE + XT_MAX_RECORD_REF_SIZE];

	retry_after_oom:
#ifdef XT_TRACK_INDEX_UPDATES
	ot->ot_ind_changed = 0;
#endif

	key_value.sv_flags = XT_SEARCH_WHOLE_KEY;
	key_value.sv_rec_id = rec_id;
	key_value.sv_row_id = 0;
	key_value.sv_key = key_buf;
	key_value.sv_length = myxt_create_key_from_row(ind, key_buf, rec_buf, NULL);

	if (!idx_delete(ot, ind, &key_value)) {
		if (idx_out_of_memory_failure(ot))
			goto retry_after_oom;
		return FAILED;
	}
	return OK;
}

xtPublic xtBool xt_idx_update_row_id(XTOpenTablePtr ot, XTIndexPtr ind, xtRecordID rec_id, xtRowID row_id, xtWord1 *rec_buf)
{
	xtIndexNodeID		current;
	XTIndReferenceRec	iref;
	XTIdxResultRec		result;
	XTIdxKeyValueRec	key_value;
	xtWord1				key_buf[XT_INDEX_MAX_KEY_SIZE + XT_MAX_RECORD_REF_SIZE];

#ifdef DEBUG
	iref.ir_xlock = 2;
	iref.ir_updated = 2;
#endif
#ifdef CHECK_AND_PRINT
	idx_check_index(ot, ind, TRUE);
#endif
	retry_after_oom:
#ifdef XT_TRACK_INDEX_UPDATES
	ot->ot_ind_changed = 0;
#endif
	key_value.sv_flags = XT_SEARCH_WHOLE_KEY;
	key_value.sv_rec_id = rec_id;
	key_value.sv_row_id = 0;
	key_value.sv_key = key_buf;
	key_value.sv_length = myxt_create_key_from_row(ind, key_buf, rec_buf, NULL);

	/* NOTE: Only a read lock is required for this!!
	 *
	 * 09.05.2008 - This has changed because the dirty list now
	 * hangs on the index. And the dirty list may be updated
	 * by any change of the index.
	 * However, the advantage is that I should be able to read
	 * lock in the first phase of the flush.
	 *
	 * 18.02.2009 - This has changed again.
	 * I am now using a read lock, because this update does not
	 * require a structural change. In fact, it does not even
	 * need a WRITE LOCK on the page affected, because there
	 * is only ONE thread that can do this (the sweeper).
	 *
	 * This has the advantage that the sweeper (which uses this
	 * function, causes less conflicts.
	 *
	 * However, it does mean that the dirty list must be otherwise
	 * protected (which it now is be a spin lock - mi_dirty_lock).
	 *
	 * It also has the dissadvantage that I am going to have to
	 * take an xlock in the first phase of the flush.
	 */
	XT_INDEX_READ_LOCK(ind, ot);

	if (!(XT_NODE_ID(current) = XT_NODE_ID(ind->mi_root)))
		goto done_ok;

	while (XT_NODE_ID(current)) {
		if (!xt_ind_fetch(ot, ind, current, XT_LOCK_READ, &iref))
			goto failed;
		ind->mi_scan_branch(ot->ot_table, ind, iref.ir_branch, &key_value, &result);
		if (result.sr_found || !result.sr_item.i_node_ref_size)
			break;
		xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
		current = result.sr_branch;
	}

	if (result.sr_found) {
		/* TODO: Check that concurrent reads can handle this!
		 * assuming the write is not atomic.
		 */
		if (!idx_set_item_row_id(ot, ind, &iref, &result.sr_item, row_id))
			goto failed;
		xt_ind_release(ot, ind, XT_UNLOCK_R_UPDATE, &iref);
	}
	else
		xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);

	done_ok:
	XT_INDEX_UNLOCK(ind, ot);

#ifdef DEBUG
	//idx_check_index(ot, ind, TRUE);
	//idx_check_on_key(ot);
#endif
	return OK;

	failed:
	XT_INDEX_UNLOCK(ind, ot);
	if (idx_out_of_memory_failure(ot))
		goto retry_after_oom;
	return FAILED;
}

xtPublic void xt_idx_prep_key(XTIndexPtr ind, register XTIdxSearchKeyPtr search_key, int flags, xtWord1 *in_key_buf, size_t in_key_length)
{
	search_key->sk_key_value.sv_flags = flags;
	search_key->sk_key_value.sv_rec_id = 0;
	search_key->sk_key_value.sv_row_id = 0;
	search_key->sk_key_value.sv_key = search_key->sk_key_buf;
	search_key->sk_key_value.sv_length = myxt_create_key_from_key(ind, search_key->sk_key_buf, in_key_buf, in_key_length);
	search_key->sk_on_key = FALSE;
}

xtPublic xtBool xt_idx_research(XTOpenTablePtr ot, XTIndexPtr ind)
{
	XTIdxSearchKeyRec search_key;

	xt_ind_lock_handle(ot->ot_ind_rhandle);
	search_key.sk_key_value.sv_flags = XT_SEARCH_WHOLE_KEY;
	xt_get_record_ref(&ot->ot_ind_rhandle->ih_branch->tb_data[ot->ot_ind_state.i_item_offset + ot->ot_ind_state.i_item_size - XT_RECORD_REF_SIZE],
		&search_key.sk_key_value.sv_rec_id, &search_key.sk_key_value.sv_row_id);
	search_key.sk_key_value.sv_key = search_key.sk_key_buf;
	search_key.sk_key_value.sv_length = ot->ot_ind_state.i_item_size - XT_RECORD_REF_SIZE;
	search_key.sk_on_key = FALSE;
	memcpy(search_key.sk_key_buf, &ot->ot_ind_rhandle->ih_branch->tb_data[ot->ot_ind_state.i_item_offset], search_key.sk_key_value.sv_length);
	xt_ind_unlock_handle(ot->ot_ind_rhandle);
	return xt_idx_search(ot, ind, &search_key);
}

/*
 * Search for a given key and position the current pointer on the first
 * key in the list of duplicates. If the key is not found the current
 * pointer is placed at the first position after the key.
 */
xtPublic xtBool xt_idx_search(XTOpenTablePtr ot, XTIndexPtr ind, register XTIdxSearchKeyPtr search_key)
{
	IdxBranchStackRec	stack;
	xtIndexNodeID		current;
	XTIndReferenceRec	iref;
	XTIdxResultRec		result;

#ifdef DEBUG
	iref.ir_xlock = 2;
	iref.ir_updated = 2;
#endif
	if (ot->ot_ind_rhandle) {
		xt_ind_release_handle(ot->ot_ind_rhandle, FALSE, ot->ot_thread);
		ot->ot_ind_rhandle = NULL;
	}
#ifdef DEBUG
	//idx_check_index(ot, ind, TRUE);
#endif

	/* Calling from recovery, this is not the case.
	 * But the index read does not require a transaction!
	 * Only insert requires this to check for duplicates.
	if (!ot->ot_thread->st_xact_data) {
		xt_register_xterr(XT_REG_CONTEXT, XT_ERR_NO_TRANSACTION);
		return FAILED;
	}
	*/

	retry_after_oom:
#ifdef XT_TRACK_INDEX_UPDATES
	ot->ot_ind_changed = 0;
#endif
	idx_newstack(&stack);

	ot->ot_curr_rec_id = 0;
	ot->ot_curr_row_id = 0;

	XT_INDEX_READ_LOCK(ind, ot);

	if (!(XT_NODE_ID(current) = XT_NODE_ID(ind->mi_root)))
		goto done_ok;

	while (XT_NODE_ID(current)) {
		if (!xt_ind_fetch(ot, ind, current, XT_LOCK_READ, &iref))
			goto failed;
		ind->mi_scan_branch(ot->ot_table, ind, iref.ir_branch, &search_key->sk_key_value, &result);
		if (result.sr_found)
			/* If we have found the key in a node: */
			search_key->sk_on_key = TRUE;
		if (!result.sr_item.i_node_ref_size)
			break;
		xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
		if (!idx_push(&stack, current, &result.sr_item))
			goto failed;
		current = result.sr_branch;
	}

	if (ind->mi_lazy_delete) {
		ignore_lazy_deleted_items:
		while (result.sr_item.i_item_offset < result.sr_item.i_total_size) {
			if (result.sr_row_id != (xtRowID) -1) {
				idx_still_on_key(ind, search_key, iref.ir_branch, &result.sr_item);
				break;
			}
			idx_next_branch_item(ot->ot_table, ind, iref.ir_branch, &result);
		}
	}

	if (result.sr_item.i_item_offset == result.sr_item.i_total_size) {
		IdxStackItemPtr node;

		/* We are at the end of a leaf node.
		 * Go up the stack to find the start position of the next key.
		 * If we find none, then we are the end of the index.
		 */
		xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
		while ((node = idx_pop(&stack))) {
			if (node->i_pos.i_item_offset < node->i_pos.i_total_size) {
				if (!xt_ind_fetch(ot, ind, node->i_branch, XT_LOCK_READ, &iref))
					goto failed;
				xt_get_res_record_ref(&iref.ir_branch->tb_data[node->i_pos.i_item_offset + node->i_pos.i_item_size - XT_RECORD_REF_SIZE], &result);

				if (ind->mi_lazy_delete) {
					result.sr_item = node->i_pos;
					if (result.sr_row_id == (xtRowID) -1) {
						/* If this node pointer is lazy deleted, then
						 * go down the next branch...
						 */
						idx_next_branch_item(ot->ot_table, ind, iref.ir_branch, &result);

						/* Go down to the bottom: */
						current = node->i_branch;
						while (XT_NODE_ID(current)) {
							xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
							if (!idx_push(&stack, current, &result.sr_item))
								goto failed;
							current = result.sr_branch;
							if (!xt_ind_fetch(ot, ind, current, XT_LOCK_READ, &iref))
								goto failed;
							idx_first_branch_item(ot->ot_table, ind, iref.ir_branch, &result);
							if (!result.sr_item.i_node_ref_size)
								break;
						}

						goto ignore_lazy_deleted_items;
					}
					idx_still_on_key(ind, search_key, iref.ir_branch, &result.sr_item);
				}

				ot->ot_curr_rec_id = result.sr_rec_id;
				ot->ot_curr_row_id = result.sr_row_id;
				ot->ot_ind_state = node->i_pos;

				/* Convert the pointer to a handle which can be used in later operations: */
				ASSERT_NS(!ot->ot_ind_rhandle);
				if (!(ot->ot_ind_rhandle = xt_ind_get_handle(ot, ind, &iref)))
					goto failed;
				/* Keep the node for next operations: */
				/*
				branch_size = XT_GET_INDEX_BLOCK_LEN(XT_GET_DISK_2(iref.ir_branch->tb_size_2));
				memcpy(&ot->ot_ind_rbuf, iref.ir_branch, branch_size);
				xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
				*/
				break;
			}
		}
	}
	else {
		ot->ot_curr_rec_id = result.sr_rec_id;
		ot->ot_curr_row_id = result.sr_row_id;
		ot->ot_ind_state = result.sr_item;

		/* Convert the pointer to a handle which can be used in later operations: */
		ASSERT_NS(!ot->ot_ind_rhandle);
		if (!(ot->ot_ind_rhandle = xt_ind_get_handle(ot, ind, &iref)))
			goto failed;
		/* Keep the node for next operations: */
		/*
		branch_size = XT_GET_INDEX_BLOCK_LEN(XT_GET_DISK_2(iref.ir_branch->tb_size_2));
		memcpy(&ot->ot_ind_rbuf, iref.ir_branch, branch_size);
		xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
		*/
	}

	done_ok:
	XT_INDEX_UNLOCK(ind, ot);

#ifdef DEBUG
	//idx_check_index(ot, ind, TRUE);
	//idx_check_on_key(ot);
#endif
	ASSERT_NS(iref.ir_xlock == 2);
	ASSERT_NS(iref.ir_updated == 2);
	return OK;

	failed:
	XT_INDEX_UNLOCK(ind, ot);
	if (idx_out_of_memory_failure(ot))
		goto retry_after_oom;
	ASSERT_NS(iref.ir_xlock == 2);
	ASSERT_NS(iref.ir_updated == 2);
	return FAILED;
}

xtPublic xtBool xt_idx_search_prev(XTOpenTablePtr ot, XTIndexPtr ind, register XTIdxSearchKeyPtr search_key)
{
	IdxBranchStackRec	stack;
	xtIndexNodeID		current;
	XTIndReferenceRec	iref;
	XTIdxResultRec		result;

#ifdef DEBUG
	iref.ir_xlock = 2;
	iref.ir_updated = 2;
#endif
	if (ot->ot_ind_rhandle) {
		xt_ind_release_handle(ot->ot_ind_rhandle, FALSE, ot->ot_thread);
		ot->ot_ind_rhandle = NULL;
	}
#ifdef DEBUG
	//idx_check_index(ot, ind, TRUE);
#endif

	/* see the comment above in xt_idx_search */
	/*
	if (!ot->ot_thread->st_xact_data) {
		xt_register_xterr(XT_REG_CONTEXT, XT_ERR_NO_TRANSACTION);
		return FAILED;
	}
	*/

	retry_after_oom:
#ifdef XT_TRACK_INDEX_UPDATES
	ot->ot_ind_changed = 0;
#endif
	idx_newstack(&stack);

	ot->ot_curr_rec_id = 0;
	ot->ot_curr_row_id = 0;

	XT_INDEX_READ_LOCK(ind, ot);

	if (!(XT_NODE_ID(current) = XT_NODE_ID(ind->mi_root)))
		goto done_ok;

	while (XT_NODE_ID(current)) {
		if (!xt_ind_fetch(ot, ind, current, XT_LOCK_READ, &iref))
			goto failed;
		ind->mi_scan_branch(ot->ot_table, ind, iref.ir_branch, &search_key->sk_key_value, &result);
		if (result.sr_found)
			/* If we have found the key in a node: */
			search_key->sk_on_key = TRUE;
		if (!result.sr_item.i_node_ref_size)
			break;
		xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
		if (!idx_push(&stack, current, &result.sr_item))
			goto failed;
		current = result.sr_branch;
	}

	if (result.sr_item.i_item_offset == 0) {
		IdxStackItemPtr node;

		search_up_stack:
		/* We are at the start of a leaf node.
		 * Go up the stack to find the start position of the next key.
		 * If we find none, then we are the end of the index.
		 */
		xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
		while ((node = idx_pop(&stack))) {
			if (node->i_pos.i_item_offset > node->i_pos.i_node_ref_size) {
				if (!xt_ind_fetch(ot, ind, node->i_branch, XT_LOCK_READ, &iref))
					goto failed;
				result.sr_item = node->i_pos;
				ind->mi_prev_item(ot->ot_table, ind, iref.ir_branch, &result);

				if (ind->mi_lazy_delete) {
					if (result.sr_row_id == (xtRowID) -1) {
						/* Go down to the bottom, in order to scan the leaf backwards: */
						current = node->i_branch;
						while (XT_NODE_ID(current)) {
							xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
							if (!idx_push(&stack, current, &result.sr_item))
								goto failed;
							current = result.sr_branch;
							if (!xt_ind_fetch(ot, ind, current, XT_LOCK_READ, &iref))
								goto failed;
							ind->mi_last_item(ot->ot_table, ind, iref.ir_branch, &result);
							if (!result.sr_item.i_node_ref_size)
								break;
						}

						/* If the leaf empty we have to go up the stack again... */
						if (result.sr_item.i_total_size == 0)
							goto search_up_stack;

						goto scan_back_in_leaf;
					}
				}

				goto record_found;
			}
		}
		goto done_ok;
	}

	/* We must just step once to the left in this leaf node... */
	ind->mi_prev_item(ot->ot_table, ind, iref.ir_branch, &result);

	if (ind->mi_lazy_delete) {
		scan_back_in_leaf:
		while (result.sr_row_id == (xtRowID) -1) {
			if (result.sr_item.i_item_offset == 0)
				goto search_up_stack;
			ind->mi_prev_item(ot->ot_table, ind, iref.ir_branch, &result);
		}
		idx_still_on_key(ind, search_key, iref.ir_branch, &result.sr_item);
	}

	record_found:
	ot->ot_curr_rec_id = result.sr_rec_id;
	ot->ot_curr_row_id = result.sr_row_id;
	ot->ot_ind_state = result.sr_item;

	/* Convert to handle for later operations: */
	ASSERT_NS(!ot->ot_ind_rhandle);
	if (!(ot->ot_ind_rhandle = xt_ind_get_handle(ot, ind, &iref)))
		goto failed;
	/* Keep a copy of the node for previous operations... */
	/*
	u_int branch_size;

	branch_size = XT_GET_INDEX_BLOCK_LEN(XT_GET_DISK_2(iref.ir_branch->tb_size_2));
	memcpy(&ot->ot_ind_rbuf, iref.ir_branch, branch_size);
	xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
	*/

	done_ok:
	XT_INDEX_UNLOCK(ind, ot);

#ifdef DEBUG
	//idx_check_index(ot, ind, TRUE);
	//idx_check_on_key(ot);
#endif
	return OK;

	failed:
	XT_INDEX_UNLOCK(ind, ot);
	if (idx_out_of_memory_failure(ot))
		goto retry_after_oom;
	return FAILED;
}

/*
 * Copy the current index value to the record.
 */
xtPublic xtBool xt_idx_read(XTOpenTablePtr ot, XTIndexPtr ind, xtWord1 *rec_buf)
{
	xtWord1	*bitem;

#ifdef DEBUG
	//idx_check_on_key(ot);
#endif
	xt_ind_lock_handle(ot->ot_ind_rhandle);
	bitem = ot->ot_ind_rhandle->ih_branch->tb_data + ot->ot_ind_state.i_item_offset;
	myxt_create_row_from_key(ot, ind, bitem, ot->ot_ind_state.i_item_size - XT_RECORD_REF_SIZE, rec_buf);
	xt_ind_unlock_handle(ot->ot_ind_rhandle);
	return OK;
}

xtPublic xtBool xt_idx_next(register XTOpenTablePtr ot, register XTIndexPtr ind, register XTIdxSearchKeyPtr search_key)
{
	XTIdxKeyValueRec	key_value;
	xtWord1				key_buf[XT_INDEX_MAX_KEY_SIZE];
	XTIdxResultRec		result;
	IdxBranchStackRec	stack;
	xtIndexNodeID		current;
	XTIndReferenceRec	iref;

#ifdef DEBUG
	iref.ir_xlock = 2;
	iref.ir_updated = 2;
#endif
	ASSERT_NS(ot->ot_ind_rhandle);
	xt_ind_lock_handle(ot->ot_ind_rhandle);
	result.sr_item = ot->ot_ind_state;
	if (!result.sr_item.i_node_ref_size && 
		result.sr_item.i_item_offset < result.sr_item.i_total_size && 
		ot->ot_ind_rhandle->ih_cache_reference) {
		XTIdxItemRec prev_item;

		key_value.sv_key = &ot->ot_ind_rhandle->ih_branch->tb_data[result.sr_item.i_item_offset];
		key_value.sv_length = result.sr_item.i_item_size - XT_RECORD_REF_SIZE;

		prev_item = result.sr_item;
		idx_next_branch_item(ot->ot_table, ind, ot->ot_ind_rhandle->ih_branch, &result);

		if (ind->mi_lazy_delete) {
			while (result.sr_item.i_item_offset < result.sr_item.i_total_size) {
				if (result.sr_row_id != (xtRowID) -1)
					break;
				prev_item = result.sr_item;
				idx_next_branch_item(ot->ot_table, ind, ot->ot_ind_rhandle->ih_branch, &result);
			}
		}

		if (result.sr_item.i_item_offset < result.sr_item.i_total_size) {
			/* Still on key? */
			idx_still_on_key(ind, search_key, ot->ot_ind_rhandle->ih_branch, &result.sr_item);
			xt_ind_unlock_handle(ot->ot_ind_rhandle);
			goto checked_on_key;
		}

		result.sr_item = prev_item;
	}

	key_value.sv_flags = XT_SEARCH_WHOLE_KEY;
	xt_get_record_ref(&ot->ot_ind_rhandle->ih_branch->tb_data[result.sr_item.i_item_offset + result.sr_item.i_item_size - XT_RECORD_REF_SIZE], &key_value.sv_rec_id, &key_value.sv_row_id);
	key_value.sv_key = key_buf;
	key_value.sv_length = result.sr_item.i_item_size - XT_RECORD_REF_SIZE;
	memcpy(key_buf, &ot->ot_ind_rhandle->ih_branch->tb_data[result.sr_item.i_item_offset], key_value.sv_length);
	xt_ind_release_handle(ot->ot_ind_rhandle, TRUE, ot->ot_thread);
	ot->ot_ind_rhandle = NULL;

	retry_after_oom:
#ifdef XT_TRACK_INDEX_UPDATES
	ot->ot_ind_changed = 0;
#endif
	idx_newstack(&stack);

	XT_INDEX_READ_LOCK(ind, ot);

	if (!(XT_NODE_ID(current) = XT_NODE_ID(ind->mi_root))) {
		XT_INDEX_UNLOCK(ind, ot);
		return OK;
	}

	while (XT_NODE_ID(current)) {
		if (!xt_ind_fetch(ot, ind, current, XT_LOCK_READ, &iref))
			goto failed;
		ind->mi_scan_branch(ot->ot_table, ind, iref.ir_branch, &key_value, &result);
		if (result.sr_item.i_node_ref_size) {
			if (result.sr_found) {
				/* If we have found the key in a node: */
				idx_next_branch_item(ot->ot_table, ind, iref.ir_branch, &result);

				/* Go down to the bottom: */
				while (XT_NODE_ID(current)) {
					xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
					if (!idx_push(&stack, current, &result.sr_item))
						goto failed;
					current = result.sr_branch;
					if (!xt_ind_fetch(ot, ind, current, XT_LOCK_READ, &iref))
						goto failed;
					idx_first_branch_item(ot->ot_table, ind, iref.ir_branch, &result);
					if (!result.sr_item.i_node_ref_size)
						break;
				}

				/* Is the leaf not empty, then we are done... */
				break;
			}
		}
		else {
			/* We have reached the leaf. */
			if (result.sr_found)
				/* If we have found the key in a leaf: */
				idx_next_branch_item(ot->ot_table, ind, iref.ir_branch, &result);
			/* If we did not find the key (although we should have). Our
			 * position is automatically the next one.
			 */
			break;
		}
		xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
		if (!idx_push(&stack, current, &result.sr_item))
			goto failed;
		current = result.sr_branch;
	}

	if (ind->mi_lazy_delete) {
		ignore_lazy_deleted_items:
		while (result.sr_item.i_item_offset < result.sr_item.i_total_size) {
			if (result.sr_row_id != (xtRowID) -1)
				break;
			idx_next_branch_item(NULL, ind, iref.ir_branch, &result);
		}
	}

	/* Check the current position in a leaf: */
	if (result.sr_item.i_item_offset == result.sr_item.i_total_size) {
		/* At the end: */
		IdxStackItemPtr node;

		/* We are at the end of a leaf node.
		 * Go up the stack to find the start poition of the next key.
		 * If we find none, then we are the end of the index.
		 */
		xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
		while ((node = idx_pop(&stack))) {
			if (node->i_pos.i_item_offset < node->i_pos.i_total_size) {
				if (!xt_ind_fetch(ot, ind, node->i_branch, XT_LOCK_READ, &iref))
					goto failed;
				result.sr_item = node->i_pos;
				xt_get_res_record_ref(&iref.ir_branch->tb_data[result.sr_item.i_item_offset + result.sr_item.i_item_size - XT_RECORD_REF_SIZE], &result);

				if (ind->mi_lazy_delete) {
					if (result.sr_row_id == (xtRowID) -1) {
						/* If this node pointer is lazy deleted, then
						 * go down the next branch...
						 */
						idx_next_branch_item(ot->ot_table, ind, iref.ir_branch, &result);

						/* Go down to the bottom: */
						current = node->i_branch;
						while (XT_NODE_ID(current)) {
							xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
							if (!idx_push(&stack, current, &result.sr_item))
								goto failed;
							current = result.sr_branch;
							if (!xt_ind_fetch(ot, ind, current, XT_LOCK_READ, &iref))
								goto failed;
							idx_first_branch_item(ot->ot_table, ind, iref.ir_branch, &result);
							if (!result.sr_item.i_node_ref_size)
								break;
						}

						/* And scan the leaf... */
						goto ignore_lazy_deleted_items;
					}
				}

				goto unlock_check_on_key;
			}
		}

		/* No more keys: */
		if (search_key)
			search_key->sk_on_key = FALSE;
		ot->ot_curr_rec_id = 0;
		ot->ot_curr_row_id = 0;
		XT_INDEX_UNLOCK(ind, ot);
		return OK;
	}

	unlock_check_on_key:

	ASSERT_NS(!ot->ot_ind_rhandle);
	if (!(ot->ot_ind_rhandle = xt_ind_get_handle(ot, ind, &iref)))
		goto failed;
	/*
	u_int branch_size;

	branch_size = XT_GET_INDEX_BLOCK_LEN(XT_GET_DISK_2(iref.ir_branch->tb_size_2));
	memcpy(&ot->ot_ind_rbuf, iref.ir_branch, branch_size);
	xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
	*/

	XT_INDEX_UNLOCK(ind, ot);

	/* Still on key? */
	if (search_key && search_key->sk_on_key) {
		/* GOTCHA: As a short-cut I was using a length compare
		 * and a memcmp() here to check whether we as still on
		 * the original search key.
		 * This does not work because it does not take into account
		 * trialing spaces (which are ignored in comparison).
		 * So lengths can be different, but values still equal.
		 * 
		 * NOTE: We have to use the original search flags for
		 * this compare.
		 */
		xt_ind_lock_handle(ot->ot_ind_rhandle);
		search_key->sk_on_key = myxt_compare_key(ind, search_key->sk_key_value.sv_flags, search_key->sk_key_value.sv_length,
			search_key->sk_key_value.sv_key, &ot->ot_ind_rhandle->ih_branch->tb_data[result.sr_item.i_item_offset]) == 0;
		xt_ind_unlock_handle(ot->ot_ind_rhandle);
	}

	checked_on_key:
	ot->ot_curr_rec_id = result.sr_rec_id;
	ot->ot_curr_row_id = result.sr_row_id;
	ot->ot_ind_state = result.sr_item;

	return OK;

	failed:
	XT_INDEX_UNLOCK(ind, ot);
	if (idx_out_of_memory_failure(ot))
		goto retry_after_oom;
	return FAILED;
}

xtPublic xtBool xt_idx_prev(register XTOpenTablePtr ot, register XTIndexPtr ind, register XTIdxSearchKeyPtr search_key)
{
	XTIdxKeyValueRec	key_value;
	xtWord1				key_buf[XT_INDEX_MAX_KEY_SIZE];
	XTIdxResultRec		result;
	IdxBranchStackRec	stack;
	xtIndexNodeID		current;
	XTIndReferenceRec	iref;
	IdxStackItemPtr		node;

#ifdef DEBUG
	iref.ir_xlock = 2;
	iref.ir_updated = 2;
#endif
	ASSERT_NS(ot->ot_ind_rhandle);
	xt_ind_lock_handle(ot->ot_ind_rhandle);
	result.sr_item = ot->ot_ind_state;
	if (!result.sr_item.i_node_ref_size && result.sr_item.i_item_offset > 0) {
		key_value.sv_key = &ot->ot_ind_rhandle->ih_branch->tb_data[result.sr_item.i_item_offset];
		key_value.sv_length = result.sr_item.i_item_size - XT_RECORD_REF_SIZE;

		ind->mi_prev_item(ot->ot_table, ind, ot->ot_ind_rhandle->ih_branch, &result);

		if (ind->mi_lazy_delete) {
			while (result.sr_row_id == (xtRowID) -1) {
				if (result.sr_item.i_item_offset == 0)
					goto research;
				ind->mi_prev_item(ot->ot_table, ind, ot->ot_ind_rhandle->ih_branch, &result);
			}
		}

		idx_still_on_key(ind, search_key, ot->ot_ind_rhandle->ih_branch, &result.sr_item);

		xt_ind_unlock_handle(ot->ot_ind_rhandle);
		goto checked_on_key;
	}

	research:
	key_value.sv_flags = XT_SEARCH_WHOLE_KEY;
	key_value.sv_rec_id = ot->ot_curr_rec_id;
	key_value.sv_row_id = 0;
	key_value.sv_key = key_buf;
	key_value.sv_length = result.sr_item.i_item_size - XT_RECORD_REF_SIZE;
	memcpy(key_buf, &ot->ot_ind_rhandle->ih_branch->tb_data[result.sr_item.i_item_offset], key_value.sv_length);
	xt_ind_release_handle(ot->ot_ind_rhandle, TRUE, ot->ot_thread);
	ot->ot_ind_rhandle = NULL;

	retry_after_oom:
#ifdef XT_TRACK_INDEX_UPDATES
	ot->ot_ind_changed = 0;
#endif
	idx_newstack(&stack);

	XT_INDEX_READ_LOCK(ind, ot);

	if (!(XT_NODE_ID(current) = XT_NODE_ID(ind->mi_root))) {
		XT_INDEX_UNLOCK(ind, ot);
		return OK;
	}

	while (XT_NODE_ID(current)) {
		if (!xt_ind_fetch(ot, ind, current, XT_LOCK_READ, &iref))
			goto failed;
		ind->mi_scan_branch(ot->ot_table, ind, iref.ir_branch, &key_value, &result);
		if (result.sr_item.i_node_ref_size) {
			if (result.sr_found) {
				/* If we have found the key in a node: */

				search_down_stack:
				/* Go down to the bottom: */
				while (XT_NODE_ID(current)) {
					xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
					if (!idx_push(&stack, current, &result.sr_item))
						goto failed;
					current = result.sr_branch;
					if (!xt_ind_fetch(ot, ind, current, XT_LOCK_READ, &iref))
						goto failed;
					ind->mi_last_item(ot->ot_table, ind, iref.ir_branch, &result);
					if (!result.sr_item.i_node_ref_size)
						break;
				}

				/* If the leaf empty we have to go up the stack again... */
				if (result.sr_item.i_total_size == 0)
					break;

				if (ind->mi_lazy_delete) {
					while (result.sr_row_id == (xtRowID) -1) {
						if (result.sr_item.i_item_offset == 0)
							goto search_up_stack;
						ind->mi_prev_item(ot->ot_table, ind, iref.ir_branch, &result);
					}
				}

				goto unlock_check_on_key;
			}
		}
		else {
			/* We have reached the leaf.
			 * Whether we found the key or not, we have
			 * to move one to the left.
			 */
			if (result.sr_item.i_item_offset == 0)
				break;
			ind->mi_prev_item(ot->ot_table, ind, iref.ir_branch, &result);

			if (ind->mi_lazy_delete) {
				while (result.sr_row_id == (xtRowID) -1) {
					if (result.sr_item.i_item_offset == 0)
						goto search_up_stack;
					ind->mi_prev_item(ot->ot_table, ind, iref.ir_branch, &result);
				}
			}

			goto unlock_check_on_key;
		}
		xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
		if (!idx_push(&stack, current, &result.sr_item))
			goto failed;
		current = result.sr_branch;
	}

	search_up_stack:
	/* We are at the start of a leaf node.
	 * Go up the stack to find the start poition of the next key.
	 * If we find none, then we are the end of the index.
	 */
	xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
	while ((node = idx_pop(&stack))) {
		if (node->i_pos.i_item_offset > node->i_pos.i_node_ref_size) {
			if (!xt_ind_fetch(ot, ind, node->i_branch, XT_LOCK_READ, &iref))
				goto failed;
			result.sr_item = node->i_pos;
			ind->mi_prev_item(ot->ot_table, ind, iref.ir_branch, &result);

			if (ind->mi_lazy_delete) {
				if (result.sr_row_id == (xtRowID) -1) {
					current = node->i_branch;
					goto search_down_stack;
				}
			}

			goto unlock_check_on_key;
		}
	}

	/* No more keys: */
	if (search_key)
		search_key->sk_on_key = FALSE;
	ot->ot_curr_rec_id = 0;
	ot->ot_curr_row_id = 0;

	XT_INDEX_UNLOCK(ind, ot);
	return OK;

	unlock_check_on_key:
	ASSERT_NS(!ot->ot_ind_rhandle);
	if (!(ot->ot_ind_rhandle = xt_ind_get_handle(ot, ind, &iref)))
		goto failed;
	/*
	u_int branch_size;

	branch_size = XT_GET_INDEX_BLOCK_LEN(XT_GET_DISK_2(iref.ir_branch->tb_size_2));
	memcpy(&ot->ot_ind_rbuf, iref.ir_branch, branch_size);
	xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
	*/

	XT_INDEX_UNLOCK(ind, ot);

	/* Still on key? */
	if (search_key && search_key->sk_on_key) {
		xt_ind_lock_handle(ot->ot_ind_rhandle);
		search_key->sk_on_key = myxt_compare_key(ind, search_key->sk_key_value.sv_flags, search_key->sk_key_value.sv_length,
			search_key->sk_key_value.sv_key, &ot->ot_ind_rhandle->ih_branch->tb_data[result.sr_item.i_item_offset]) == 0;
		xt_ind_unlock_handle(ot->ot_ind_rhandle);
	}

	checked_on_key:
	ot->ot_curr_rec_id = result.sr_rec_id;
	ot->ot_curr_row_id = result.sr_row_id;
	ot->ot_ind_state = result.sr_item;
	return OK;

	failed:
	XT_INDEX_UNLOCK(ind, ot);
	if (idx_out_of_memory_failure(ot))
		goto retry_after_oom;
	return FAILED;
}

/* Return TRUE if the record matches the current index search! */
xtPublic xtBool xt_idx_match_search(register XTOpenTablePtr XT_UNUSED(ot), register XTIndexPtr ind, register XTIdxSearchKeyPtr search_key, xtWord1 *buf, int mode)
{
	int		r;
	xtWord1	key_buf[XT_INDEX_MAX_KEY_SIZE];

	myxt_create_key_from_row(ind, key_buf, (xtWord1 *) buf, NULL);
	r = myxt_compare_key(ind, search_key->sk_key_value.sv_flags, search_key->sk_key_value.sv_length, search_key->sk_key_value.sv_key, key_buf);
	switch (mode) {
		case XT_S_MODE_MATCH:
			return r == 0;
		case XT_S_MODE_NEXT:
			return r <= 0;
		case XT_S_MODE_PREV:
			return r >= 0;
	}
	return FALSE;
}

static void idx_set_index_selectivity(XTOpenTablePtr ot, XTIndexPtr ind, XTThreadPtr thread)
{
	static const xtRecordID MAX_RECORDS = 100;

	XTIdxSearchKeyRec	search_key;
	XTIndexSegPtr		key_seg;
	u_int				select_count[2] = {0, 0};
	xtWord1				key_buf[XT_INDEX_MAX_KEY_SIZE];
	u_int				key_len;
	xtWord1				*next_key_buf;
	u_int				next_key_len;
	u_int				curr_len;
	u_int				diff;
	u_int				j, i;
	/* these 2 vars are used to check the overlapping if we have < 200 records */
	xtRecordID			last_rec = 0;		/* last record accounted in this iteration */
	xtRecordID			last_iter_rec = 0;	/* last record accounted in the previous iteration */

	xtBool	(* xt_idx_iterator[2])(
		register struct XTOpenTable *ot, register struct XTIndex *ind, register XTIdxSearchKeyPtr search_key) = {

		xt_idx_next,
		xt_idx_prev
	};

	xtBool	(* xt_idx_begin[2])(
		struct XTOpenTable *ot, struct XTIndex *ind, register XTIdxSearchKeyPtr search_key) = {
	
		xt_idx_search,
		xt_idx_search_prev
	};

	ind->mi_select_total = 0;
	key_seg = ind->mi_seg;
	for (i=0; i < ind->mi_seg_count; key_seg++, i++) {
		key_seg->is_selectivity = 1;
		key_seg->is_recs_in_range = 1;
	}

	for (j=0; j < 2; j++) {
		xt_idx_prep_key(ind, &search_key, j == 0 ? XT_SEARCH_FIRST_FLAG : XT_SEARCH_AFTER_LAST_FLAG, NULL, 0);
		if (!(xt_idx_begin[j])(ot, ind, &search_key))
			goto failed;

		/* Initialize the buffer with the first index valid index entry: */
		while (!select_count[j] && ot->ot_curr_rec_id != last_iter_rec) {
			if (ot->ot_curr_row_id) {
				select_count[j]++;
				last_rec = ot->ot_curr_rec_id;

				key_len = ot->ot_ind_state.i_item_size - XT_RECORD_REF_SIZE;
				xt_ind_lock_handle(ot->ot_ind_rhandle);
				memcpy(key_buf, ot->ot_ind_rhandle->ih_branch->tb_data + ot->ot_ind_state.i_item_offset, key_len);
				xt_ind_unlock_handle(ot->ot_ind_rhandle);
			}
			if (!(xt_idx_iterator[j])(ot, ind, &search_key))
				goto failed_1;
		}

		while (select_count[j] < MAX_RECORDS && ot->ot_curr_rec_id != last_iter_rec) {
			/* Check if the index entry is committed: */
			if (ot->ot_curr_row_id) {
				xt_ind_lock_handle(ot->ot_ind_rhandle);
				select_count[j]++;
				last_rec = ot->ot_curr_rec_id;

				next_key_len = ot->ot_ind_state.i_item_size - XT_RECORD_REF_SIZE;
				next_key_buf = ot->ot_ind_rhandle->ih_branch->tb_data + ot->ot_ind_state.i_item_offset;
			
				curr_len = 0;
				diff = FALSE;
				key_seg = ind->mi_seg;
				for (i=0; i < ind->mi_seg_count; key_seg++, i++) {
					curr_len += myxt_key_seg_length(key_seg, curr_len, key_buf);
					if (!diff && myxt_compare_key(ind, 0, curr_len, key_buf, next_key_buf) != 0)
						diff = i+1;
					if (diff)
						key_seg->is_selectivity++;
				}

				/* Store the key for the next comparison: */
				key_len = next_key_len;
				memcpy(key_buf, next_key_buf, key_len);
				xt_ind_unlock_handle(ot->ot_ind_rhandle);
			}

			if (!(xt_idx_iterator[j])(ot, ind, &search_key))
				goto failed_1;
		}

		last_iter_rec = last_rec;

		if (ot->ot_ind_rhandle) {
			xt_ind_release_handle(ot->ot_ind_rhandle, FALSE, thread);
			ot->ot_ind_rhandle = NULL;
		}
	}

	u_int select_total;

	select_total = select_count[0] + select_count[1];
	if (select_total) {
		u_int recs;

		ind->mi_select_total = select_total;
		key_seg = ind->mi_seg;
		for (i=0; i < ind->mi_seg_count; key_seg++, i++) {
			recs = (u_int) ((double) select_total / (double) key_seg->is_selectivity + (double) 0.5);
			key_seg->is_recs_in_range = recs ? recs : 1;
		}
	}
	return;

	failed_1:
	if (ot->ot_ind_rhandle) {
		xt_ind_release_handle(ot->ot_ind_rhandle, FALSE, thread);
		ot->ot_ind_rhandle = NULL;
	}

	failed:
	xt_tab_disable_index(ot->ot_table, XT_INDEX_CORRUPTED);
	xt_log_and_clear_exception_ns();
	return;
}

xtPublic void xt_ind_set_index_selectivity(XTOpenTablePtr ot, XTThreadPtr thread)
{
	XTTableHPtr		tab = ot->ot_table;
	XTIndexPtr		*ind;
	u_int			i;
	time_t			now;

	now = time(NULL);
	xt_lock_mutex_ns(&tab->tab_ind_stat_lock);
	if (tab->tab_ind_stat_calc_time < now) {
		if (!tab->tab_dic.dic_disable_index) {
			for (i=0, ind=tab->tab_dic.dic_keys; i<tab->tab_dic.dic_key_count; i++, ind++)
				idx_set_index_selectivity(ot, *ind, thread);
		}
		tab->tab_ind_stat_calc_time = time(NULL);
	}
	xt_unlock_mutex_ns(&tab->tab_ind_stat_lock);
}

/*
 * -----------------------------------------------------------------------
 * Print a b-tree
 */

#ifdef TEST_CODE
static void idx_check_on_key(XTOpenTablePtr ot)
{
	u_int		offs = ot->ot_ind_state.i_item_offset + ot->ot_ind_state.i_item_size - XT_RECORD_REF_SIZE;
	xtRecordID	rec_id;
	xtRowID		row_id;
	
	if (ot->ot_curr_rec_id && ot->ot_ind_state.i_item_offset < ot->ot_ind_state.i_total_size) {
		xt_get_record_ref(&ot->ot_ind_rbuf.tb_data[offs], &rec_id, &row_id);
		
		ASSERT_NS(rec_id == ot->ot_curr_rec_id);
	}
}
#endif

static void idx_check_space(int depth)
{
#ifdef DUMP_INDEX
	for (int i=0; i<depth; i++)
		printf(". ");
#endif
}

#ifdef DO_COMP_TEST

//#define FILL_COMPRESS_BLOCKS

#ifdef FILL_COMPRESS_BLOCKS
#define COMPRESS_BLOCK_SIZE			(1024 * 128)
#else
#define COMPRESS_BLOCK_SIZE			16384
#endif

int blocks;
int usage_total;

int zlib_1024;
int zlib_2048;
int zlib_4096;
int zlib_8192;
int zlib_16384;
int zlib_total;
int zlib_time;
int read_time;
int uncomp_time;
int fill_size;
int filled_size;
unsigned char out[COMPRESS_BLOCK_SIZE];
unsigned char uncomp[COMPRESS_BLOCK_SIZE];
xtWord1 precomp[COMPRESS_BLOCK_SIZE+16000];

u_int idx_precompress(XTIndexPtr ind, u_int node_ref_size, u_int item_size, u_int insize, xtWord1 *in_data, xtWord1 *out_data)
{
	xtWord1 *prev_item = NULL;
	xtWord1 *in_ptr = in_data;
	xtWord1 *out_ptr = out_data;
	u_int	out_size = 0;
	u_int	same_size;

	if (insize >= node_ref_size) {
		memcpy(out_ptr, in_ptr, node_ref_size);
		insize -= node_ref_size;
		out_size += node_ref_size;
		in_ptr += node_ref_size;
		out_ptr += node_ref_size;
	}

	while (insize >= item_size + node_ref_size) {
		if (prev_item) {
			same_size = 0;
			while (same_size < item_size + node_ref_size && *prev_item == *in_ptr) {
				same_size++;
				prev_item++;
				in_ptr++;
			}
			ASSERT_NS(same_size < 256);
			*out_ptr = (xtWord1) same_size;
			out_size++;
			out_ptr++;
			same_size = item_size + node_ref_size - same_size;
			memcpy(out_ptr, in_ptr, same_size);
			out_size += same_size;
			out_ptr += same_size;
			in_ptr += same_size;
			prev_item += same_size;
		}
		else {
			prev_item = in_ptr;
			memcpy(out_ptr, in_ptr, item_size + node_ref_size);
			out_size += item_size + node_ref_size;
			out_ptr += item_size + node_ref_size;
			in_ptr += item_size + node_ref_size;
		}
		insize -= (item_size + node_ref_size);
	}
	return out_size;
}

u_int idx_compress(u_int insize, xtWord1 *in_data, u_int outsize, xtWord1 *out_data)
{
	z_stream strm;
	int ret;

	strm.zalloc = Z_NULL;
	strm.zfree = Z_NULL;
	strm.opaque = Z_NULL;
	strm.avail_in = 0;
	strm.next_in = Z_NULL;
	ret = deflateInit(&strm, Z_DEFAULT_COMPRESSION);
	strm.avail_out = outsize;
	strm.next_out = out_data;
	strm.avail_in = insize;
	strm.next_in = in_data;
	ret = deflate(&strm, Z_FINISH);
	deflateEnd(&strm);
	return outsize - strm.avail_out;

/*
	bz_stream strm;
	int ret;

	memset(&strm, 0, sizeof(strm));

	ret = BZ2_bzCompressInit(&strm, 1, 0, 0);
	strm.avail_out = outsize;
	strm.next_out = (char *) out_data;
	strm.avail_in = insize;
	strm.next_in = (char *) in_data;
	ret = BZ2_bzCompress(&strm, BZ_FINISH);

	BZ2_bzCompressEnd(&strm);
	return outsize - strm.avail_out;
*/
}

u_int idx_decompress(u_int insize, xtWord1 *in_data, u_int outsize, xtWord1 *out_data)
{
	z_stream strm;
	int ret;

	strm.zalloc = Z_NULL;
	strm.zfree = Z_NULL;
	strm.opaque = Z_NULL;
	strm.avail_in = 0;
	strm.next_in = Z_NULL;
	ret = inflateInit(&strm);
	strm.avail_out = outsize;
	strm.next_out = out_data;
	strm.avail_in = insize;
	strm.next_in = in_data;
	ret = inflate(&strm, Z_FINISH);
	inflateEnd(&strm);
	return outsize - strm.avail_out;

/*
	bz_stream strm;
	int ret;

	memset(&strm, 0, sizeof(strm));

	ret = BZ2_bzDecompressInit(&strm, 0, 0);
	strm.avail_out = outsize;
	strm.next_out = (char *) out_data;
	strm.avail_in = insize;
	strm.next_in = (char *) in_data;
	ret = BZ2_bzDecompress(&strm);

	BZ2_bzDecompressEnd(&strm);
	return outsize - strm.avail_out;
*/
}
#endif // DO_COMP_TEST

static u_int idx_check_node(XTOpenTablePtr ot, XTIndexPtr ind, int depth, xtIndexNodeID node)
{
	XTIdxResultRec		result;
	u_int				block_count = 1;
	XTIndReferenceRec	iref;

#ifdef DO_COMP_TEST
	unsigned comp_size;
	unsigned uncomp_size;
	xtWord8 now;
	xtWord8 time;
#endif

#ifdef DEBUG
	iref.ir_xlock = 2;
	iref.ir_updated = 2;
#endif
	ASSERT_NS(XT_NODE_ID(node) <= XT_NODE_ID(ot->ot_table->tab_ind_eof));
#ifdef DO_COMP_TEST
	now = xt_trace_clock();
#endif
	/* A deadlock can occur when taking a read lock
	 * because the XT_IPAGE_WRITE_TRY_LOCK(&block->cb_lock, ot->ot_thread->t_id)
	 * only takes into account WRITE locks.
	 * So, if we hold a READ lock on a page, and ind_free_block() trys to
	 * free the block, it hangs on its own read lock!
	 *
	 * So we change from READ lock to a WRITE lock.
	 * If too restrictive then locks need to handle TRY on a
	 * read lock as well.
	 *
	 * #3	0x00e576b6 in xt_yield at thread_xt.cc:1351
	 * #4	0x00e7218e in xt_spinxslock_xlock at lock_xt.cc:1467
	 * #5	0x00dee1a9 in ind_free_block at cache_xt.cc:901
	 * #6	0x00dee500 in ind_cac_free_lru_blocks at cache_xt.cc:1054
	 * #7	0x00dee88c in ind_cac_fetch at cache_xt.cc:1151
	 * #8	0x00def6d4 in xt_ind_fetch at cache_xt.cc:1480
	 * #9	0x00e1ce2e in idx_check_node at index_xt.cc:3996
	 * #10	0x00e1cf2b in idx_check_node at index_xt.cc:4106
	 * #11	0x00e1cf2b in idx_check_node at index_xt.cc:4106
	 * #12	0x00e1cfdc in idx_check_index at index_xt.cc:4130
	 * #13	0x00e1d11c in xt_check_indices at index_xt.cc:4181
	 * #14	0x00e4aa82 in xt_check_table at table_xt.cc:2363
	 */
	if (!xt_ind_fetch(ot, ind, node, XT_LOCK_WRITE, &iref))
		return 0;
#ifdef DO_COMP_TEST
	time = xt_trace_clock() - now;
	read_time += time;
#endif

	idx_first_branch_item(ot->ot_table, ind, iref.ir_branch, &result);
	ASSERT_NS(result.sr_item.i_total_size + offsetof(XTIdxBranchDRec, tb_data) <= XT_INDEX_PAGE_SIZE);

#ifdef DO_COMP_TEST
	u_int size = result.sr_item.i_total_size;
	xtWord1 *data = iref.ir_branch->tb_data;

/*
	size = idx_precompress(ind, result.sr_item.i_node_ref_size, result.sr_item.i_item_size, size, data, precomp);
	if (size > result.sr_item.i_total_size)
		size = result.sr_item.i_total_size;
	else
		data = precomp;
*/

	blocks++;
	usage_total += result.sr_item.i_total_size;

#ifdef FILL_COMPRESS_BLOCKS
	if (fill_size + size > COMPRESS_BLOCK_SIZE) {
		now = xt_trace_clock();
		comp_size = idx_compress(fill_size, precomp, COMPRESS_BLOCK_SIZE, out);
		time = xt_trace_clock() - now;
		zlib_time += time;

		zlib_total += comp_size;
		filled_size += fill_size;

		now = xt_trace_clock();
		uncomp_size = idx_decompress(comp_size, out, COMPRESS_BLOCK_SIZE, uncomp);
		time = xt_trace_clock() - now;
		uncomp_time += time;

		if (uncomp_size != fill_size)
			printf("what?\n");

		fill_size = 0;
	}
	memcpy(precomp + fill_size, data, size);
	fill_size += size;
#else
	now = xt_trace_clock();
	comp_size = idx_compress(size, data, COMPRESS_BLOCK_SIZE, out);
	time = xt_trace_clock() - now;
	zlib_time += time;
	zlib_total += comp_size;

	now = xt_trace_clock();
	uncomp_size = idx_decompress(comp_size, out, COMPRESS_BLOCK_SIZE, uncomp);
	time = xt_trace_clock() - now;
	uncomp_time += time;
	if (uncomp_size != size)
		printf("what?\n");
#endif

	if (comp_size <= 1024)
		zlib_1024++;
	else if (comp_size <= 2048)
		zlib_2048++;
	else if (comp_size <= 4096)
		zlib_4096++;
	else if (comp_size <= 8192)
		zlib_8192++;
	else
		zlib_16384++;

#endif // DO_COMP_TEST

	if (result.sr_item.i_node_ref_size) {
		idx_check_space(depth);
#ifdef DUMP_INDEX
		printf("%04d -->\n", (int) XT_NODE_ID(result.sr_branch));
#endif
#ifdef TRACK_ACTIVITY
		track_block_exists(result.sr_branch);
#endif
		block_count += idx_check_node(ot, ind, depth+1, result.sr_branch);
	}

	while (result.sr_item.i_item_offset < result.sr_item.i_total_size) {
#ifdef CHECK_PRINTS_RECORD_REFERENCES
		idx_check_space(depth);
		if (result.sr_item.i_item_size == 12) {
			/* Assume this is a NOT-NULL INT!: */
			xtWord4 val = XT_GET_DISK_4(&iref.ir_branch->tb_data[result.sr_item.i_item_offset]);
#ifdef DUMP_INDEX
			printf("(%6d) ", (int) val);
#endif
		}
#ifdef DUMP_INDEX
		printf("rec=%d row=%d ", (int) result.sr_rec_id, (int) result.sr_row_id);
		printf("\n");
#endif
#endif
		idx_next_branch_item(ot->ot_table, ind, iref.ir_branch, &result);
		if (result.sr_item.i_node_ref_size) {
			idx_check_space(depth);
#ifdef DUMP_INDEX
			printf("%04d -->\n", (int) XT_NODE_ID(result.sr_branch));
#endif
#ifdef TRACK_ACTIVITY
			track_block_exists(result.sr_branch);
#endif
			block_count += idx_check_node(ot, ind, depth+1, result.sr_branch);
		}
	}

	xt_ind_release(ot, ind, XT_UNLOCK_WRITE, &iref);
	return block_count;
}

static u_int idx_check_index(XTOpenTablePtr ot, XTIndexPtr ind, xtBool with_lock)
{
	xtIndexNodeID			current;
	u_int					block_count = 0;
	u_int					i;

	if (with_lock)
		XT_INDEX_WRITE_LOCK(ind, ot);

#ifdef DUMP_INDEX
	printf("INDEX (%d) %04d ---------------------------------------\n", (int) ind->mi_index_no, (int) XT_NODE_ID(ind->mi_root));
#endif
	if ((XT_NODE_ID(current) = XT_NODE_ID(ind->mi_root))) {
#ifdef TRACK_ACTIVITY
		track_block_exists(ind->mi_root);
#endif
		block_count = idx_check_node(ot, ind, 0, current);
	}

	if (ind->mi_free_list && ind->mi_free_list->fl_free_count) {
#ifdef DUMP_INDEX
		printf("INDEX (%d) FREE ---------------------------------------", (int) ind->mi_index_no);
#endif
		ASSERT_NS(ind->mi_free_list->fl_start == 0);
		for (i=0; i<ind->mi_free_list->fl_free_count; i++) {
#ifdef DUMP_INDEX
			if ((i % 40) == 0)
				printf("\n");
#endif
			block_count++;
#ifdef TRACK_ACTIVITY
			track_block_exists(ind->mi_free_list->fl_page_id[i]);
#endif
#ifdef DUMP_INDEX
			printf("%2d ", (int) XT_NODE_ID(ind->mi_free_list->fl_page_id[i]));
#endif
		}
#ifdef DUMP_INDEX
		if ((i % 40) != 0)
			printf("\n");
#endif
	}

	if (with_lock)
		XT_INDEX_UNLOCK(ind, ot);
	return block_count;

}

xtPublic void xt_check_indices(XTOpenTablePtr ot)
{
	register XTTableHPtr	tab = ot->ot_table;
	XTIndexPtr				*ind;
	xtIndexNodeID			current;
	XTIndFreeBlockRec		free_block;
	u_int					ind_count, block_count = 0;
	u_int					free_count = 0;
	u_int					i, j;

	xt_lock_mutex_ns(&tab->tab_ind_flush_lock);
	printf("CHECK INDICES %s ==============================\n", tab->tab_name->ps_path);
#ifdef TRACK_ACTIVITY
	track_reset_missing();
#endif

	ind = tab->tab_dic.dic_keys;
	for (u_int k=0; k<tab->tab_dic.dic_key_count; k++, ind++) {
		ind_count = idx_check_index(ot, *ind, TRUE);
		block_count += ind_count;
	}

#ifdef DO_COMP_TEST
	int block_total;

#ifdef FILL_COMPRESS_BLOCKS
	if (fill_size > 0) {
		unsigned	comp_size;
		unsigned	uncomp_size;
		xtWord8		now;
		xtWord8		time;

		now = xt_trace_clock();
		comp_size = idx_compress(fill_size, precomp, COMPRESS_BLOCK_SIZE, out);
		time = xt_trace_clock() - now;
		zlib_time += time;
		zlib_total += comp_size;
		filled_size += fill_size;

		now = xt_trace_clock();
		uncomp_size = idx_decompress(comp_size, out, COMPRESS_BLOCK_SIZE, uncomp);
		time = xt_trace_clock() - now;
		uncomp_time += time;
	}
	if (filled_size != usage_total)
		printf("What?\n");
#endif

	printf("Total blocks  = %d\n", blocks);
	printf("zlib <=  1024 = %d\n", zlib_1024);
	printf("zlib <=  2048 = %d\n", zlib_2048);
	printf("zlib <=  4096 = %d\n", zlib_4096);
	printf("zlib <=  8192 = %d\n", zlib_8192);
	printf("zlib <= 16384 = %d\n", zlib_16384);
	printf("zlib average size = %.2f\n", (double) zlib_total / (double) blocks);
	printf("zlib average time = %.2f\n", (double) zlib_time / (double) blocks);
	printf("read average time = %.2f\n", (double) read_time / (double) blocks);
	printf("uncompress time   = %.2f\n", (double) uncomp_time / (double) blocks);
	block_total = (zlib_1024 + zlib_2048) * 8192;
	block_total += zlib_4096 * 8192;
	block_total += zlib_8192 * 8192;
	block_total += zlib_16384 * 16384;
	printf("block total       = %d\n", block_total);
	printf("block %% compress  = %.2f\n", ((double) block_total * (double) 100) / ((double) blocks * (double) 16384));
	printf("Total size        = %d\n", blocks * 16384);
	printf("total before zlib = %d\n", usage_total);
	printf("total after zlib  = %d\n", zlib_total);
	printf("zlib %% compress   = %.2f\n", ((double) zlib_total * (double) 100) / (double) usage_total);
	printf("total %% compress  = %.2f\n", ((double) zlib_total * (double) 100) / (double) (blocks * 16384));
#endif

	xt_lock_mutex_ns(&tab->tab_ind_lock);
#ifdef DUMP_INDEX
	printf("\nFREE: ---------------------------------------\n");
#endif
	if (tab->tab_ind_free_list) {
		XTIndFreeListPtr	ptr;

		ptr = tab->tab_ind_free_list;
		while (ptr) {
#ifdef DUMP_INDEX
			printf("Memory List:");
#endif
			i = 0;
			for (j=ptr->fl_start; j<ptr->fl_free_count; j++, i++) {
#ifdef DUMP_INDEX
				if ((i % 40) == 0)
					printf("\n");
#endif
				free_count++;
#ifdef TRACK_ACTIVITY
				track_block_exists(ptr->fl_page_id[j]);
#endif
#ifdef DUMP_INDEX
				printf("%2d ", (int) XT_NODE_ID(ptr->fl_page_id[j]));
#endif
			}
#ifdef DUMP_INDEX
			if ((i % 40) != 0)
				printf("\n");
#endif
			ptr = ptr->fl_next_list;
		}
	}

	current = tab->tab_ind_free;
	if (XT_NODE_ID(current)) {
		u_int k = 0;
#ifdef DUMP_INDEX
		printf("Disk List:");
#endif
		while (XT_NODE_ID(current)) {
#ifdef DUMP_INDEX
			if ((k % 40) == 0)
				printf("\n");
#endif
			free_count++;
#ifdef TRACK_ACTIVITY
			track_block_exists(current);
#endif
#ifdef DUMP_INDEX
			printf("%d ", (int) XT_NODE_ID(current));
#endif
			if (!xt_ind_read_bytes(ot, *ind, current, sizeof(XTIndFreeBlockRec), (xtWord1 *) &free_block)) {
				xt_log_and_clear_exception_ns();
				break;
			}
			XT_NODE_ID(current) = (xtIndexNodeID) XT_GET_DISK_8(free_block.if_next_block_8);
			k++;
		}
#ifdef DUMP_INDEX
		if ((k % 40) != 0)
			printf("\n");
#endif
	}
#ifdef DUMP_INDEX
	printf("\n-----------------------------\n");
	printf("used blocks %d + free blocks %d = %d\n", block_count, free_count, block_count + free_count);
	printf("EOF = %"PRIu64", total blocks = %d\n", (xtWord8) xt_ind_node_to_offset(tab, tab->tab_ind_eof), (int) (XT_NODE_ID(tab->tab_ind_eof) - 1));
	printf("-----------------------------\n");
#endif
	xt_unlock_mutex_ns(&tab->tab_ind_lock);
#ifdef TRACK_ACTIVITY
	track_dump_missing(tab->tab_ind_eof);
	printf("===================================================\n");
	track_dump_all((u_int) (XT_NODE_ID(tab->tab_ind_eof) - 1));
#endif
	printf("===================================================\n");
	xt_unlock_mutex_ns(&tab->tab_ind_flush_lock);
}

/*
 * -----------------------------------------------------------------------
 * Load index
 */

static void idx_load_node(XTThreadPtr self, XTOpenTablePtr ot, XTIndexPtr ind, xtIndexNodeID node)
{
	XTIdxResultRec		result;
	XTIndReferenceRec	iref;

	ASSERT_NS(XT_NODE_ID(node) <= XT_NODE_ID(ot->ot_table->tab_ind_eof));
	if (!xt_ind_fetch(ot, ind, node, XT_LOCK_READ, &iref))
		xt_throw(self);

	idx_first_branch_item(ot->ot_table, ind, iref.ir_branch, &result);
	if (result.sr_item.i_node_ref_size)
		idx_load_node(self, ot, ind, result.sr_branch);
	while (result.sr_item.i_item_offset < result.sr_item.i_total_size) {
		idx_next_branch_item(ot->ot_table, ind, iref.ir_branch, &result);
		if (result.sr_item.i_node_ref_size)
			idx_load_node(self, ot, ind, result.sr_branch);
	}

	xt_ind_release(ot, ind, XT_UNLOCK_READ, &iref);
}

xtPublic void xt_load_indices(XTThreadPtr self, XTOpenTablePtr ot)
{
	register XTTableHPtr	tab = ot->ot_table;
	XTIndexPtr				*ind_ptr;
	XTIndexPtr				ind;
	xtIndexNodeID			current;

	xt_lock_mutex(self, &tab->tab_ind_flush_lock);
	pushr_(xt_unlock_mutex, &tab->tab_ind_flush_lock);

	ind_ptr = tab->tab_dic.dic_keys;
	for (u_int k=0; k<tab->tab_dic.dic_key_count; k++, ind_ptr++) {
		ind = *ind_ptr;
		XT_INDEX_WRITE_LOCK(ind, ot);
		if ((XT_NODE_ID(current) = XT_NODE_ID(ind->mi_root)))
			idx_load_node(self, ot, ind, current);
		XT_INDEX_UNLOCK(ind, ot);
	}

	freer_(); // xt_unlock_mutex(&tab->tab_ind_flush_lock)
}

/*
 * -----------------------------------------------------------------------
 * Count the number of deleted entries in a node:
 */

/*
 * {LAZY-DEL-INDEX-ITEMS}
 *
 * Use this function to count the number of deleted items 
 * in a node when it is loaded.
 *
 * The count helps us decide of the node should be "packed".
 */
xtPublic void xt_ind_count_deleted_items(XTTableHPtr tab, XTIndexPtr ind, XTIndBlockPtr block)
{
	XTIdxResultRec		result;
	int					del_count = 0;
	xtWord2				branch_size;

	branch_size = XT_GET_DISK_2(((XTIdxBranchDPtr) block->cb_data)->tb_size_2);

	/* This is possible when reading free pages. */
	if (XT_GET_INDEX_BLOCK_LEN(branch_size) < 2 || XT_GET_INDEX_BLOCK_LEN(branch_size) > XT_INDEX_PAGE_SIZE)
		return;

	idx_first_branch_item(tab, ind, (XTIdxBranchDPtr) block->cb_data, &result);
	while (result.sr_item.i_item_offset < result.sr_item.i_total_size) {
		if (result.sr_row_id == (xtRowID) -1)
			del_count++;
		idx_next_branch_item(tab, ind, (XTIdxBranchDPtr) block->cb_data, &result);
	}
	block->cp_del_count = del_count;
}

/*
 * -----------------------------------------------------------------------
 * Dirty list
 */

xtBool XTIndDirtyList::dl_add_block(XTIndBlockPtr block)
{
	XTIndDirtyBlocksPtr blocks;

	blocks = dl_block_lists;
	if (dl_list_usage == XT_DIRTY_BLOCK_LIST_SIZE || !blocks) {
		if (!(blocks = (XTIndDirtyBlocksPtr) xt_malloc_ns(sizeof(XTIndDirtyBlocksRec))))
			return FAILED;
		dl_list_usage = 0;
		blocks->db_next = dl_block_lists;
		dl_block_lists = blocks;
	}
	blocks->db_blocks[dl_list_usage] = block;
	dl_list_usage++;
	dl_total_blocks++;
	return OK;
}

static int idx_compare_blocks(const void *a, const void *b)
{
	XTIndBlockPtr b_a = *((XTIndBlockPtr *) a);
	XTIndBlockPtr b_b = *((XTIndBlockPtr *) b);

	if (b_a->cb_address == b_b->cb_address)
		return 0;
	if (b_a->cb_address < b_b->cb_address)
		return -1;
	return 1;
}

void XTIndDirtyList::dl_sort_blocks()
{
	XTIndDirtyBlocksPtr blocks;
	size_t				size;

	size = dl_list_usage;
	blocks = dl_block_lists;
	while (blocks) {
		qsort(blocks->db_blocks, size, sizeof(XTIndBlockPtr), idx_compare_blocks);
		blocks = blocks->db_next;
		size = XT_DIRTY_BLOCK_LIST_SIZE;
	}
}

void XTIndDirtyList::dl_free_all()
{
	XTIndDirtyBlocksPtr blocks, n_blocks;

	blocks = dl_block_lists;
	dl_block_lists = NULL;
	dl_total_blocks = 0;
	dl_list_usage = 0;
	while (blocks) {
		n_blocks = blocks->db_next;
		xt_free_ns(blocks);
		blocks = n_blocks;
	}
}

/*
 * -----------------------------------------------------------------------
 * Index consistent flush
 */

xtBool XTFlushIndexTask::tk_task(XTThreadPtr thread)
{
	XTOpenTablePtr		ot;

	fit_dirty_blocks = 0;
	fit_blocks_flushed = 0;

	/* See {TASK-TABLE-GONE} */
	if (!(xt_db_open_pool_table_ns(&ot, fit_table->tab_db, fit_table->tab_id)))
		return FAILED;

	if (!ot) {
		/* Can happen if the table has been dropped: */
		if (thread->t_exception.e_xt_err)
			xt_log_and_clear_exception(thread);
		xt_logf(XT_NT_WARNING, "Checkpoint skipping table (ID) %lu: table was not found\n", (u_long) fit_table->tab_id);
		xt_checkpoint_set_flush_state(fit_table->tab_db, fit_table->tab_id, XT_CPT_STATE_DONE_ALL);
		return OK;
	}

	if (ot->ot_table != fit_table) {
		/* Can happen if the table has been renamed: */
		if (thread->t_exception.e_xt_err)
			xt_log_and_clear_exception(thread);
		xt_logf(XT_NT_WARNING, "Checkpoint skipping table (ID) %lu: table has been renamed\n", (u_long) fit_table->tab_id);
		xt_checkpoint_set_flush_state(fit_table->tab_db, fit_table->tab_id, XT_CPT_STATE_DONE_ALL);
		goto table_gone;
	}

	if (!xt_flush_indices(ot, NULL, FALSE, this)) {
		xt_db_return_table_to_pool_ns(ot);
		return FAILED;
	}

	table_gone:
	xt_db_return_table_to_pool_ns(ot);
	return OK;
}

void XTFlushIndexTask::tk_reference()
{
	xt_heap_reference_ns(fit_table);
}

void XTFlushIndexTask::tk_release()
{
	xt_heap_release_ns(fit_table);
}

/*
 * Set notify_before_write to TRUE if the caller requires
 * notification before the index file is written.
 *
 * This is used if the index is flushed due to lock of index cache.
 */
xtPublic xtBool xt_async_flush_indices(XTTableHPtr tab, xtBool notify_complete, xtBool notify_before_write, XTThreadPtr thread)
{
	/* Run the task: */
	return xt_run_async_task(tab->tab_ind_flush_task, notify_complete, notify_before_write, thread, tab->tab_db);
}

#if defined(PRINT_IND_FLUSH_STATS) || defined(TRACE_FLUSH_TIMES)

static char *idx_format(char *buffer, double v)
{
	if (v != 0.0) {
		sprintf(buffer, "%9.2f", v);
		if (strcmp(buffer, "      nan") == 0)
			strcpy(buffer, "         ");
	}
	else
		strcpy(buffer, "         ");
	return buffer;
}

static char *idx_format_mb(char *buffer, double v)
{
	if (v != 0.0) {
		sprintf(buffer, "%7.3f", v / (double) 1024);
		if (strcmp(buffer, "    nan") == 0)
			strcpy(buffer, "       ");
	}
	else
		strcpy(buffer, "       ");
	return buffer;
}
#endif

#ifdef TRACE_FLUSH_TIMES

#define ILOG_FLUSH	1
#define INDEX_FLUSH	2

struct idxstats {
	u_int i_log_flush;
	u_int i_log_write;
	u_int idx_flush;
	u_int idx_write;
};

static void idx_print(char *msg, XTThreadPtr thread, struct idxstats *st, xtWord8 *now, int flush)
{
	xtWord8	then, t;
	double	ilogw, idxw;
	double	dilogw, didxw;
	char	buf1[30];
	char	buf2[30];
	char	buf3[30];
	char	buf4[30];

	then = xt_trace_clock();
	t = then - *now;
	ilogw = (double) (thread->st_statistics.st_ilog.ts_write - st->i_log_write) / (double) 1024;
	dilogw = ((double) ilogw * (double) 1000000) / (double) t;
	idxw = (double) (thread->st_statistics.st_ind.ts_write - st->idx_write) / (double) 1024;
	didxw = ((double) idxw * (double) 1000000) / (double) t;

	printf("%26s | TIME: %7d ", msg, (int) t);
	printf("ILOG: %s - %s INDX: %s - %s\n", 
		idx_format_mb(buf1, dilogw), idx_format(buf2, ilogw),
		idx_format_mb(buf3, didxw), idx_format(buf4, idxw));
	st->i_log_write = thread->st_statistics.st_ilog.ts_write;
	st->idx_write = thread->st_statistics.st_ind.ts_write;

	switch (flush) {
		case ILOG_FLUSH:
			ilogw = (double) (thread->st_statistics.st_ilog.ts_write - st->i_log_flush) / (double) 1024;
			dilogw = ((double) ilogw * (double) 1000000) / (double) t;
			printf("%26s | TIME: %7s ", " ", " ");
			printf("ILOG: %s - %s INDX: %s - %s\n", 
				idx_format_mb(buf1, dilogw), idx_format(buf2, ilogw),
				idx_format_mb(buf3, 0.0), idx_format(buf4, 0.0));
			st->i_log_flush = thread->st_statistics.st_ilog.ts_write;
			break;
		case INDEX_FLUSH:
			idxw = (double) (thread->st_statistics.st_ind.ts_write - st->idx_flush) / (double) 1024;
			didxw = ((double) idxw * (double) 1000000) / (double) t;
			printf("%26s | TIME: %7s ", " ", " ");
			printf("ILOG: %s - %s INDX: %s - %s\n", 
				idx_format_mb(buf1, 0.0), idx_format(buf2, 0.0),
				idx_format_mb(buf3, didxw), idx_format(buf4, idxw));
			st->idx_flush = thread->st_statistics.st_ind.ts_write;
			break;
	}

	*now = xt_trace_clock();
}

#define TRACE_FLUSH(a, b, c, d, e)		idx_print(a, b, c, d, e)

#else // TRACE_FLUSH_TIMES

#define TRACE_FLUSH(a, b, c, d, e)

#endif // TRACE_FLUSH_TIMES

/* Flush the indexes of a table.
 * If a ft is given, then this means this is an asynchronous flush.
 */
xtPublic xtBool xt_flush_indices(XTOpenTablePtr ot, off_t *bytes_flushed, xtBool have_table_lock, XTFlushIndexTask *fit)
{
	register XTTableHPtr	tab = ot->ot_table;
	XTIndexLogPtr			il;
	XTIndexPtr				*indp;
	XTIndexPtr				ind;
	u_int					i, j;
	XTIndBlockPtr			block, fblock;
	xtWord1					*data;
	xtIndexNodeID			ind_free;
	xtBool					block_on_free_list = FALSE;
	xtIndexNodeID			last_address, next_address;
	XTIndFreeListPtr		list_ptr;
	u_int					dirty_blocks;
	XTIndDirtyListItorRec	it;
	//u_int					dirty_count;
#ifdef TRACE_FLUSH_INDEX
	time_t					tnow = 0;
#endif

#ifdef TRACE_FLUSH_TIMES
	XTThreadPtr thread = ot->ot_thread;
	struct idxstats st;
	xtWord8 now;
	st.i_log_flush = thread->st_statistics.st_ilog.ts_write;
	st.i_log_write = thread->st_statistics.st_ilog.ts_write;
	st.idx_flush = thread->st_statistics.st_ind.ts_write;
	st.idx_write = thread->st_statistics.st_ind.ts_write;
	now = xt_trace_clock();
#endif

#ifdef DEBUG_CHECK_IND_CACHE
	xt_ind_check_cache(NULL);
#endif
	xt_lock_mutex_ns(&tab->tab_ind_flush_lock);
	TRACE_FLUSH("LOCKED flush index lock", thread, &st, &now, 0);

	if (!xt_begin_checkpoint(tab->tab_db, have_table_lock, ot->ot_thread))
		return FAILED;

	ASSERT_NS(!tab->tab_ind_flush_ilog);
	if (!tab->tab_db->db_indlogs.ilp_get_log(&tab->tab_ind_flush_ilog, ot->ot_thread))
		goto failed_3;
	il = tab->tab_ind_flush_ilog;

	if (!il->il_reset(ot))
		goto failed_2;
	if (!il->il_write_byte(ot, XT_DT_LOG_HEAD))
		goto failed_2;
	if (!il->il_write_word4(ot, tab->tab_id))
		goto failed_2;
	if (!il->il_write_word4(ot, 0))
		goto failed_2;
	TRACE_FLUSH("reset ilog", thread, &st, &now, 0);

	/* Lock all: */
	dirty_blocks = 0;
	indp = tab->tab_dic.dic_keys;
	for (i=0; i<tab->tab_dic.dic_key_count; i++, indp++) {
		ind = *indp;
		XT_INDEX_WRITE_LOCK(ind, ot);
		if (ind->mi_free_list && ind->mi_free_list->fl_free_count)
			block_on_free_list = TRUE;
		dirty_blocks += ind->mi_dirty_blocks;
	}
	TRACE_FLUSH("LOCKED all indexes", thread, &st, &now, 0);

	if (!dirty_blocks && !block_on_free_list) {
		/* Nothing to flush... */
		indp = tab->tab_dic.dic_keys;
		for (i=0; i<tab->tab_dic.dic_key_count; i++, indp++) {
			ind = *indp;
			XT_INDEX_UNLOCK(ind, ot);
		}
		goto flush_done;
	}

#ifdef TRACE_FLUSH_INDEX
	tnow = time(NULL);
	printf("FLUSH INDEX pages=%lu %s\n", (u_long) dirty_blocks, tab->tab_name->ps_path);
#endif

	if (fit)
		fit->fit_dirty_blocks = dirty_blocks;

	// 128 dirty blocks == 2MB
	if (bytes_flushed)
		*bytes_flushed += (dirty_blocks * XT_INDEX_PAGE_SIZE);

	/* Collect the index roots: */
	data = tab->tab_index_head->tp_data;

	/* Collect a complete list of all dirty blocks: */
	indp = tab->tab_dic.dic_keys;
	for (i=0; i<tab->tab_dic.dic_key_count; i++, indp++) {
		ind = *indp;
		xt_spinlock_lock(&ind->mi_dirty_lock);
		if ((block = ind->mi_dirty_list)) {
			while (block) {
				ASSERT_NS(block->cb_state == IDX_CAC_BLOCK_DIRTY);
#ifdef IND_OPT_DATA_WRITTEN
				ASSERT_NS(block->cb_max_pos <= XT_INDEX_PAGE_SIZE-2);
#endif
				tab->tab_ind_dirty_list.dl_add_block(block);
				fblock = block->cb_dirty_next;
				block->cb_dirty_next = NULL;
				block->cb_dirty_prev = NULL;
				block->cb_state = IDX_CAC_BLOCK_FLUSHING;
				block = fblock;
			}
		}
		//dirty_count = ind->mi_dirty_blocks;
		ind->mi_dirty_blocks = 0;
		ind->mi_dirty_list = NULL;
		xt_spinlock_unlock(&ind->mi_dirty_lock);
		//ot->ot_thread->st_statistics.st_ind_cache_dirty -= dirty_count;
		XT_SET_NODE_REF(tab, data, ind->mi_root);
		data += XT_NODE_REF_SIZE;
	}

	TRACE_FLUSH("Collected all blocks", thread, &st, &now, 0);

	xt_lock_mutex_ns(&tab->tab_ind_lock);
	TRACE_FLUSH("LOCKED table index lock", thread, &st, &now, 0);

	/* Write the free list: */
	if (block_on_free_list) {
		union {
			xtWord1				buffer[XT_BLOCK_SIZE_FOR_DIRECT_IO];
			XTIndFreeBlockRec	free_block;
		} x;
		memset(x.buffer, 0, sizeof(XTIndFreeBlockRec));

		/* The old start of the free list: */
		XT_NODE_ID(ind_free) = 0;
		/* This is a list of lists: */
		while ((list_ptr = tab->tab_ind_free_list)) {
			/* If this free list still has unused blocks,
			 * pick the first. That is the front of
			 * the list of free blocks.
			 */
			if (list_ptr->fl_start < list_ptr->fl_free_count) {
				ind_free = list_ptr->fl_page_id[list_ptr->fl_start];
				break;
			}
			/* This list is empty, free it: */
			tab->tab_ind_free_list = list_ptr->fl_next_list;
			xt_free_ns(list_ptr);
		}
		/* If nothing is on any list, then
		 * take the value stored in the index header.
		 * It is the from of the list on disk.
		 */
		if (!XT_NODE_ID(ind_free))
			ind_free = tab->tab_ind_free;

		if (!il->il_write_byte(ot, XT_DT_FREE_LIST))
			goto failed;
		indp = tab->tab_dic.dic_keys;
		XT_NODE_ID(last_address) = 0;
		for (i=0; i<tab->tab_dic.dic_key_count; i++, indp++) {
			ind = *indp;
			if (ind->mi_free_list && ind->mi_free_list->fl_free_count) {
				for (j=0; j<ind->mi_free_list->fl_free_count; j++) {
					next_address = ind->mi_free_list->fl_page_id[j];
					/* Write out the IDs of the free blocks. */
					if (!il->il_write_word4(ot, XT_NODE_ID(ind->mi_free_list->fl_page_id[j])))
						goto failed;
					if (XT_NODE_ID(last_address)) {
						/* Update the page in cache, if it is in the cache! */
						XT_SET_DISK_8(x.free_block.if_next_block_8, XT_NODE_ID(next_address));
						if (!xt_ind_write_cache(ot, last_address, 8, x.buffer))
							goto failed;
					}
					last_address = next_address;
				}
			}
		}
		if (!il->il_write_word4(ot, XT_NODE_ID(ind_free)))
			goto failed;
		if (XT_NODE_ID(last_address)) {
			XT_SET_DISK_8(x.free_block.if_next_block_8, XT_NODE_ID(tab->tab_ind_free));
			if (!xt_ind_write_cache(ot, last_address, 8, x.buffer))
				goto failed;
		}
		if (!il->il_write_word4(ot, 0xFFFFFFFF))
			goto failed;
	}

	/*
	 * Add the free list caches to the global free list cache.
	 * Added backwards to match the write order.
	 */
	indp = tab->tab_dic.dic_keys + tab->tab_dic.dic_key_count-1;
	for (i=0; i<tab->tab_dic.dic_key_count; i++, indp--) {
		ind = *indp;
		if (ind->mi_free_list) {
			ind->mi_free_list->fl_next_list = tab->tab_ind_free_list;
			tab->tab_ind_free_list = ind->mi_free_list;
		}
		ind->mi_free_list = NULL;
	}

	/*
	 * The new start of the free list is the first
	 * item on the table free list:
	 */
	XT_NODE_ID(ind_free) = 0;
	while ((list_ptr = tab->tab_ind_free_list)) {
		if (list_ptr->fl_start < list_ptr->fl_free_count) {
			ind_free = list_ptr->fl_page_id[list_ptr->fl_start];
			break;
		}
		tab->tab_ind_free_list = list_ptr->fl_next_list;
		xt_free_ns(list_ptr);
	}
	if (!XT_NODE_ID(ind_free))
		ind_free = tab->tab_ind_free;
	TRACE_FLUSH("did free block stuff", thread, &st, &now, 0);
	xt_unlock_mutex_ns(&tab->tab_ind_lock);

	XT_SET_DISK_6(tab->tab_index_head->tp_ind_eof_6, XT_NODE_ID(tab->tab_ind_eof));
	XT_SET_DISK_6(tab->tab_index_head->tp_ind_free_6, XT_NODE_ID(ind_free));

	TRACE_FLUSH("UN-LOCKED table index lock", thread, &st, &now, 0);
	if (!il->il_write_header(ot, XT_INDEX_HEAD_SIZE, (xtWord1 *) tab->tab_index_head))
		goto failed;

	TRACE_FLUSH("wrote ilog header", thread, &st, &now, 0);
	indp = tab->tab_dic.dic_keys;
	for (i=0; i<tab->tab_dic.dic_key_count; i++, indp++) {
		ind = *indp;
		XT_INDEX_UNLOCK(ind, ot);
	}

	TRACE_FLUSH("UN-LOCKED all indexes", thread, &st, &now, 0);

	/* Write all blocks to the index log: */
	if (tab->tab_ind_dirty_list.dl_total_blocks) {
		tab->tab_ind_dirty_list.dl_sort_blocks();
		it.dli_reset();
		while ((block = tab->tab_ind_dirty_list.dl_next_block(&it))) {
			XT_IPAGE_WRITE_LOCK(&block->cb_lock, ot->ot_thread->t_id);
			if (block->cb_state == IDX_CAC_BLOCK_FLUSHING) {
				if (!il->il_write_block(ot, block)) {
					XT_IPAGE_UNLOCK(&block->cb_lock, TRUE);
					goto failed_2;
				}
			}
			XT_IPAGE_UNLOCK(&block->cb_lock, TRUE);
		}
	}

	/* {PAGE-NO-IN-INDEX-FILE}
	 * At this point all blocks have been written to the index file.
	 * It is not safe to release them from the cache.
	 * It was not safe to do this before this point because
	 * read would read the wrong data.
	 *
	 * The exception to this is freed blocks.
	 * These are cached separately in the free block list.
	 */
	TRACE_FLUSH("Wrote all blocks to the ilog", thread, &st, &now, 0);

	if (il->il_data_written()) {
		/* Flush the log before we flush the index.
		 *
		 * The reason is, we must make sure that changes that
		 * will be in the index are already in the transaction
		 * log.
		 *
		 * Only then are we able to undo those changes on
		 * recovery.
		 *
		 * Simple example:
		 * CREATE TABLE t1 (s1 INT PRIMARY KEY);
		 * INSERT INTO t1 VALUES (1);
		 *
		 * BEGIN;
		 * INSERT INTO t1 VALUES (2);
		 *
		 * --- INDEX IS FLUSHED HERE ---
		 *
		 * --- SERVER CRASH HERE ---
		 *
		 *
		 * The INSERT VALUES (2) has been written
		 * to the log, but not flushed.
		 * But the index has been updated.
		 * If the index is flushed it will contain
		 * the entry for record with s1=2.
		 * 
		 * This entry must be removed on recovery.
		 *
		 * To prevent this situation I flush the log
		 * here.
		 */
		if (!XT_IS_TEMP_TABLE(tab->tab_dic.dic_tab_flags)) {
			/* Note, thread->st_database may not be set here:
			 * #0	0x00defba3 in xt_spinlock_set at lock_xt.h:249
			 * #1	0x00defbfd in xt_spinlock_lock at lock_xt.h:299
			 * #2	0x00e65a98 in XTDatabaseLog::xlog_flush_pending at xactlog_xt.cc:737
			 * #3	0x00e6a27f in XTDatabaseLog::xlog_flush at xactlog_xt.cc:727
			 * #4	0x00e6a308 in xt_xlog_flush_log at xactlog_xt.cc:1476
			 * #5	0x00e22fee in xt_flush_indices at index_xt.cc:4599
			 * #6	0x00e49fe0 in xt_close_table at table_xt.cc:2678
			 * #7	0x00df0f10 in xt_db_pool_exit at database_xt.cc:758
			 * #8	0x00df3ca2 in db_finalize at database_xt.cc:342
			 * #9	0x00e17037 in xt_heap_release at heap_xt.cc:110
			 * #10	0x00df07c0 in db_hash_free at database_xt.cc:245
			 * #11	0x00e0b145 in xt_free_hashtable at hashtab_xt.cc:84
			 * #12	0x00df093e in xt_exit_databases at database_xt.cc:303
			 * #13	0x00e0d3cf in pbxt_call_exit at ha_pbxt.cc:946
			 * #14	0x00e0d498 in ha_exit at ha_pbxt.cc:975
			 * #15	0x00e0dce6 in pbxt_end at ha_pbxt.cc:1274
			 * #16	0x00e0dd03 in pbxt_panic at ha_pbxt.cc:1287
			 * #17	0x002321a1 in ha_finalize_handlerton at handler.cc:392
			 * #18	0x002f0567 in plugin_deinitialize at sql_plugin.cc:815
			 * #19	0x002f370e in reap_plugins at sql_plugin.cc:903
			 * #20	0x002f3eac in plugin_shutdown at sql_plugin.cc:1512
			 * #21	0x000f3ba6 in clean_up at mysqld.cc:1238
			 * #22	0x000f4046 in unireg_end at mysqld.cc:1166
			 * #23	0x000fc2c5 in kill_server at mysqld.cc:1108
			 * #24	0x000fd0d5 in kill_server_thread at mysqld.cc:1129
			 */
			if (!tab->tab_db->db_xlog.xlog_flush(ot->ot_thread))
				goto failed_2;
			TRACE_FLUSH("FLUSHED xlog", thread, &st, &now, 0);
		}

		if (!il->il_flush(ot))
			goto failed_2;
		TRACE_FLUSH("FLUSHED ilog", thread, &st, &now, ILOG_FLUSH);

		if (!il->il_apply_log_write(ot))
			goto failed_2;

		TRACE_FLUSH("wrote all blocks to index", thread, &st, &now, 0);
		/*
		 * {NOT-IN-IND-FILE}
		 * 1.0.01 - I have split apply log into flush and write
		 * parts.
		 *
		 * I have to write before waiting threads can continue
		 * because otherwise incorrect data will be read
		 * when the cache block is freed before it is written
		 * here.
		 *
		 * Risk: (see below).
		 */
		it.dli_reset();
		while ((block = tab->tab_ind_dirty_list.dl_next_block(&it))) {
			XT_IPAGE_WRITE_LOCK(&block->cb_lock, ot->ot_thread->t_id);
			if (block->cb_state == IDX_CAC_BLOCK_LOGGED) {
				block->cb_state = IDX_CAC_BLOCK_CLEAN;
				ot->ot_thread->st_statistics.st_ind_cache_dirty--;
			}
			XT_IPAGE_UNLOCK(&block->cb_lock, TRUE);
		}

		/* This will early notification for threads waiting for this operation
		 * to get to this point.
		 */
		if (fit) {
			fit->fit_blocks_flushed = fit->fit_dirty_blocks;
			fit->fit_dirty_blocks = 0;
			xt_async_task_notify(fit);
		}

		/*
		 * 1.0.01 - Note: I have moved the flush to here.
		 * It allows the calling thread to continue during the
		 * flush, but:
		 *
		 * The  problem is, if the ilog flush fails then we have
		 * lost the information to re-create a consistent flush again!
		 */
		if (!il->il_apply_log_flush(ot))
			goto failed_2;
		TRACE_FLUSH("FLUSHED index file", thread, &st, &now, INDEX_FLUSH);
	}

	flush_done:
	tab->tab_ind_dirty_list.dl_free_all();
	tab->tab_ind_flush_ilog = NULL;
	il->il_release();

	/* Mark this table as index flushed: */
	xt_checkpoint_set_flush_state(tab->tab_db, tab->tab_id, XT_CPT_STATE_DONE_INDEX);
	TRACE_FLUSH("set index state", thread, &st, &now, 0);

#ifdef TRACE_FLUSH_INDEX
	if (tnow) {
		printf("flush index (%d) %s DONE\n", (int) (time(NULL) - tnow), tab->tab_name->ps_path);
		fflush(stdout);
	}
#endif

	xt_unlock_mutex_ns(&tab->tab_ind_flush_lock);
	TRACE_FLUSH("UN-LOCKED flush index lock", thread, &st, &now, 0);

	if (!xt_end_checkpoint(tab->tab_db, ot->ot_thread, NULL))
		return FAILED;

#ifdef DEBUG_CHECK_IND_CACHE
	xt_ind_check_cache((XTIndex *) 1);
#endif
	return OK;

	failed:
	indp = tab->tab_dic.dic_keys;
	for (i=0; i<tab->tab_dic.dic_key_count; i++, indp++) {
		ind = *indp;
		XT_INDEX_UNLOCK(ind, ot);
	}

	failed_2:
	tab->tab_ind_dirty_list.dl_free_all();
	tab->tab_ind_flush_ilog = NULL;
	il->il_release();

	failed_3:
	xt_checkpoint_set_flush_state(tab->tab_db, tab->tab_id, XT_CPT_STATE_STOP_INDEX);

#ifdef TRACE_FLUSH_INDEX
	if (tnow) {
		printf("flush index (%d) %s FAILED\n", (int) (time(NULL) - tnow), tab->tab_name->ps_path);
		fflush(stdout);
	}
#endif

	xt_unlock_mutex_ns(&tab->tab_ind_flush_lock);
#ifdef DEBUG_CHECK_IND_CACHE
	xt_ind_check_cache(NULL);
#endif
	return FAILED;
}

void XTIndexLogPool::ilp_init(struct XTThread *self, struct XTDatabase *db, size_t log_buffer_size)
{
	char			path[PATH_MAX];
	XTOpenDirPtr	od;
	xtLogID			log_id;
	char			*file;
	XTIndexLogPtr	il = NULL;
	XTOpenTablePtr	ot = NULL;

	ilp_db = db;
	ilp_log_buffer_size = log_buffer_size;
	xt_init_mutex_with_autoname(self, &ilp_lock);

	xt_strcpy(PATH_MAX, path, db->db_main_path);
	xt_add_system_dir(PATH_MAX, path);
	if (xt_fs_exists(path)) {
		pushsr_(od, xt_dir_close, xt_dir_open(self, path, NULL));
		while (xt_dir_next(self, od)) {
			file = xt_dir_name(self, od);
			if (xt_starts_with(file, "ilog")) {
				if ((log_id = (xtLogID) xt_file_name_to_id(file))) {
					if (!ilp_open_log(&il, log_id, FALSE, self))
						goto failed;
					if (il->il_tab_id && il->il_log_eof) {
						if (!il->il_open_table(&ot))
							goto failed;
						if (ot) {
							if (!il->il_apply_log_write(ot))
								goto failed;
							if (!il->il_apply_log_flush(ot))
								goto failed;
							ot->ot_thread = self;
							il->il_close_table(ot);
						}
					}
					il->il_close(TRUE);
				}
			}
		}
		freer_(); // xt_dir_close(od)
	}
	return;

	failed:
	/* TODO: Mark index as corrupted: */
	if (ot && il)
		il->il_close_table(ot);
	if (il)
		il->il_close(FALSE);
	xt_throw(self);
}

void XTIndexLogPool::ilp_close(struct XTThread *XT_UNUSED(self), xtBool lock)
{
	XTIndexLogPtr	il;

	if (lock)
		xt_lock_mutex_ns(&ilp_lock);
	while ((il = ilp_log_pool)) {
		ilp_log_pool = il->il_next_in_pool;
		il_pool_count--;
		il->il_close(TRUE);
	}
	if (lock)
		xt_unlock_mutex_ns(&ilp_lock);
}

void XTIndexLogPool::ilp_exit(struct XTThread *self)
{
	ilp_close(self, FALSE);
	ASSERT_NS(il_pool_count == 0);
	xt_free_mutex(&ilp_lock);
}

void XTIndexLogPool::ilp_name(size_t size, char *path, xtLogID log_id)
{
	char name[50];

	sprintf(name, "ilog-%lu.xt", (u_long) log_id);
	xt_strcpy(size, path, ilp_db->db_main_path);
	xt_add_system_dir(size, path);
	xt_add_dir_char(size, path);
	xt_strcat(size, path, name);
}

xtBool XTIndexLogPool::ilp_open_log(XTIndexLogPtr *ret_il, xtLogID log_id, xtBool excl, XTThreadPtr thread)
{
	char				log_path[PATH_MAX];
	XTIndexLogPtr		il;
	XTIndLogHeadDRec	log_head;
	size_t				read_size;

	ilp_name(PATH_MAX, log_path, log_id);
	if (!(il = (XTIndexLogPtr) xt_calloc_ns(sizeof(XTIndexLogRec))))
		return FAILED;
	xt_spinlock_init_with_autoname(NULL, &il->il_write_lock);
	il->il_log_id = log_id;
	il->il_pool = this;

	/* Writes will be rounded up to the nearest direct write block size (see {WRITE-IN-BLOCKS}),
	 * so make sure we have space in the buffer for that:
	 */
#ifdef DEBUG
	if (IND_WRITE_BLOCK_SIZE < XT_BLOCK_SIZE_FOR_DIRECT_IO)
		ASSERT_NS(FALSE);
#ifdef IND_WRITE_IN_BLOCK_SIZES
	if (XT_INDEX_PAGE_SIZE < IND_WRITE_BLOCK_SIZE)
		ASSERT_NS(FALSE);
#endif
#endif
#ifdef IND_FILL_BLOCK_TO_NEXT
	if (!(il->il_buffer = (xtWord1 *) xt_malloc_ns(ilp_log_buffer_size + XT_INDEX_PAGE_SIZE)))
		goto failed;
#else
	if (!(il->il_buffer = (xtWord1 *) xt_malloc_ns(ilp_log_buffer_size + IND_WRITE_BLOCK_SIZE)))
		goto failed;
#endif
	il->il_buffer_size = ilp_log_buffer_size;

	if (!(il->il_of = xt_open_file_ns(log_path, XT_FT_STANDARD, (excl ? XT_FS_EXCLUSIVE : 0) | XT_FS_CREATE | XT_FS_MAKE_PATH, 0)))
		goto failed;

	if (!xt_pread_file(il->il_of, 0, sizeof(XTIndLogHeadDRec), 0, &log_head, &read_size, &thread->st_statistics.st_ilog, thread))
		goto failed;

	if (read_size == sizeof(XTIndLogHeadDRec)) {
		il->il_tab_id = XT_GET_DISK_4(log_head.ilh_tab_id_4);
		il->il_log_eof = XT_GET_DISK_4(log_head.ilh_log_eof_4);
	}
	else {
		il->il_tab_id = 0;
		il->il_log_eof = 0;
	}

	*ret_il = il;
	return OK;

	failed:
	il->il_close(FALSE);
	return FAILED;
}

xtBool XTIndexLogPool::ilp_get_log(XTIndexLogPtr *ret_il, XTThreadPtr thread)
{
	XTIndexLogPtr	il;
	xtLogID			log_id = 0;

	xt_lock_mutex_ns(&ilp_lock);
	if ((il = ilp_log_pool)) {
		ilp_log_pool = il->il_next_in_pool;
		il_pool_count--;
	}
	else {
		ilp_next_log_id++;
		log_id = ilp_next_log_id;
	}
	xt_unlock_mutex_ns(&ilp_lock);
	if (!il) {
		if (!ilp_open_log(&il, log_id, TRUE, thread))
			return FAILED;
	}
	*ret_il= il;
	return OK;
}

void XTIndexLogPool::ilp_release_log(XTIndexLogPtr il)
{
	xt_lock_mutex_ns(&ilp_lock);
	if (il_pool_count == 5)
		il->il_close(TRUE);
	else {
		il_pool_count++;
		il->il_next_in_pool = ilp_log_pool;
		ilp_log_pool = il;
	}
	xt_unlock_mutex_ns(&ilp_lock);
}

xtBool XTIndexLog::il_reset(struct XTOpenTable *ot)
{
	XTIndLogHeadDRec	log_head;
	xtTableID			tab_id = ot->ot_table->tab_id;

	il_tab_id = tab_id;
	il_log_eof = 0;
	il_buffer_len = 0;
	il_buffer_offset = 0;

	/* We must write the header and flush here or the "previous" status (from the
	 * last flush run) could remain. Failure to write the file completely leave the
	 * old header in place, and other parts of the file changed.
	 * This would lead to index corruption.  
	 */
	log_head.ilh_data_type = XT_DT_LOG_HEAD;
	XT_SET_DISK_4(log_head.ilh_tab_id_4, tab_id);
	XT_SET_DISK_4(log_head.ilh_log_eof_4, 0);

	if (!xt_pwrite_file(il_of, 0, sizeof(XTIndLogHeadDRec), (xtWord1 *) &log_head, &ot->ot_thread->st_statistics.st_ilog, ot->ot_thread))
		return FAILED;

	if (!xt_flush_file(il_of, &ot->ot_thread->st_statistics.st_ilog, ot->ot_thread))
		return FAILED;

	return OK;
}

xtBool XTIndexLog::il_data_written()
{
	return il_buffer_offset != 0 || il_buffer_len != 0;
}

void XTIndexLog::il_close(xtBool delete_it)
{
	xtLogID	log_id = il_log_id;

	if (il_of) {
		xt_close_file_ns(il_of);
		il_of = NULL;
	}
	
	if (delete_it && log_id) {
		char	log_path[PATH_MAX];

		il_pool->ilp_name(PATH_MAX, log_path, log_id);
		xt_fs_delete(NULL, log_path);
	}

	if (il_buffer) {
		xt_free_ns(il_buffer);
		il_buffer = NULL;
	}

	xt_spinlock_free(NULL, &il_write_lock);
	xt_free_ns(this);
}

void XTIndexLog::il_release()
{
	il_pool->ilp_db->db_indlogs.ilp_release_log(this);
}

xtBool XTIndexLog::il_require_space(size_t bytes, XTThreadPtr thread)
{
	if (il_buffer_len + bytes > il_buffer_size) {
		if (!xt_pwrite_file(il_of, il_buffer_offset, il_buffer_len, il_buffer, &thread->st_statistics.st_ilog, thread))
			return FAILED;
		il_buffer_offset += il_buffer_len;
		il_buffer_len = 0;
	}

	return OK;
}

xtBool XTIndexLog::il_write_byte(struct XTOpenTable *ot, xtWord1 byte)
{
	if (!il_require_space(1, ot->ot_thread))
		return FAILED;
	*(il_buffer + il_buffer_len) = byte;
	il_buffer_len++;
	return OK;
}

xtBool XTIndexLog::il_write_word4(struct XTOpenTable *ot, xtWord4 value)
{
	xtWord1 *buffer;

	if (!il_require_space(4, ot->ot_thread))
		return FAILED;
	buffer = il_buffer + il_buffer_len;
	XT_SET_DISK_4(buffer, value);
	il_buffer_len += 4;
	return OK;
}

/*
 * This function assumes that the block is xlocked!
 */
xtBool XTIndexLog::il_write_block(struct XTOpenTable *ot, XTIndBlockPtr block)
{
	xtIndexNodeID		node_id;
	XTIdxBranchDPtr		node;
	u_int				block_len;

	node_id = block->cb_address;
	node = (XTIdxBranchDPtr) block->cb_data;
	block_len = XT_GET_INDEX_BLOCK_LEN(XT_GET_DISK_2(node->tb_size_2));
	
	//il_min_byte_count += (block->cb_max_pos - block->cb_min_pos) + (block->cb_header ? XT_INDEX_PAGE_HEAD_SIZE : 0);
#ifdef IND_WRITE_MIN_DATA
	u_int				max_pos;
	u_int				min_pos;
	xtBool				eo_block = FALSE;

#ifdef IND_WRITE_IN_BLOCK_SIZES
	/* Round up to block boundary: */
	max_pos = ((block->cb_max_pos + XT_INDEX_PAGE_HEAD_SIZE + IND_WRITE_BLOCK_SIZE - 1) / IND_WRITE_BLOCK_SIZE) * IND_WRITE_BLOCK_SIZE;
	if (max_pos > block_len)
		max_pos = block_len;
	max_pos -= XT_INDEX_PAGE_HEAD_SIZE;
	/* Round down to block boundary: */
	min_pos = ((block->cb_min_pos + XT_INDEX_PAGE_HEAD_SIZE) / IND_WRITE_BLOCK_SIZE) * IND_WRITE_BLOCK_SIZE;
	if (min_pos > 0)
		min_pos -= XT_INDEX_PAGE_HEAD_SIZE;
#else
	max_pos = block->cb_max_pos;
	min_pos = block->cb_min_pos;
#endif

	if (block_len == max_pos + XT_INDEX_PAGE_HEAD_SIZE)
		eo_block = TRUE;
		
	ASSERT_NS(max_pos <= XT_INDEX_PAGE_SIZE-XT_INDEX_PAGE_HEAD_SIZE);
	ASSERT_NS(min_pos <= block_len-XT_INDEX_PAGE_HEAD_SIZE);
	ASSERT_NS(max_pos <= block_len-XT_INDEX_PAGE_HEAD_SIZE);
	ASSERT_NS(min_pos <= max_pos);

	xt_spinlock_lock(&il_write_lock);
	if (block->cb_min_pos == block->cb_max_pos) {
		/* This means just the header was changed. */
#ifdef IND_WRITE_IN_BLOCK_SIZES
		XTIndShortPageDataDPtr		sh_data;

		if (!il_require_space(offsetof(XTIndShortPageDataDRec, ild_data) + IND_WRITE_BLOCK_SIZE, ot->ot_thread))
			goto failed;

		sh_data = (XTIndShortPageDataDPtr) (il_buffer + il_buffer_len);
		sh_data->ild_data_type = XT_DT_SHORT_IND_PAGE;
		XT_SET_DISK_4(sh_data->ild_page_id_4, XT_NODE_ID(node_id));
		XT_SET_DISK_2(sh_data->ild_size_2, IND_WRITE_BLOCK_SIZE);
		memcpy(sh_data->ild_data, block->cb_data, IND_WRITE_BLOCK_SIZE);
		il_buffer_len += offsetof(XTIndShortPageDataDRec, ild_data) + IND_WRITE_BLOCK_SIZE;
#else
		XTIndSetPageHeadDataDPtr	sph_data;

		if (!il_require_space(sizeof(XTIndSetPageHeadDataDRec), ot->ot_thread))
			goto failed;

		ASSERT_NS(sizeof(XTIndSetPageHeadDataDRec) <= il_buffer_size);

		sph_data = (XTIndSetPageHeadDataDPtr) (il_buffer + il_buffer_len);
		sph_data->ild_data_type = XT_DT_SET_PAGE_HEAD;
		XT_SET_DISK_4(sph_data->ild_page_id_4, XT_NODE_ID(node_id));
		XT_COPY_DISK_2(sph_data->ild_page_head_2, block->cb_data);
		il_buffer_len += sizeof(XTIndSetPageHeadDataDRec);
#endif
	}
#ifdef IND_WRITE_IN_BLOCK_SIZES
	else if (min_pos == 0 || (block->cb_header && min_pos == IND_WRITE_BLOCK_SIZE - XT_INDEX_PAGE_HEAD_SIZE))
#else
	else if (min_pos < 16 - XT_INDEX_PAGE_HEAD_SIZE)
#endif
	{
		/* Fuse, and write the whole block: */
		if (eo_block) {
			XTIndPageDataDPtr	p_data;

			if (!il_require_space(offsetof(XTIndPageDataDRec, ild_data) + block_len, ot->ot_thread))
				goto failed;

			ASSERT_NS(offsetof(XTIndPageDataDRec, ild_data) + block_len <= il_buffer_size);

			p_data = (XTIndPageDataDPtr) (il_buffer + il_buffer_len);
			p_data->ild_data_type = XT_DT_INDEX_PAGE;
			XT_SET_DISK_4(p_data->ild_page_id_4, XT_NODE_ID(node_id));
			memcpy(p_data->ild_data, block->cb_data, block_len);
			il_buffer_len += offsetof(XTIndPageDataDRec, ild_data) + block_len;
		}
		else {
			XTIndShortPageDataDPtr	sp_data;

			block_len = max_pos + XT_INDEX_PAGE_HEAD_SIZE;

			if (!il_require_space(offsetof(XTIndShortPageDataDRec, ild_data) + block_len, ot->ot_thread))
				goto failed;

			ASSERT_NS(offsetof(XTIndShortPageDataDRec, ild_data) + block_len <= il_buffer_size);

			sp_data = (XTIndShortPageDataDPtr) (il_buffer + il_buffer_len);
			sp_data->ild_data_type = XT_DT_SHORT_IND_PAGE;
			XT_SET_DISK_4(sp_data->ild_page_id_4, XT_NODE_ID(node_id));
			XT_SET_DISK_2(sp_data->ild_size_2, block_len);
			memcpy(sp_data->ild_data, block->cb_data, block_len);
			il_buffer_len += offsetof(XTIndShortPageDataDRec, ild_data) + block_len;
		}
	}
	else {
		block_len = max_pos - min_pos;

		if (block->cb_header) {
#ifdef IND_WRITE_IN_BLOCK_SIZES
			XTIndDoubleModPageDataDPtr	dd_data;

			ASSERT_NS(min_pos + XT_INDEX_PAGE_HEAD_SIZE >= 2*IND_WRITE_BLOCK_SIZE);
			ASSERT_NS((min_pos + XT_INDEX_PAGE_HEAD_SIZE) % IND_WRITE_BLOCK_SIZE == 0);
			if (!il_require_space(offsetof(XTIndDoubleModPageDataDRec, dld_data) + IND_WRITE_BLOCK_SIZE + block_len, ot->ot_thread))
				goto failed;

			dd_data = (XTIndDoubleModPageDataDPtr) (il_buffer + il_buffer_len);
			dd_data->dld_data_type = eo_block ? XT_DT_2_MOD_IND_PAGE_EOB : XT_DT_2_MOD_IND_PAGE;
			XT_SET_DISK_4(dd_data->dld_page_id_4, XT_NODE_ID(node_id));
			XT_SET_DISK_2(dd_data->dld_size1_2, IND_WRITE_BLOCK_SIZE);
			XT_SET_DISK_2(dd_data->dld_offset2_2, min_pos);
			XT_SET_DISK_2(dd_data->dld_size2_2, block_len);
			memcpy(dd_data->dld_data, block->cb_data, IND_WRITE_BLOCK_SIZE);
			memcpy(dd_data->dld_data + IND_WRITE_BLOCK_SIZE, block->cb_data + XT_INDEX_PAGE_HEAD_SIZE + min_pos, block_len);
			il_buffer_len += offsetof(XTIndDoubleModPageDataDRec, dld_data) + IND_WRITE_BLOCK_SIZE + block_len;
#else
			XTIndModPageHeadDataDPtr	mph_data;

			if (!il_require_space(offsetof(XTIndModPageHeadDataDRec, ild_data) + block_len, ot->ot_thread))
				goto failed;

			mph_data = (XTIndModPageHeadDataDPtr) (il_buffer + il_buffer_len);
			mph_data->ild_data_type = eo_block ? XT_DT_MOD_IND_PAGE_HEAD_EOB : XT_DT_MOD_IND_PAGE_HEAD;
			XT_SET_DISK_4(mph_data->ild_page_id_4, XT_NODE_ID(node_id));
			XT_SET_DISK_2(mph_data->ild_size_2, block_len);
			XT_SET_DISK_2(mph_data->ild_offset_2, min_pos);
			XT_COPY_DISK_2(mph_data->ild_page_head_2, block->cb_data);
			memcpy(mph_data->ild_data, block->cb_data + XT_INDEX_PAGE_HEAD_SIZE + min_pos, block_len);
			il_buffer_len += offsetof(XTIndModPageHeadDataDRec, ild_data) + block_len;
#endif
		}
		else {
			XTIndModPageDataDPtr	mp_data;

			if (!il_require_space(offsetof(XTIndModPageDataDRec, ild_data) + block_len, ot->ot_thread))
				goto failed;

			mp_data = (XTIndModPageDataDPtr) (il_buffer + il_buffer_len);
			mp_data->ild_data_type = eo_block ? XT_DT_MOD_IND_PAGE_EOB : XT_DT_MOD_IND_PAGE;
			XT_SET_DISK_4(mp_data->ild_page_id_4, XT_NODE_ID(node_id));
			XT_SET_DISK_2(mp_data->ild_size_2, block_len);
			XT_SET_DISK_2(mp_data->ild_offset_2, min_pos);
			memcpy(mp_data->ild_data, block->cb_data + XT_INDEX_PAGE_HEAD_SIZE + min_pos, block_len);
			il_buffer_len += offsetof(XTIndModPageDataDRec, ild_data) + block_len;
		}
	}

	block->cb_header = FALSE;
	block->cb_min_pos = 0xFFFF;
	block->cb_max_pos = 0;

#else // IND_OPT_DATA_WRITTEN
	XTIndPageDataDPtr	page_data;

	if (!il_require_space(offsetof(XTIndPageDataDRec, ild_data) + block_len, ot->ot_thread))
		goto failed;

	ASSERT_NS(offsetof(XTIndPageDataDRec, ild_data) + XT_INDEX_PAGE_SIZE <= il_buffer_size);

	page_data = (XTIndPageDataDPtr) (il_buffer + il_buffer_len);
	TRACK_BLOCK_TO_FLUSH(node_id);
	page_data->ild_data_type = XT_DT_INDEX_PAGE;
	XT_SET_DISK_4(page_data->ild_page_id_4, XT_NODE_ID(node_id));
	memcpy(page_data->ild_data, block->cb_data, block_len);

	il_buffer_len += offsetof(XTIndPageDataDRec, ild_data) + block_len;

#endif // IND_OPT_DATA_WRITTEN
	xt_spinlock_unlock(&il_write_lock);

	ASSERT_NS(block->cb_state == IDX_CAC_BLOCK_FLUSHING);
	block->cb_state = IDX_CAC_BLOCK_LOGGED;

	TRACK_BLOCK_TO_FLUSH(node_id);
	return OK;

	failed:
	xt_spinlock_unlock(&il_write_lock);
	return FAILED;
}

xtBool XTIndexLog::il_write_header(struct XTOpenTable *ot, size_t head_size, xtWord1 *head_buf)
{
	XTIndHeadDataDPtr	head_data;

	if (!il_require_space(offsetof(XTIndHeadDataDRec, ilh_data) + head_size, ot->ot_thread))
		return FAILED;

	head_data = (XTIndHeadDataDPtr) (il_buffer + il_buffer_len);
	head_data->ilh_data_type = XT_DT_HEADER;
	XT_SET_DISK_2(head_data->ilh_head_size_2, head_size);
	memcpy(head_data->ilh_data, head_buf, head_size);

	il_buffer_len += offsetof(XTIndHeadDataDRec, ilh_data) + head_size;

	return OK;
}

xtBool XTIndexLog::il_flush(struct XTOpenTable *ot)
{
	XTIndLogHeadDRec	log_head;
	xtTableID			tab_id = ot->ot_table->tab_id;

	if (il_buffer_len) {
		if (!xt_pwrite_file(il_of, il_buffer_offset, il_buffer_len, il_buffer, &ot->ot_thread->st_statistics.st_ilog, ot->ot_thread))
			return FAILED;
		il_buffer_offset += il_buffer_len;
		il_buffer_len = 0;
	}

	if (il_log_eof != il_buffer_offset) {
		log_head.ilh_data_type = XT_DT_LOG_HEAD;
		XT_SET_DISK_4(log_head.ilh_tab_id_4, tab_id);
		XT_SET_DISK_4(log_head.ilh_log_eof_4, il_buffer_offset);

		if (!XT_IS_TEMP_TABLE(ot->ot_table->tab_dic.dic_tab_flags)) {
			if (!xt_flush_file(il_of, &ot->ot_thread->st_statistics.st_ilog, ot->ot_thread))
				return FAILED;
		}

		if (!xt_pwrite_file(il_of, 0, sizeof(XTIndLogHeadDRec), (xtWord1 *) &log_head, &ot->ot_thread->st_statistics.st_ilog, ot->ot_thread))
			return FAILED;

		if (!XT_IS_TEMP_TABLE(ot->ot_table->tab_dic.dic_tab_flags)) {
			if (!xt_flush_file(il_of, &ot->ot_thread->st_statistics.st_ilog, ot->ot_thread))
				return FAILED;
		}

		il_tab_id = tab_id;
		il_log_eof = il_buffer_offset;
	}
	return OK;
}

#ifdef CHECK_IF_WRITE_WAS_OK
static void check_buff(void *in_a, void *in_b, int len)
{
	xtWord1 *a = (xtWord1 *) in_a;
	xtWord1 *b = (xtWord1 *) in_b;
	int offset = 0;

	while (offset < len) {
		if (*a != *b) {
			printf("Missmatch at offset = %d %x != %x\n", offset, (int) *a, (int) *b);
			//xt_dump_trace();
			ASSERT_NS(FALSE);
		}
		offset++;
		a++;
		b++;
	}
}
#endif

xtBool XTIndexLog::il_apply_log_write(struct XTOpenTable *ot)
{
	XT_NODE_TEMP;
	register XTTableHPtr	tab = ot->ot_table;
	off_t					offset;
	size_t					pos;
	xtWord1					*buffer;
	off_t					address;
	xtIndexNodeID			node_id;
	size_t					req_size = 0;
	XTIdxBranchDPtr			node;
	u_int					block_len;
	u_int					block_offset;
	xtWord1					*block_header;
	xtWord1					*block_data;
#ifdef CHECK_IF_WRITE_WAS_OK
	XTIndReferenceRec		c_iref;
	XTIdxBranchDPtr			c_node;
	u_int					c_block_len;
#endif

	offset = 0;
	while (offset < il_log_eof) {
		if (offset < il_buffer_offset ||
			offset >= il_buffer_offset + (off_t) il_buffer_len) {
			il_buffer_len = il_buffer_size;
			if (il_log_eof - offset < (off_t) il_buffer_len)
				il_buffer_len = (size_t) (il_log_eof - offset);

			/* Corrupt log?! */
			if (il_buffer_len < req_size) {
				xt_register_ixterr(XT_REG_CONTEXT, XT_ERR_INDEX_LOG_CORRUPT, xt_file_path(il_of));
				xt_log_and_clear_exception_ns();
				return OK;
			}
			if (!xt_pread_file(il_of, offset, il_buffer_len, il_buffer_len, il_buffer, NULL, &ot->ot_thread->st_statistics.st_ilog, ot->ot_thread))
				return FAILED;
			il_buffer_offset = offset;
		}
		pos = (size_t) (offset - il_buffer_offset);
		ASSERT_NS(pos < il_buffer_len);
#ifdef CHECK_IF_WRITE_WAS_OK
		node_id = 0;
#endif
		buffer = il_buffer + pos;
		switch (*buffer) {
			case XT_DT_LOG_HEAD:
				req_size = sizeof(XTIndLogHeadDRec);
				if (il_buffer_len - pos < req_size) {
					il_buffer_len = 0;
					continue;
				}
				offset += req_size;
				req_size = 0;
				break;
			case XT_DT_SHORT_IND_PAGE: {
				XTIndShortPageDataDPtr		sp_data;

				req_size = offsetof(XTIndShortPageDataDRec, ild_data);
				if (il_buffer_len - pos < req_size) {
					il_buffer_len = 0;
					continue;
				}

				sp_data = (XTIndShortPageDataDPtr) buffer;
				node_id = XT_RET_NODE_ID(XT_GET_DISK_4(sp_data->ild_page_id_4));
				block_len = XT_GET_DISK_2(sp_data->ild_size_2);
				block_data = sp_data->ild_data;
				goto do_ind_page;
			}
			case XT_DT_INDEX_PAGE:
				XTIndPageDataDPtr	p_data;

				req_size = offsetof(XTIndPageDataDRec, ild_data);
				if (il_buffer_len - pos < req_size + 2) {
					il_buffer_len = 0;
					continue;
				}

				p_data = (XTIndPageDataDPtr) buffer;
				node_id = XT_RET_NODE_ID(XT_GET_DISK_4(p_data->ild_page_id_4));
				node = (XTIdxBranchDPtr) p_data->ild_data;
				block_len = XT_GET_INDEX_BLOCK_LEN(XT_GET_DISK_2(node->tb_size_2));
				block_data = p_data->ild_data;

				do_ind_page:
				if (block_len < 2 || block_len > XT_INDEX_PAGE_SIZE) {
					xt_register_taberr(XT_REG_CONTEXT, XT_ERR_INDEX_CORRUPTED, tab->tab_name);
					return FAILED;
				}

				req_size += block_len;
#ifdef IND_FILL_BLOCK_TO_NEXT
				/* Make sure we have the start of the next block in the buffer: 
				 * Should always work because a XT_DT_INDEX_PAGE is never the last
				 * block.
				 */
				if (il_buffer_len - pos < req_size + offsetof(XTIndPageDataDRec, ild_data))
#else
				if (il_buffer_len - pos < req_size)
#endif
				{
					il_buffer_len = 0;
					continue;
				}

				TRACK_BLOCK_FLUSH_N(node_id);
				address = xt_ind_node_to_offset(tab, node_id);
#ifdef IND_WRITE_IN_BLOCK_SIZES
				/* {WRITE-IN-BLOCKS} Round up the block size. Space has been provided. */
				block_len = ((block_len + IND_WRITE_BLOCK_SIZE - 1) / IND_WRITE_BLOCK_SIZE) * IND_WRITE_BLOCK_SIZE;
#endif
				IDX_TRACE("%d- W%x\n", (int) XT_NODE_ID(node_id), (int) XT_GET_DISK_2(block_data));
#ifdef IND_FILL_BLOCK_TO_NEXT
				if (block_len < XT_INDEX_PAGE_SIZE) {
					XTIndPageDataDPtr	next_page_data;

					next_page_data = (XTIndPageDataDPtr) (buffer + req_size);
					if (next_page_data->ild_data_type == XT_DT_INDEX_PAGE) {
						xtIndexNodeID next_node_id;

						next_node_id = XT_RET_NODE_ID(XT_GET_DISK_4(next_page_data->ild_page_id_4));
						/* Write the whole page, if that means leaving no gaps! */
						if (next_node_id == node_id+1)
							block_len = XT_INDEX_PAGE_SIZE;
					}
				}
#endif
				ASSERT_NS(block_len >= 2 && block_len <= XT_INDEX_PAGE_SIZE);
				if (!il_pwrite_file(ot, address, block_len, block_data))
					return FAILED;

				offset += req_size;
				req_size = 0;
				break;
			case XT_DT_SET_PAGE_HEAD: {
				XTIndSetPageHeadDataDPtr	sph_data;
	
				req_size = sizeof(XTIndSetPageHeadDataDRec);
				if (il_buffer_len - pos < req_size) {
					il_buffer_len = 0;
					continue;
				}

				sph_data = (XTIndSetPageHeadDataDPtr) buffer;
				node_id = XT_RET_NODE_ID(XT_GET_DISK_4(sph_data->ild_page_id_4));
				block_offset = 0;
				block_len = 0;
				block_header = sph_data->ild_page_head_2;
				block_data = NULL;
				goto do_mod_page;
			}
			case XT_DT_MOD_IND_PAGE_HEAD:
			case XT_DT_MOD_IND_PAGE_HEAD_EOB: {
				XTIndModPageHeadDataDPtr	mph_data;
	
				req_size = offsetof(XTIndModPageHeadDataDRec, ild_data);
				if (il_buffer_len - pos < req_size) {
					il_buffer_len = 0;
					continue;
				}

				mph_data = (XTIndModPageHeadDataDPtr) buffer;
				node_id = XT_RET_NODE_ID(XT_GET_DISK_4(mph_data->ild_page_id_4));
				block_offset = XT_GET_DISK_2(mph_data->ild_offset_2);
				block_len = XT_GET_DISK_2(mph_data->ild_size_2);
				block_header = mph_data->ild_page_head_2;
				block_data = mph_data->ild_data;
				goto do_mod_page;
			}
			case XT_DT_MOD_IND_PAGE:
			case XT_DT_MOD_IND_PAGE_EOB:
				XTIndModPageDataDPtr		mp_data;

				req_size = offsetof(XTIndModPageDataDRec, ild_data);
				if (il_buffer_len - pos < req_size) {
					il_buffer_len = 0;
					continue;
				}

				mp_data = (XTIndModPageDataDPtr) buffer;
				node_id = XT_RET_NODE_ID(XT_GET_DISK_4(mp_data->ild_page_id_4));
				block_offset = XT_GET_DISK_2(mp_data->ild_offset_2);
				block_len = XT_GET_DISK_2(mp_data->ild_size_2);
				block_header = NULL;
				block_data = mp_data->ild_data;

				do_mod_page:
				if (block_offset + block_len > XT_INDEX_PAGE_DATA_SIZE) {
					xt_register_taberr(XT_REG_CONTEXT, XT_ERR_INDEX_CORRUPTED, tab->tab_name);
					return FAILED;
				}

				req_size += block_len;
				if (il_buffer_len - pos < req_size) {
					il_buffer_len = 0;
					continue;
				}

				TRACK_BLOCK_FLUSH_N(node_id);
				address = xt_ind_node_to_offset(tab, node_id);
				/* {WRITE-IN-BLOCKS} Round up the block size. Space has been provided. */
				IDX_TRACE("%d- W%x\n", (int) XT_NODE_ID(node_id), (int) XT_GET_DISK_2(block_data));
				if (block_header) {
					if (!il_pwrite_file(ot, address, 2, block_header))
						return FAILED;
				}

				if (block_data) {
#ifdef IND_WRITE_IN_BLOCK_SIZES
					if (*buffer == XT_DT_MOD_IND_PAGE_HEAD_EOB || *buffer == XT_DT_MOD_IND_PAGE_EOB)
						block_len = ((block_len + IND_WRITE_BLOCK_SIZE - 1) / IND_WRITE_BLOCK_SIZE) * IND_WRITE_BLOCK_SIZE;
#endif
					if (!il_pwrite_file(ot, address + XT_INDEX_PAGE_HEAD_SIZE + block_offset, block_len, block_data))
						return FAILED;
				}

				offset += req_size;
				req_size = 0;
				break;
			case XT_DT_FREE_LIST:
				xtWord4	block, nblock;
				union {
					xtWord1				buffer[IND_WRITE_BLOCK_SIZE];
					XTIndFreeBlockRec	free_block;
				} x;
				off_t	aoff;

				memset(x.buffer, 0, sizeof(XTIndFreeBlockRec));

				pos++;
				offset++;
				
				for (;;) {
					req_size = 8;
					if (il_buffer_len - pos < req_size) {
						il_buffer_len = il_buffer_size;
						if (il_log_eof - offset < (off_t) il_buffer_len)
							il_buffer_len = (size_t) (il_log_eof - offset);
						/* Corrupt log?! */
						if (il_buffer_len < req_size) {
							xt_register_ixterr(XT_REG_CONTEXT, XT_ERR_INDEX_LOG_CORRUPT, xt_file_path(il_of));
							xt_log_and_clear_exception_ns();
							return OK;
						}
						if (!xt_pread_file(il_of, offset, il_buffer_len, il_buffer_len, il_buffer, NULL, &ot->ot_thread->st_statistics.st_ilog, ot->ot_thread))
							return FAILED;
						pos = 0;
					}
					block = XT_GET_DISK_4(il_buffer + pos);
					nblock = XT_GET_DISK_4(il_buffer + pos + 4);
					if (nblock == 0xFFFFFFFF)
						break;
					aoff = xt_ind_node_to_offset(tab, XT_RET_NODE_ID(block));
					XT_SET_DISK_8(x.free_block.if_next_block_8, nblock);
					IDX_TRACE("%d- *%x\n", (int) block, (int) XT_GET_DISK_2(x.buffer));
					if (!il_pwrite_file(ot, aoff, IND_WRITE_BLOCK_SIZE, x.buffer))
						return FAILED;
					pos += 4;
					offset += 4;
				}

				offset += 8;
				req_size = 0;
				break;
			case XT_DT_HEADER:
				XTIndHeadDataDPtr	head_data;
				size_t				len;

				req_size = offsetof(XTIndHeadDataDRec, ilh_data);
				if (il_buffer_len - pos < req_size) {
					il_buffer_len = 0;
					continue;
				}
				head_data = (XTIndHeadDataDPtr) buffer;
				len = XT_GET_DISK_2(head_data->ilh_head_size_2);

				req_size = offsetof(XTIndHeadDataDRec, ilh_data) + len;
				if (il_buffer_len - pos < req_size) {
					il_buffer_len = 0;
					continue;
				}

				if (!il_pwrite_file(ot, 0, len, head_data->ilh_data))
					return FAILED;

				offset += req_size;
				req_size = 0;
				break;
			case XT_DT_2_MOD_IND_PAGE:
			case XT_DT_2_MOD_IND_PAGE_EOB:
				XTIndDoubleModPageDataDPtr	dd_data;
				u_int						block_len2;

				req_size = offsetof(XTIndDoubleModPageDataDRec, dld_data);
				if (il_buffer_len - pos < req_size) {
					il_buffer_len = 0;
					continue;
				}

				dd_data = (XTIndDoubleModPageDataDPtr) buffer;
				node_id = XT_RET_NODE_ID(XT_GET_DISK_4(dd_data->dld_page_id_4));
				block_len = XT_GET_DISK_2(dd_data->dld_size1_2);
				block_offset = XT_GET_DISK_2(dd_data->dld_offset2_2);
				block_len2 = XT_GET_DISK_2(dd_data->dld_size2_2);
				block_data = dd_data->dld_data;

				req_size += block_len + block_len2;
				if (il_buffer_len - pos < req_size)
				{
					il_buffer_len = 0;
					continue;
				}

				TRACK_BLOCK_FLUSH_N(node_id);
				address = xt_ind_node_to_offset(tab, node_id);
				IDX_TRACE("%d- W%x\n", (int) XT_NODE_ID(node_id), (int) XT_GET_DISK_2(block_data));
				if (!il_pwrite_file(ot, address, block_len, block_data))
					return FAILED;

#ifdef IND_WRITE_IN_BLOCK_SIZES
				if (*buffer == XT_DT_2_MOD_IND_PAGE_EOB)
					block_len2 = ((block_len2 + IND_WRITE_BLOCK_SIZE - 1) / IND_WRITE_BLOCK_SIZE) * IND_WRITE_BLOCK_SIZE;
#endif
				if (!il_pwrite_file(ot, address + XT_INDEX_PAGE_HEAD_SIZE + block_offset, block_len2, block_data + block_len))
					return FAILED;

				offset += req_size;
				req_size = 0;
				break;
			default:
				xt_register_ixterr(XT_REG_CONTEXT, XT_ERR_INDEX_LOG_CORRUPT, xt_file_path(il_of));
				xt_log_and_clear_exception_ns();
				return OK;
		}
#ifdef CHECK_IF_WRITE_WAS_OK
		if (node_id) {
			if (!xt_ind_get(ot, node_id, &c_iref))
				ASSERT_NS(FALSE);
			if (c_iref.ir_block) {
				c_node = (XTIdxBranchDPtr) c_iref.ir_block->cb_data;
				c_block_len = XT_GET_INDEX_BLOCK_LEN(XT_GET_DISK_2(c_node->tb_size_2));

				if (!xt_pread_file(ot->ot_ind_file, address, XT_INDEX_PAGE_SIZE, 0, &ot->ot_ind_tmp_buf, NULL, &ot->ot_thread->st_statistics.st_ilog, ot->ot_thread))
					ASSERT_NS(FALSE);
				if (c_iref.ir_block->cb_min_pos == 0xFFFF)
					check_buff(&ot->ot_ind_tmp_buf, c_node, c_block_len);
				else {
					if (!c_iref.ir_block->cb_header)
						check_buff(&ot->ot_ind_tmp_buf, c_node, 2);
					check_buff(ot->ot_ind_tmp_buf.tb_data, c_node->tb_data, c_iref.ir_block->cb_min_pos);
					check_buff(ot->ot_ind_tmp_buf.tb_data + c_iref.ir_block->cb_max_pos,
						c_node->tb_data + c_iref.ir_block->cb_max_pos,
						c_block_len - XT_INDEX_PAGE_HEAD_SIZE - c_iref.ir_block->cb_max_pos);
				}
				xt_ind_release(ot, NULL, XT_UNLOCK_WRITE, &c_iref);
			}
		}
#endif
		if (il_bytes_written >= IND_FLUSH_THRESHOLD) {
			if (!il_flush_file(ot))
				return FAILED;
		}
	}
	return OK;
}

xtBool XTIndexLog::il_apply_log_flush(struct XTOpenTable *ot)
{
	register XTTableHPtr	tab = ot->ot_table;
	XTIndLogHeadDRec		log_head;

#ifdef PRINT_IND_FLUSH_STATS
	xtWord8					b_flush_time = ot->ot_thread->st_statistics.st_ind.ts_flush_time;
#endif
	if (!il_flush_file(ot))
		return FAILED;
#ifdef PRINT_IND_FLUSH_STATS
	char	buf1[30];
	char	buf2[30];
	char	buf3[30];

	double time;
	double kb;

	ot->ot_table->tab_ind_flush_time += ot->ot_thread->st_statistics.st_ind.ts_flush_time - b_flush_time;
	ot->ot_table->tab_ind_flush++;

	time = (double) ot->ot_table->tab_ind_flush_time / (double) 1000000 / (double) ot->ot_table->tab_ind_flush;
	kb = (double) ot->ot_table->tab_ind_write / (double) ot->ot_table->tab_ind_flush / (double) 1024;
	printf("TIME: %s      Kbytes: %s      Mps: %s      Flush Count: %d\n", 
		idx_format(buf1, time),
		idx_format(buf2, kb),
		idx_format_mb(buf3, kb / time),
		(int) ot->ot_table->tab_ind_flush
		);
#endif

	log_head.ilh_data_type = XT_DT_LOG_HEAD;
	XT_SET_DISK_4(log_head.ilh_tab_id_4, il_tab_id);
	XT_SET_DISK_4(log_head.ilh_log_eof_4, 0);

	if (!xt_pwrite_file(il_of, 0, sizeof(XTIndLogHeadDRec), (xtWord1 *) &log_head, &ot->ot_thread->st_statistics.st_ilog, ot->ot_thread))
		return FAILED;

	if (!XT_IS_TEMP_TABLE(tab->tab_dic.dic_tab_flags)) {
		if (!xt_flush_file(il_of, &ot->ot_thread->st_statistics.st_ilog, ot->ot_thread))
			return FAILED;
	}
	return OK;
}

inline xtBool XTIndexLog::il_pwrite_file(struct XTOpenTable *ot, off_t offs, size_t siz, void *dat)
{
#ifdef IND_WRITE_IN_BLOCK_SIZES
	ASSERT_NS(((offs) % IND_WRITE_BLOCK_SIZE) == 0);
	ASSERT_NS(((siz) % IND_WRITE_BLOCK_SIZE) == 0);
#endif
	il_bytes_written += siz;
#ifdef PRINT_IND_FLUSH_STATS
	xtBool ok;

	u_int	b_write = ot->ot_thread->st_statistics.st_ind.ts_write;
	ok = xt_pwrite_file(ot->ot_ind_file, offs, siz, dat, &ot->ot_thread->st_statistics.st_ind, ot->ot_thread);
	ot->ot_table->tab_ind_write += ot->ot_thread->st_statistics.st_ind.ts_write - b_write;
	return ok;
#else
	return xt_pwrite_file(ot->ot_ind_file, offs, siz, dat, &ot->ot_thread->st_statistics.st_ind, ot->ot_thread);
#endif
}

inline xtBool XTIndexLog::il_flush_file(struct XTOpenTable *ot)
{
	xtBool ok = TRUE;

	il_bytes_written = 0;
	if (!XT_IS_TEMP_TABLE(ot->ot_table->tab_dic.dic_tab_flags)) {
		ok = xt_flush_file(ot->ot_ind_file, &ot->ot_thread->st_statistics.st_ind, ot->ot_thread);
	}
	return ok;
}

xtBool XTIndexLog::il_open_table(struct XTOpenTable **ot)
{
	return xt_db_open_pool_table_ns(ot, il_pool->ilp_db, il_tab_id);
}

void XTIndexLog::il_close_table(struct XTOpenTable *ot)
{
	xt_db_return_table_to_pool_ns(ot);
}


