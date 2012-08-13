/*
   Copyright (c) 2005, 2011, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

/*
  InnoDB offline file checksum utility.  85% of the code in this file
  was taken wholesale fron the InnoDB codebase.

  The final 15% was originally written by Mark Smith of Danga
  Interactive, Inc. <junior@danga.com>

  Published with a permission.
*/

#include <my_global.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <zlib.h>
#include <string.h>

#undef max
#undef min

#include <map>

/* all of these ripped from InnoDB code from MySQL 4.0.22 */
#define UT_HASH_RANDOM_MASK     1463735687
#define UT_HASH_RANDOM_MASK2    1653893711
#define FIL_PAGE_LSN          16 
#define FIL_PAGE_FILE_FLUSH_LSN 26
#define FIL_PAGE_OFFSET     4
#define FIL_PAGE_TYPE           24
#define FIL_PAGE_DATA       38
#define FIL_PAGE_END_LSN_OLD_CHKSUM 8
#define FIL_PAGE_SPACE_OR_CHKSUM 0
#define FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID  34

#define FIL_PAGE_INDEX		17855	/*!< B-tree node */
#define FIL_PAGE_UNDO_LOG	2	/*!< Undo log page */
#define FIL_PAGE_INODE		3	/*!< Index node */
#define FIL_PAGE_IBUF_FREE_LIST	4	/*!< Insert buffer free list */
#define FIL_PAGE_TYPE_ALLOCATED	0	/*!< Freshly allocated page */
#define FIL_PAGE_IBUF_BITMAP	5	/*!< Insert buffer bitmap */
#define FIL_PAGE_TYPE_SYS	6	/*!< System page */
#define FIL_PAGE_TYPE_TRX_SYS	7	/*!< Transaction system data */
#define FIL_PAGE_TYPE_FSP_HDR	8	/*!< File space header */
#define FIL_PAGE_TYPE_XDES	9	/*!< Extent descriptor page */
#define FIL_PAGE_TYPE_BLOB	10	/*!< Uncompressed BLOB page */
#define FIL_PAGE_TYPE_ZBLOB	11	/*!< First compressed BLOB page */
#define FIL_PAGE_TYPE_ZBLOB2	12	/*!< Subsequent compressed BLOB page */

#define	PAGE_LEVEL	 26	/* level of the node in an index tree; the
				leaf level is the level 0.  This field should
				not be written to after page creation. */
#define	PAGE_N_RECS	 16	/* number of user records on the page */
#define PAGE_INDEX_ID    28   
#define	PAGE_HEAP_TOP	 2	/* pointer to record heap top */
#define	PAGE_N_HEAP	 4	/* number of records in the heap,
				bit 15=flag: new-style compact page format */
#define	PAGE_FREE	 6	/* pointer to start of page free record list */
#define	PAGE_GARBAGE	 8	/* number of bytes in deleted records */


#define FSEG_PAGE_DATA          FIL_PAGE_DATA
#define FSEG_HEADER_SIZE	10	/*!< Length of the file system
					header, in bytes */
#define PAGE_HEADER     	FSEG_PAGE_DATA
#define TRX_UNDO_PAGE_HDR       FSEG_PAGE_DATA


#define REC_N_NEW_EXTRA_BYTES   5
#define REC_N_OLD_EXTRA_BYTES   6

/*----*/
#define PAGE_DATA	(PAGE_HEADER + 36 + 2 * FSEG_HEADER_SIZE)
				/* start of data on the page */

#define PAGE_OLD_INFIMUM	(PAGE_DATA + 1 + REC_N_OLD_EXTRA_BYTES)
				/* offset of the page infimum record on an
				old-style page */
#define PAGE_OLD_SUPREMUM	(PAGE_DATA + 2 + 2 * REC_N_OLD_EXTRA_BYTES + 8)
				/* offset of the page supremum record on an
				old-style page */
#define PAGE_OLD_SUPREMUM_END (PAGE_OLD_SUPREMUM + 9)
				/* offset of the page supremum record end on
				an old-style page */
#define PAGE_NEW_INFIMUM	(PAGE_DATA + REC_N_NEW_EXTRA_BYTES)
				/* offset of the page infimum record on a
				new-style compact page */
#define PAGE_NEW_SUPREMUM	(PAGE_DATA + 2 * REC_N_NEW_EXTRA_BYTES + 8)
				/* offset of the page supremum record on a
				new-style compact page */
#define PAGE_NEW_SUPREMUM_END (PAGE_NEW_SUPREMUM + 8)
				/* offset of the page supremum record end on
				a new-style compact page */
/*-----------------------------*/

#define FIL_ADDR_SIZE 6
#define FLST_NODE_SIZE          (2 * FIL_ADDR_SIZE)

#define TRX_UNDO_PAGE_HDR_SIZE  (6 + FLST_NODE_SIZE)
#define TRX_UNDO_SEG_HDR        (TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_HDR_SIZE)
#define TRX_UNDO_STATE          0       /*!< TRX_UNDO_ACTIVE, ... */

#define TRX_UNDO_PAGE_TYPE      0       /*!< TRX_UNDO_INSERT or TRX_UNDO_UPDATE */

/* Types of an undo log segment */
#define TRX_UNDO_INSERT         1       /* contains undo entries for inserts */
#define TRX_UNDO_UPDATE         2       /* contains undo entries for updates
                                        and delete markings: in short,
                                        modifys (the name 'UPDATE' is a
                                        historical relic) */
/* States of an undo log segment */
#define TRX_UNDO_ACTIVE         1       /* contains an undo log of an active
                                        transaction */
#define TRX_UNDO_CACHED         2       /* cached for quick reuse */
#define TRX_UNDO_TO_FREE        3       /* insert undo segment can be freed */
#define TRX_UNDO_TO_PURGE       4       /* update undo segment will not be
                                        reused: it can be freed in purge when
                                        all undo data in it is removed */
#define TRX_UNDO_PREPARED       5       /* contains an undo log of an
                                        prepared transaction */

#define PAGE_ZIP_MIN_SIZE_SHIFT 10
#define PAGE_ZIP_MIN_SIZE       (1 << PAGE_ZIP_MIN_SIZE_SHIFT)
#define FSP_SPACE_FLAGS         16
#define DICT_TF_ZSSIZE_SHIFT    1
#define DICT_TF_ZSSIZE_MASK     (15 << DICT_TF_ZSSIZE_SHIFT)

/* command line argument to do page checks (that's it) */
/* another argument to specify page ranges... seek to right spot and go from there */

typedef unsigned long int ulint;
typedef unsigned char uchar;
typedef unsigned int uint32;
typedef unsigned long ulong;
typedef unsigned int ib_uint32_t;

extern "C" void my_init_cpu_optimizations();
extern "C" uint32 my_fast_crc32(const uchar* data, ulong length);

/** Type definition for a 64-bit unsigned integer, which works also
in 32-bit machines. NOTE! Access the fields only with the accessor
functions. This definition appears here only for the compiler to
know the size of a dulint. */
struct dulint_struct{
	ulint	high;	/*!< most significant 32 bits */
	ulint	low;	/*!< least significant 32 bits */
};
typedef	struct dulint_struct	dulint;

int n_undo_state_active;
int n_undo_state_cached;
int n_undo_state_to_free;
int n_undo_state_to_purge;
int n_undo_state_prepared;
int n_undo_state_other;
int n_undo_insert, n_undo_update, n_undo_other;
int n_bad_checksum;
int n_fil_page_index;
int n_fil_page_undo_log;
int n_fil_page_inode;
int n_fil_page_ibuf_free_list;
int n_fil_page_allocated;
int n_fil_page_ibuf_bitmap;
int n_fil_page_type_sys;
int n_fil_page_type_trx_sys;
int n_fil_page_type_fsp_hdr;
int n_fil_page_type_allocated;
int n_fil_page_type_xdes;
int n_fil_page_type_blob;
int n_fil_page_type_zblob;
int n_fil_page_type_other;

int n_fil_page_max_index_id;

#define MAX_DATA_BYTES_PER_PAGE 16384
#define SIZE_RANGES_FOR_PAGE 10
#define NUM_RETRIES 3
#define DEFAULT_RETRY_DELAY 1000000

struct per_index_stats {
  unsigned long long pages;
  unsigned long long leaf_pages;
  unsigned long long total_n_recs;
  unsigned long long total_data_bytes;
  unsigned long long pages_in_size_range[SIZE_RANGES_FOR_PAGE+2]; /*!< first element for empty pages, last element for pages with more than MAX_DATA_BYTES_PER_PAGE */
  per_index_stats():pages(0), leaf_pages(0), total_n_recs(0),
		    total_data_bytes(0)
    {
      memset(pages_in_size_range, 0, sizeof(pages_in_size_range));
    }
};

std::map<unsigned long long, per_index_stats> index_ids;

/*******************************************************//**
Gets the high-order 32 bits of a dulint.
@return	32 bits in ulint */
ulint
ut_dulint_get_high(
/*===============*/
	dulint	d)	/*!< in: dulint */
{
	return(d.high);
}

/*******************************************************//**
Gets the low-order 32 bits of a dulint.
@return	32 bits in ulint */
ulint
ut_dulint_get_low(
/*==============*/
	dulint	d)	/*!< in: dulint */
{
	return(d.low);
}

/*******************************************************//**
Gets the low-order 32 bits of a dulint.
@return	32 bits in ulint */
unsigned long long
ut_dulint_to_ull(
/*==============*/
	dulint	d)	/*!< in: dulint */
{
	return(((unsigned long long) d.high << 32) |
		(unsigned long long) d.low);
}

/*******************************************************//**
Creates a 64-bit dulint out of two ulints.
@return	created dulint */
dulint
ut_dulint_create(
/*=============*/
	ulint	high,	/*!< in: high-order 32 bits */
	ulint	low)	/*!< in: low-order 32 bits */
{
	dulint	res;

	res.high = high;
	res.low	 = low;

	return(res);
}

/* innodb function in name; modified slightly to not have the ASM version (lots of #ifs that didn't apply) */
ulint mach_read_from_4(const uchar *b)
{
  return( ((ulint)(b[0]) << 24)
          + ((ulint)(b[1]) << 16)
          + ((ulint)(b[2]) << 8)
          + (ulint)(b[3])
          );
}

/********************************************************//**
The following function is used to fetch data from 2 consecutive
bytes. The most significant byte is at the lowest address.
@return	ulint integer */
ulint
mach_read_from_2(
/*=============*/
	const uchar*	b)	/*!< in: pointer to 2 bytes */
{
	return( ((ulint)(b[0]) << 8)
		+ (ulint)(b[1])
		);
}

/********************************************************//**
The following function is used to fetch data from 8 consecutive
bytes. The most significant byte is at the lowest address.
@return	dulint integer */
dulint
mach_read_from_8(
/*=============*/
	const uchar*	b)	/*!< in: pointer to 8 bytes */
{
	ulint	high;
	ulint	low;

	high = mach_read_from_4(b);
	low = mach_read_from_4(b + 4);

	return(ut_dulint_create(high, low));
}

/*************************************************************//**
Reads the given header field. */
ulint
page_header_get_field(
/*==================*/
	const uchar*	page,	/*!< in: page */
	ulint		field)	/*!< in: PAGE_LEVEL, ... */
{
	return(mach_read_from_2(page + PAGE_HEADER + field));
}


ulint
page_get_n_recs(
/*============*/
	const uchar*	page)	/*!< in: index page */
{
	return(page_header_get_field(page, PAGE_N_RECS));
}


/************************************************************//**
Determine whether the page is in new-style compact format.
@return nonzero if the page is in compact format, zero if it is in
old-style format */
ulint
page_is_comp(
/*=========*/
	const uchar*	page)	/*!< in: index page */
{
	return (page_header_get_field(page, PAGE_N_HEAP) & 0x8000) != 0;
}

/************************************************************//**
Returns the sum of the sizes of the records in the record list, excluding
the infimum and supremum records.
@return data in bytes */
ulint
page_get_data_size(
/*===============*/
	const uchar*   page)   /*!< in: index page */
{
	ulint   ret;

	ret = (ulint)(page_header_get_field(page, PAGE_HEAP_TOP)
		- (page_is_comp(page)
		   ? PAGE_NEW_SUPREMUM_END
		   : PAGE_OLD_SUPREMUM_END)
		- page_header_get_field(page, PAGE_GARBAGE));

	return(ret);
}

ulint
page_get_page_no(
/*=============*/
	const uchar*	page)	/*!< in: page */
{
	return(mach_read_from_4(page + FIL_PAGE_OFFSET));
}

/*********************************************************************//**
Gets the file page type.
@return type; NOTE that if the type has not been written to page, the
return value not defined */
ulint
fil_page_get_type(
/*==============*/
	uchar*	page)	/*!< in: file page */
{
	return(mach_read_from_2(page + FIL_PAGE_TYPE));
}

/**************************************************************//**
Gets the index id field of a page.
@return	index id */
dulint
btr_page_get_index_id(
/*==================*/
	uchar*	page)	/*!< in: index page */
{
	return(mach_read_from_8(page + PAGE_HEADER + PAGE_INDEX_ID));
}

ulint
ut_fold_ulint_pair(
/*===============*/
            /* out: folded value */
    ulint   n1, /* in: ulint */
    ulint   n2) /* in: ulint */
{
    return(((((n1 ^ n2 ^ UT_HASH_RANDOM_MASK2) << 8) + n1)
                        ^ UT_HASH_RANDOM_MASK) + n2);
}

ulint
ut_fold_binary(
/*===========*/
            /* out: folded value */
    uchar*   str,    /* in: string of bytes */
    ulint   len)    /* in: length */
{
    ulint   i;
    ulint   fold= 0;

    for (i= 0; i < len; i++)
    {
      fold= ut_fold_ulint_pair(fold, (ulint)(*str));

      str++;
    }

    return(fold);
}

/**********************************************************************//**
Calculate the compressed page checksum.
@return	page checksum */
ulint
page_zip_calc_checksum_old(
/*===================*/
	const void*	data,	/*!< in: compressed page */
	ulint		size)	/*!< in: size of compressed page */
{
	/* Exclude FIL_PAGE_SPACE_OR_CHKSUM, FIL_PAGE_LSN,
	and FIL_PAGE_FILE_FLUSH_LSN from the checksum. */
	const Bytef*	s	= (const Bytef*)data;
	uLong		adler;

	adler = adler32(0L, s + FIL_PAGE_OFFSET,
			FIL_PAGE_LSN - FIL_PAGE_OFFSET);
	adler = adler32(adler, s + FIL_PAGE_TYPE, 2);
	adler = adler32(adler, s + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID,
			size - FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);

	return((ulint) adler);
}

/**********************************************************************//**
Calculate the compressed page checksum.
@return	page checksum */
ulint
page_zip_calc_checksum_fast(
/*===================*/
	const void*	data,	/*!< in: compressed page */
	ulint		size)	/*!< in: size of compressed page */
{
	/* Exclude FIL_PAGE_SPACE_OR_CHKSUM, FIL_PAGE_LSN,
	and FIL_PAGE_FILE_FLUSH_LSN from the checksum. */

	const Bytef*	s	= (const Bytef*)data;
	ib_uint32_t crc32;

	/* nizam: This is compatible with 5.6 page_zip_calc_checksum() */
	crc32 = my_fast_crc32(s + FIL_PAGE_OFFSET,
	                      FIL_PAGE_LSN - FIL_PAGE_OFFSET)
	        ^ my_fast_crc32(s + FIL_PAGE_TYPE, 2)
	        ^ my_fast_crc32(s + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID,
	                        size - FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);

	return((ulint) crc32);
}


ulint
buf_calc_page_fast_checksum(
/*=======================*/
               /* out: checksum */
    uchar*    page, /* in: buffer page */
    ulint     page_size)
{
    ulint checksum;

    /* Since the fields FIL_PAGE_FILE_FLUSH_LSN and ..._ARCH_LOG_NO
    are written outside the buffer pool to the first pages of data
    files, we have to skip them in the page checksum calculation.
    We must also skip the field FIL_PAGE_SPACE_OR_CHKSUM where the
    checksum is stored, and also the last 8 bytes of page because
    there we store the old formula checksum. */

    checksum= my_fast_crc32(page + FIL_PAGE_OFFSET,
                             FIL_PAGE_FILE_FLUSH_LSN - FIL_PAGE_OFFSET)
            + my_fast_crc32(page + FIL_PAGE_DATA,
                             page_size - FIL_PAGE_DATA
                             - FIL_PAGE_END_LSN_OLD_CHKSUM);
    checksum= checksum & 0xFFFFFFFF;

    return(checksum);
}

ulint
buf_calc_page_new_checksum(
/*=======================*/
               /* out: checksum */
    uchar*    page, /* in: buffer page */
    ulint     page_size)
{
    ulint checksum;

    /* Since the fields FIL_PAGE_FILE_FLUSH_LSN and ..._ARCH_LOG_NO
    are written outside the buffer pool to the first pages of data
    files, we have to skip them in the page checksum calculation.
    We must also skip the field FIL_PAGE_SPACE_OR_CHKSUM where the
    checksum is stored, and also the last 8 bytes of page because
    there we store the old formula checksum. */

    checksum= ut_fold_binary(page + FIL_PAGE_OFFSET,
                             FIL_PAGE_FILE_FLUSH_LSN - FIL_PAGE_OFFSET)
            + ut_fold_binary(page + FIL_PAGE_DATA,
                             page_size - FIL_PAGE_DATA
                             - FIL_PAGE_END_LSN_OLD_CHKSUM);
    checksum= checksum & 0xFFFFFFFF;

    return(checksum);
}

ulint
buf_calc_page_old_checksum(
/*=======================*/
               /* out: checksum */
    uchar*    page) /* in: buffer page */
{
    ulint checksum;

    checksum= ut_fold_binary(page, FIL_PAGE_FILE_FLUSH_LSN);

    checksum= checksum & 0xFFFFFFFF;

    return(checksum);
}

unsigned char
page_is_leaf(
/*=========*/
	const uchar*	page)	/*!< in: page */
{
	return(!*(const unsigned short*) (page + (PAGE_HEADER + PAGE_LEVEL)));
}


void
parse_page(
/*=======*/
	uchar* page,  /* in: buffer page */
	int per_page_details) /* in: print out per-page details */
{
	unsigned long long id;
	ulint x;
	ulint n_recs;
	ulint page_no;
	ulint data_bytes;
	int is_leaf;
	int size_range_id;

	n_recs = page_get_n_recs(page);
	page_no = page_get_page_no(page);
	data_bytes = page_get_data_size(page);
	is_leaf = page_is_leaf(page);
	size_range_id = (data_bytes * SIZE_RANGES_FOR_PAGE
                         + MAX_DATA_BYTES_PER_PAGE - 1) / MAX_DATA_BYTES_PER_PAGE;
	if (size_range_id > SIZE_RANGES_FOR_PAGE + 1) {
	  /* data_bytes is bigger than MAX_DATA_BYTES_PER_PAGE */
	  size_range_id = SIZE_RANGES_FOR_PAGE + 1;
	}

	switch (fil_page_get_type(page)) {
	case FIL_PAGE_INDEX:
		n_fil_page_index++;
		id = ut_dulint_to_ull(btr_page_get_index_id(page));
		if (per_page_details) {
			printf("index %Lu page %lu leaf %u n_recs %lu data_bytes %lu\n",
			       id, page_no, is_leaf, n_recs, data_bytes);
		}
		/* update per-index statistics */
		if (index_ids.count(id) == 0) {
		  index_ids.insert(std::make_pair(id, per_index_stats()));
		}
		index_ids[id].pages++;
		if (is_leaf) index_ids[id].leaf_pages++;
		index_ids[id].total_n_recs += n_recs;
		index_ids[id].total_data_bytes += data_bytes;
		index_ids[id].pages_in_size_range[size_range_id] ++;
		break;
	case FIL_PAGE_UNDO_LOG:
		if (per_page_details) {
			printf("FIL_PAGE_UNDO_LOG\n");
		}
		n_fil_page_undo_log++;
		x = mach_read_from_2(page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE);
		if (x == TRX_UNDO_INSERT)
			n_undo_insert++;
		else if (x == TRX_UNDO_UPDATE)
			n_undo_update++;
		else
			n_undo_other++;

		x = mach_read_from_2(page + TRX_UNDO_SEG_HDR + TRX_UNDO_STATE);
		switch (x) {
			case TRX_UNDO_ACTIVE: n_undo_state_active++; break;
			case TRX_UNDO_CACHED: n_undo_state_cached++; break;
			case TRX_UNDO_TO_FREE: n_undo_state_to_free++; break;
			case TRX_UNDO_TO_PURGE: n_undo_state_to_purge++; break;
			case TRX_UNDO_PREPARED: n_undo_state_prepared++; break;
			default: n_undo_state_other++; break;
		}
		break;
	case FIL_PAGE_INODE:
		if (per_page_details) {
			printf("FIL_PAGE_INODE\n");
		}
		n_fil_page_inode++;
		break;
	case FIL_PAGE_IBUF_FREE_LIST:
		if (per_page_details) {
			printf("FIL_PAGE_IBUF_FREE_LIST\n");
		}
		n_fil_page_ibuf_free_list++;
		break;
	case FIL_PAGE_TYPE_ALLOCATED:
		if (per_page_details) {
			printf("FIL_PAGE_TYPE_ALLOCATED\n");
		}
		n_fil_page_type_allocated++;
		break;
	case FIL_PAGE_IBUF_BITMAP:
		if (per_page_details) {
			printf("FIL_PAGE_IBUF_BITMAP\n");
		}
		n_fil_page_ibuf_bitmap++;
		break;
	case FIL_PAGE_TYPE_SYS:
		if (per_page_details) {
			printf("FIL_PAGE_TYPE_SYS\n");
		}
		n_fil_page_type_sys++;
		break;
	case FIL_PAGE_TYPE_TRX_SYS:
		if (per_page_details) {
			printf("FIL_PAGE_TYPE_TRX_SYS\n");
		}
		n_fil_page_type_trx_sys++;
		break;
	case FIL_PAGE_TYPE_FSP_HDR:
		if (per_page_details) {
			printf("FIL_PAGE_TYPE_FSP_HDR\n");
		}
		n_fil_page_type_fsp_hdr++;
		break;
	case FIL_PAGE_TYPE_XDES:
		if (per_page_details) {
			printf("FIL_PAGE_TYPE_XDES\n");
		}
		n_fil_page_type_xdes++;
/*		Can we find out the index id here? *** */
		break;
	case FIL_PAGE_TYPE_BLOB:
		if (per_page_details) {
			printf("FIL_PAGE_TYPE_BLOB\n");
		}
		n_fil_page_type_blob++;
		break;
	case FIL_PAGE_TYPE_ZBLOB:
	case FIL_PAGE_TYPE_ZBLOB2:
		if (per_page_details) {
			printf("FIL_PAGE_TYPE_ZBLOB/2\n");
		}
		n_fil_page_type_zblob++;
		break;
	default:
		if (per_page_details) {
			printf("FIL_PAGE_TYPE_OTHER\n");
		}
		n_fil_page_type_other++;
	}
}

void
print_stats()
/*========*/
{
	unsigned long long i;

	printf("%d\tbad checksum\n", n_bad_checksum);
	printf("%d\tFIL_PAGE_INDEX\n", n_fil_page_index);
	printf("%d\tFIL_PAGE_UNDO_LOG\n", n_fil_page_undo_log);
	printf("%d\tFIL_PAGE_INODE\n", n_fil_page_inode);
	printf("%d\tFIL_PAGE_IBUF_FREE_LIST\n", n_fil_page_ibuf_free_list);
	printf("%d\tFIL_PAGE_TYPE_ALLOCATED\n", n_fil_page_type_allocated);
	printf("%d\tFIL_PAGE_IBUF_BITMAP\n", n_fil_page_ibuf_bitmap);
	printf("%d\tFIL_PAGE_TYPE_SYS\n", n_fil_page_type_sys);
	printf("%d\tFIL_PAGE_TYPE_TRX_SYS\n", n_fil_page_type_trx_sys);
	printf("%d\tFIL_PAGE_TYPE_FSP_HDR\n", n_fil_page_type_fsp_hdr);
	printf("%d\tFIL_PAGE_TYPE_XDES\n", n_fil_page_type_xdes);
	printf("%d\tFIL_PAGE_TYPE_BLOB\n", n_fil_page_type_blob);
	printf("%d\tFIL_PAGE_TYPE_ZBLOB\n", n_fil_page_type_zblob);
	printf("%d\tother\n", n_fil_page_type_other);
	printf("%d\tmax index_id\n", n_fil_page_max_index_id);
	printf("undo type: %d insert, %d update, %d other\n",
		n_undo_insert, n_undo_update, n_undo_other);
	printf("undo state: %d active, %d cached, %d to_free, %d to_purge,"
		" %d prepared, %d other\n", n_undo_state_active,
		n_undo_state_cached, n_undo_state_to_free,
		n_undo_state_to_purge, n_undo_state_prepared, n_undo_state_other);

	printf("index_id\t#pages\t\t#leaf_pages\t#recs_per_page\t#bytes_per_page\n");
	for (std::map<unsigned long long, per_index_stats>::const_iterator it
	       = index_ids.begin(); it != index_ids.end(); it++) {
	  const per_index_stats& index = it->second;
	  printf("%lld\t\t%lld\t\t%lld\t\t%lld\t\t%lld\n",
		 it->first, index.pages, index.leaf_pages,
		 index.total_n_recs / index.pages,
		 index.total_data_bytes / index.pages);
	}
	printf("\n");
	printf("index_id\tpage_data_bytes_histgram(empty,...,oversized)\n");
	for (std::map<unsigned long long, per_index_stats>::const_iterator it
	       = index_ids.begin(); it != index_ids.end(); it++) {
	  printf("%lld\t", it->first);
	  const per_index_stats& index = it->second;
	  for (i = 0; i < SIZE_RANGES_FOR_PAGE+2; i++) {
	    printf("\t%lld", index.pages_in_size_range[i]);
	  }
	  printf("\n");
	}
}

int lsn_match(uchar* p, ulint page_no, ulint page_size, int compressed, int debug) {
  ulint logseq, logseqfield;
  logseq= mach_read_from_4(p + FIL_PAGE_LSN + 4);
  logseqfield= mach_read_from_4(p + page_size - FIL_PAGE_END_LSN_OLD_CHKSUM + 4);
  if (debug) {
    printf("page %lu: log sequence number: first = %lu; second = %lu\n", page_no, logseq, logseqfield);
    if (compressed && (logseq == logseqfield))
      printf("WARNING: lsns should not always match for compressed pages!\n");
  }
  return logseq == logseqfield;
}

int checksum_match(uchar* p, ulint page_no, ulint page_size, int compressed, int debug) {
  ulint csum, csumfield, oldcsumfield, oldcsum, fastcsum;
  if (compressed) {
    csumfield= mach_read_from_4(p + FIL_PAGE_SPACE_OR_CHKSUM);
    fastcsum = page_zip_calc_checksum_fast(p, page_size);
    if (fastcsum == csumfield) {
      if (debug)
        printf("page %lu: fastcsum = %lu; recorded = %lu\n", page_no, fastcsum, csumfield);
      return 1;
    }
    oldcsum = page_zip_calc_checksum_old(p, page_size);
    if (debug)
      printf("page %lu: oldcsum = %lu; fastcsum = %lu; recorded = %lu\n", page_no, oldcsum, fastcsum, csumfield);
    return oldcsum == csumfield;
  } else {
    /* check the "stored log sequence numbers" */
    if (!lsn_match(p, page_no, page_size, 0, debug))
    {
      return 0;
    }
    /* check old method of checksumming */
    oldcsum= buf_calc_page_old_checksum(p);
    oldcsumfield= mach_read_from_4(p + page_size - FIL_PAGE_END_LSN_OLD_CHKSUM);
    if (debug)
      printf("page %lu: old style: calculated = %lu; recorded = %lu\n", page_no, oldcsum, oldcsumfield);
    if (oldcsumfield != mach_read_from_4(p + FIL_PAGE_LSN) && oldcsumfield != oldcsum)
    {
      return 0;
    }
    /* now check the new method */
    csum= buf_calc_page_new_checksum(p, page_size);
    fastcsum= buf_calc_page_fast_checksum(p, page_size);
    csumfield= mach_read_from_4(p + FIL_PAGE_SPACE_OR_CHKSUM);
    if (debug)
      printf("page %lu: new style: calculated = %lu; fast = %lu; recorded = %lu\n",
             page_no, csum, fastcsum, csumfield);
    if (csumfield != 0 && fastcsum != csumfield && csum != csumfield)
    {
      return 0;
    }
    return 1;
  }
}


int find_page_size(FILE *f, ulint *page_size, int *compressed, int debug)
{
  uchar *p = (uchar*)malloc(PAGE_ZIP_MIN_SIZE); /* buffer to read data */
  ulint psize;
  int bytes;
  ulint flags;
  ulint zip_ssize;
  bytes= fread(p, 1, PAGE_ZIP_MIN_SIZE, f);
  rewind(f);
  if (bytes != PAGE_ZIP_MIN_SIZE) {
     fprintf(stderr,
             "Error in reading the first %d bytes of the ibd file.\n",
             PAGE_ZIP_MIN_SIZE);
     free(p);
     return 0;
  }

  flags = mach_read_from_4(p + FIL_PAGE_DATA + FSP_SPACE_FLAGS);
  zip_ssize = (flags & DICT_TF_ZSSIZE_MASK) >> DICT_TF_ZSSIZE_SHIFT;
  if (zip_ssize) { /* table is compressed */
    free(p);
    *compressed = 1;
    if (!*page_size)
      *page_size = ((ulint)PAGE_ZIP_MIN_SIZE >> 1) << zip_ssize;
    if (*page_size != ((ulint)PAGE_ZIP_MIN_SIZE >> 1) << zip_ssize) {
      fprintf(stderr, "Wrong page size is specified.\n"
                      "actual page size = %luK\n"
                      "specified page size = %luK\n"
                      "You can skip this option and innochecksum will "
                      "automatically determine the page size.\n",
                      (((ulint)PAGE_ZIP_MIN_SIZE >> 1) << zip_ssize) >> 10,
                      (*page_size) >> 10);
      return 0;
    }
    return 1;
  }

  *compressed = 0;
  if (*page_size) {
    free(p);
    return 1;
  }

  for (psize = 1024; psize < (1024 << 7); psize <<= 1) {
    if (debug)
      printf("checking if page_size is %luK\n", psize >> 10);
    p = (uchar*)realloc(p, psize);
    bytes= fread(p, 1, psize, f);
    rewind(f);

    if (bytes != (int)psize) {
       fprintf(stderr, "Error in reading the first %lu bytes of the ibd file."
                       "It may be that file is corrupt. You can also try "
                       "specifying page size (-b <size in kb>).\n", psize);
       free(p);
       return 0;
    }

    if (checksum_match(p, 0, psize, 0, debug)) {
      if (debug)
        printf("table has page size %lu and is uncompressed\n", psize);
      *page_size = psize;
      free(p);
      return 1;
    }
  }

  fprintf(stderr, "Page size can not be determined for the table\n");
  free(p);
  return 0;
}

int main(int argc, char **argv)
{
  FILE *f;                     /* our input file */
  uchar *p;                     /* storage of pages read */
  int bytes;                   /* bytes read count */
  ulint ct;                    /* current page number (0 based) */
  int now;                     /* current time */
  int lastt;                   /* last time */
  struct stat st;              /* for stat, if you couldn't guess */
  unsigned long long int size; /* size of file (has to be 64 bits) */
  ulint pages;                 /* number of pages in file */
  ulint start_page= 0, end_page= 0, use_end_page= 0; /* for starting and ending at certain pages */
  off_t offset= 0;
  int just_count= 0;          /* if true, just print page count */
  int verbose= 0;
  int per_page_details= 0;
  int debug= 0;
  int c;
  int fd;
  int skip_corrupt= 0;
  int compressed = 0;
  ulint page_size = 0;
  int retry = 0;
  int retry_delay_microsec = DEFAULT_RETRY_DELAY;
  int is_read_success = 1;

  /* remove arguments */
  while ((c= getopt(argc, argv, "r:cvids:e:p:ub:")) != -1)
  {
    switch (c)
    {
    case 'r':
      retry_delay_microsec = atoi(optarg) * 1000;
      break;
    case 'v':
      verbose= 1;
      break;
    case 'i':
      per_page_details= 1;
      break;
    case 'c':
      just_count= 1;
      break;
    case 's':
      start_page= atoi(optarg);
      break;
    case 'e':
      end_page= atoi(optarg);
      use_end_page= 1;
      break;
    case 'p':
      start_page= atoi(optarg);
      end_page= atoi(optarg);
      use_end_page= 1;
      break;
    case 'd':
      debug= 1;
      break;
    case 'u':
      skip_corrupt= 1;
      break;
    case 'b': /* b for block size */
      page_size = strtoul(optarg, NULL, 0);
      if (page_size & (page_size - 1)) {
        fprintf(stderr, "page_size (option -b) must be a power of 2\n");
        return 1;
      }
      if (page_size > 64) {
        fprintf(stderr, "page_size (option -b) can be at most 64 KB\n");
        return 1;
      }
      page_size <<= 10;
      break;
    case ':':
      fprintf(stderr, "option -%c requires an argument\n", optopt);
      return 1;
      break;
    case '?':
      fprintf(stderr, "unrecognized option: -%c\n", optopt);
      return 1;
      break;
    }
  }

  /* debug implies verbose... */
  if (debug) verbose= 1;

  /* make sure we have the right arguments */
  if (optind >= argc)
  {
    printf("InnoDB offline file checksum utility.\n");
    printf("usage: %s [-c] [-s <start page>] [-e <end page>] [-p <page>] [-r <retry delay>] [-v] [-d] [-i] <filename>\n", argv[0]);
    printf("\t-c\tprint the count of pages in the file\n");
    printf("\t-s n\tstart on this page number (0 based)\n");
    printf("\t-e n\tend at this page number (0 based)\n");
    printf("\t-p n\tcheck only this page (0 based)\n");
    printf("\t-v\tverbose (prints progress every 5 seconds)\n");
    printf("\t-d\tdebug mode (prints checksums for each page)\n");
    printf("\t-i\tprint per-page details\n");
    printf("\t-r n\tDelay (in millisec) between retries when there is a read error or checksum error. Default is 1000 millisec with 3 retries\n");
    return 1;
  }

  /* stat the file to get size and page count */
  if (stat(argv[optind], &st))
  {
    perror("error statting file");
    return 1;
  }

  /* open the file for reading */
  f= fopen(argv[optind], "r");
  if (!f)
  {
    perror("error opening file");
    return 1;
  }

  if (posix_fadvise(fileno(f), 0, 0, POSIX_FADV_SEQUENTIAL) ||
      posix_fadvise(fileno(f), 0, 0, POSIX_FADV_NOREUSE))
  {
    perror("posix_fadvise failed");
  }

  my_init_cpu_optimizations();

  if (!find_page_size(f, &page_size, &compressed, debug)) {
     fprintf(stderr, "error in determining the page size and/or"
                     " whether the table is in compressed format\n");
     return 1;
  }

  printf("Table is %s\n", (compressed ? "compressed" : "not compressed"));
  printf("%s size is %luK\n", (compressed ? "Key block" : "Page"), page_size >> 10);

  size= st.st_size;
  pages= size / page_size;
  if (just_count)
  {
    fclose(f);
    printf("%lu\n", pages);
    return 0;
  }
  else if (verbose)
  {
    printf("file %s = %llu bytes (%lu pages)...\n", argv[optind], size, pages);
    printf("checking pages in range %lu to %lu\n", start_page, use_end_page ? end_page : (pages - 1));
  }

  /* seek to the necessary position */
  if (start_page)
  {
    fd= fileno(f);
    if (!fd)
    {
      perror("unable to obtain file descriptor number");
      return 1;
    }

    offset= (off_t)start_page * (off_t)page_size;

    if (lseek(fd, offset, SEEK_SET) != offset)
    {
      perror("unable to seek to necessary offset");
      return 1;
    }
  }

  /* allocate buffer for reading (so we don't realloc every time) */
  p= (uchar *)malloc(page_size);

  /* main checksumming loop */
  ct= start_page;
  lastt= 0;
  while (!feof(f))
  {
    int page_ok = 1;
    is_read_success = 1;

    bytes= fread(p, 1, page_size, f);
    if (!bytes && feof(f))
    {
      print_stats();
      return 0;
    }

    if (bytes != (int)page_size)
    {
      if (retry < NUM_RETRIES) {
        retry++;
        is_read_success = 0;
      }
      else {
        fprintf(stderr, "bytes read (%d) doesn't match the page size (%lu)\n", bytes, page_size);
        print_stats();
        return 1;
      }
    }

    if (is_read_success && !checksum_match(p, ct, page_size, compressed, debug)) {
      if (retry < NUM_RETRIES) {
        retry++;
        is_read_success = 0;
      }
      else {
        fprintf(stderr, "page %lu invalid\n", ct);
        if (!skip_corrupt) return 1;
        page_ok = 0;
      }
    }
    if (!is_read_success) {
      fprintf(stderr, "Retrying in %dms for page %lu\n", retry_delay_microsec/1000, ct);
      fflush(f);
      fseek(f, ct * page_size, SEEK_SET);
      usleep(retry_delay_microsec);
      continue;
    }
    retry = 0;
    /* end if this was the last page we were supposed to check */
    if (use_end_page && (ct >= end_page))
    {
      print_stats();
      return 0;
    }

    if (per_page_details) {
      printf("page %ld ", ct);
    }

    ct++;

    if (!page_ok)
    {
      if (per_page_details) {
        printf("BAD_CHECKSUM\n");
      }
      n_bad_checksum++;
      continue;
    }

    parse_page(p, per_page_details);

    /* progress printing */
    if (verbose)
    {
      if (ct % 10000 == 0)
      {
        now= time(0);
        if (!lastt) lastt= now;
        if (now - lastt >= 1)
        {
          fprintf(stderr, "page %lu okay: %.3f%% done\n", (ct - 1), (float) ct / pages * 100);
          lastt= now;
        }
      }
    }
  }
  print_stats();

  return 0;
}
