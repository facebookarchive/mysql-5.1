/* Copyright (C) 2009-2010 Facebook, Inc.  All Rights Reserved.

   Dual licensed under BSD license and GPLv2.

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are met:
   1. Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimer.
   2. Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimer in the documentation
      and/or other materials provided with the distribution.
  
   THIS SOFTWARE IS PROVIDED BY FACEBOOK, INC. ``AS IS'' AND ANY EXPRESS OR
   IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
   MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO
   EVENT SHALL FACEBOOK, INC. BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
   PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
   OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
   WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
   OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
   ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

   This program is free software; you can redistribute it and/or modify it
   under the terms of the GNU General Public License as published by the Free
   Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful, but WITHOUT
   ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
   FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
   more details.

   You should have received a copy of the GNU General Public License along with
   this program; if not, write to the Free Software Foundation, Inc., 59 Temple
   Place, Suite 330, Boston, MA  02111-1307  USA */

/* Various performance statistics utilities.
 *
 *
 * Fast timers derive from the CPU time stamp counter provide a very low
 * overhead timer for performance metrics.  The values returned may be very
 * large (greater than the maximum integral value that can be stored accurately
 * in a double) so you should only convert elapsed times to doubles.
 *
 *
 * Table stats are used to track per-table operation and IO statistics.  The
 * data is stored in a global hash table of TABLE_STATS objects.  This hash
 * table is protected by a mutex.
 *
 * During a query, operation counters (rows inserted, updated, deleted, read,
 * and requested) are accumulated in the base handler class.  These values are
 * updated by the derived ha_innodb and ha_myisam classes.  When the table is
 * closed the values are added to the global TABLE_STATS object for the given
 * table using atomic operations.  The TABLE_STATS pointer is cached in the
 * handler to avoid redoing hash table lookups and locking the hash mutex.
 *
 * IO counters are currently only implemented for the innodb_plugin.  The trx_t
 * struct has an added table_io_perf member that contains counters for
 * synchronous IO operations and a pointer to the global TABLE_STATS used to
 * track asynchronous IO through a callback.  Before entering into innodb, we
 * check if the handler has a cached TABLE_STATS pointer.  If not it is
 * retrieved from the global hash table and cached for reuse.
 *
 * Within innodb the trx_t pointer is stored in the mtr_t struct to make the
 * table_io_perf available to lower level IO code (it may be preferable to
 * store a table_io_perf pointer directly instead of a trx_t pointer).
 *
 * The os_aio function updates the synchronous IO counters in the
 * table_io_perf.  When flow returns from innodb, these IO counters are
 * accumulated into the handler's IO statistics and will be added to the global
 * TABLE_STATS when the table is closed.
 *
 * Each access to a buffer pool page now updates a TABLE_STATS pointer stored
 * in the buffer page header.  When a dirty buffer pool page is flushed, the
 * TABLE_STATS pointer is stored in a stack-local table_io_perf object and
 * passed into the os_aio function.  The TABLE_STATS pointer is retrieved from
 * the stack-local table_io_perf object and stored in the async IO request
 * slot.  When the async IO request is completed, the global TABLE_STATS is
 * updated using atomic operations.  We assume that merged consecutive IO
 * belongs to the same table.
 */

#ifndef _my_perf_h
#define _my_perf_h

#include "my_global.h"

C_MODE_START

/** Compression statistics for a fil_space */
struct comp_stat_struct {
  /** Size of the compressed data on the page */
  int page_size;
  /** Current padding for compression */
  int padding;
  /** Number of times a page is prevented from failing to compress */
  ulonglong padding_savings;
  /** Number of page compressions */
  ulonglong compressed;
  /** Number of successful page compressions */
  ulonglong compressed_ok;
  /** Number of compressions in primary index */
  ulonglong compressed_primary;
  /** Number of successful compressions in primary index */
  ulonglong compressed_primary_ok;
  /** Number of page decompressions */
  ulonglong decompressed;
  /** Duration of page compressions in microseconds */
  ulonglong compressed_usec;
  /** Duration of succesful page compressions in microseconds */
  ulonglong compressed_ok_usec;
  /** Duration of page decompressions in microseconds */
  ulonglong decompressed_usec;
  /** Duration of primary index page compressions in microseconds */
  ulonglong compressed_primary_usec;
  /** Duration of successful primary index page compressions in microseconds */
  ulonglong compressed_primary_ok_usec;
};

/** Compression statistics */
typedef struct comp_stat_struct comp_stat_t;

/* Type used for low-overhead timers */
typedef ulonglong my_fast_timer_t;

/* Struct used for IO performance counters */
struct my_io_perf_struct {
  volatile my_atomic_bigint bytes;
  volatile my_atomic_bigint requests;
  volatile my_atomic_bigint svc_usecs; /*!< time to do read or write operation */
  volatile my_atomic_bigint svc_usecs_max;
  volatile my_atomic_bigint wait_usecs; /*!< total time in the request array */
  volatile my_atomic_bigint wait_usecs_max;
  volatile my_atomic_bigint slow_ios; /*!< requests that take too long */
};
typedef struct my_io_perf_struct my_io_perf_t;

/* struct used in per page type stats in IS.table_stats */
struct page_stats_struct {
  ulong n_pages_read; /*!< number read operations of all pages at given space*/
  ulong n_pages_read_index; /*!< number read operations of FIL_PAGE_INDEX pages at given space*/
  ulong n_pages_read_blob; /*!< number read operations FIL_PAGE_TYPE_BLOB and FIL_PAGE_TYPE_ZBLOB and FIL_PAGE_TYPE_ZBLOB2 pages at given space*/
  ulong n_pages_written;/*!< number write operations of all pages at given space*/
  ulong n_pages_written_index;/*!< number write operations of FIL_PAGE_INDEX pages at given space*/
  ulong n_pages_written_blob; /*!< number write operations FIL_PAGE_TYPE_BLOB and FIL_PAGE_TYPE_ZBLOB and FIL_PAGE_TYPE_ZBLOB2 pages at given space*/
};
typedef struct page_stats_struct page_stats_t;
/* Per-table operation and IO statistics */
struct st_table_stats;
struct st_table;
struct handlerton;

/***************************************************************************
Initialize an my_io_perf_t struct. */
void my_io_perf_init(my_io_perf_t* perf);

/* Accumulates io perf values */
void my_io_perf_sum(my_io_perf_t* sum, const my_io_perf_t* perf);

/* Returns a - b in diff */
void my_io_perf_diff(my_io_perf_t* diff,
                     const my_io_perf_t* a, const my_io_perf_t* b);

/* Accumulates io perf values using atomic operations */
void my_io_perf_sum_atomic(my_io_perf_t* sum, longlong bytes,
    longlong requests, longlong svc_usecs, longlong wait_usecs,
    longlong slow_ios);

/* Accumulates io perf values using atomic operations */
STATIC_INLINE void my_io_perf_sum_atomic_helper(my_io_perf_t* sum,
                                                const my_io_perf_t* perf)
{
  my_io_perf_sum_atomic(sum, perf->bytes, perf->requests, perf->svc_usecs,
      perf->wait_usecs, perf->slow_ios);
}


/* Fetches table stats for a given table */
struct st_table_stats* get_table_stats(struct st_table *table,
                                       struct handlerton *engine_type);

unsigned char get_db_stats_index(const char* db);
void update_global_db_stats_access(unsigned char db_stats_index,
                                   uint64 space,
                                   uint64 offset);

/* The inverse of the CPU frequency used to convert the time stamp counter
   to seconds. */
extern double my_tsc_scale;

/* True if fast timers are available. */
extern my_bool my_fast_timer_enabled;

/* Initialize the fast timer at startup Counts timer ticks for given seconds.
 */
extern void my_init_fast_timer(int seconds);

/* Reads the time stamp counter on an Intel processor */
STATIC_INLINE ulonglong rdtsc(void)
{
  ulonglong tsc;

#if defined(__GNUC__) && defined(__i386__)
  asm volatile ("rdtsc" : "=A" (tsc));
#elif defined(__GNUC__) && defined(__x86_64__)
  uint high, low;
  asm volatile ("rdtsc" : "=a"(low), "=d"(high));
  tsc = (((ulonglong)high)<<32) | low;
#else
  tsc = 0;
  assert(! "Aborted: rdtsc unimplemented for this configuration.");
#endif

  return tsc;
}

/* Returns a fast timer suitable for performance measurements. */
STATIC_INLINE void my_get_fast_timer(my_fast_timer_t* timer)
{
  if (likely(my_fast_timer_enabled))
  {
    *timer = rdtsc();
  }
  else
  {
    struct timeval tv;
    if (!gettimeofday(&tv, NULL))
      *timer = ((ulonglong)tv.tv_sec * 1000000) + tv.tv_usec;
    else
      *timer = 0;
  }
}

/* Converts the time represented by timer to seconds. */
STATIC_INLINE uint32 my_convert_to_seconds(my_fast_timer_t* timer)
{
  return (uint32) (my_tsc_scale * (*timer));
}

/* Returns the difference between stop and start in seconds. Returns 0
   when stop < start. */
STATIC_INLINE double my_fast_timer_diff(my_fast_timer_t const *start,
                                        my_fast_timer_t const *stop)
{
  ulonglong delta;

  if (*stop <= *start)
    return 0;

  delta = *stop - *start;

  return my_tsc_scale * delta;
}

/* Returns the difference between now and the time from 'in' in seconds.  Also
   optionally returns current fast timer in 'out'.  It is safe to pass the same
   struct for 'in' and 'out'. */
STATIC_INLINE double my_fast_timer_diff_now(my_fast_timer_t const *in,
                                            my_fast_timer_t *out)
{
  double diff;
  my_fast_timer_t now;
  my_get_fast_timer(&now);

  diff = my_fast_timer_diff(in, &now);

  if (out) {
    *out = now;
  }

  return diff;
}

/* Returns -1, 1, or 0 if *x is less than, greater than, or equal to *y */
STATIC_INLINE int my_fast_timer_cmp(my_fast_timer_t const *x,
                                    my_fast_timer_t const *y)
{
  if (*x < *y)
    return -1;
  if (*x > *y)
    return 1;
  else
    return 0;
}

/* Sets a fast timer to an invalid value */
STATIC_INLINE void my_fast_timer_invalidate(my_fast_timer_t *timer)
{
  *timer = 0;
}

/* Sets a fast timer to the value of another */
STATIC_INLINE void my_fast_timer_set(my_fast_timer_t *dest,
                                     my_fast_timer_t const *src)
{
  *dest = *src;
}

/* Returns true if the timer is valid */
STATIC_INLINE my_bool my_fast_timer_is_valid(my_fast_timer_t const *timer)
{
  return (*timer != 0);
}

/* Return the fast timer scale factor.  If this value is 0 fast timers are
   not initialized. */
STATIC_INLINE double my_fast_timer_get_scale()
{
  return my_tsc_scale;
}

/* Enable hardware-optimized functions based on CPUID */
void my_init_cpu_optimizations();


/* Fast crc32 implementation using SSE4.2 if available and slice-8 if not.
   This is not compatible with the zlib crc32 implementation because the
   SSE4.2 routine uses a different polynomial */
uint32 my_fast_crc32(const uchar* data, ulong length);

#define SUPPORT_BROKEN_CRC32_SLICE8

#ifdef SUPPORT_BROKEN_CRC32_SLICE8
/* Implementation of broken crc32 used to support old format files
   until migration to fixed crc32 is completed. */
uint32 my_fast_crc32_broken_slice8(const uchar* data, ulong length);
#endif

/* Implementatio of a Substitution Box (S-Box) hash using 256 values
   Ideal for use in generating uniform hashes (CRC32 is very unsuitable
	 for use as a uniform hash) */
uint32 my_sbox_hash(const uchar* data, ulong length);

C_MODE_END

#endif /* _my_perf_h */
