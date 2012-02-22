/*****************************************************************************

Copyright (c) 1995, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2008, 2009, Google Inc.
Copyright (c) 2009, Percona Inc.

Portions of this file contain modifications contributed and copyrighted by
Google, Inc. Those modifications are gratefully acknowledged and are described
briefly in the InnoDB documentation. The contributions by Google are
incorporated with their permission, and subject to the conditions contained in
the file COPYING.Google.

Portions of this file contain modifications contributed and copyrighted
by Percona Inc.. Those modifications are
gratefully acknowledged and are described briefly in the InnoDB
documentation. The contributions by Percona Inc. are incorporated with
their permission, and subject to the conditions contained in the file
COPYING.Percona.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 59 Temple
Place, Suite 330, Boston, MA 02111-1307 USA

*****************************************************************************/

/**************************************************//**
@file include/srv0srv.h
The server main program

Created 10/10/1995 Heikki Tuuri
*******************************************************/

#ifndef srv0srv_h
#define srv0srv_h

#include "univ.i"
#ifndef UNIV_HOTBACKUP
#include "sync0sync.h"
#include "os0sync.h"
#include "que0types.h"
#include "trx0types.h"

extern const char*	srv_main_thread_op_info;

/** Prefix used by MySQL to indicate pre-5.1 table name encoding */
extern const char	srv_mysql50_table_name_prefix[9];

/* When this event is set the lock timeout and InnoDB monitor
thread starts running */
extern os_event_t	srv_lock_timeout_thread_event;

/* If the last data file is auto-extended, we add this many pages to it
at a time */
#define SRV_AUTO_EXTEND_INCREMENT	\
	(srv_auto_extend_increment * ((1024 * 1024) / UNIV_PAGE_SIZE))

/* This is set to TRUE if the MySQL user has set it in MySQL */
extern ibool	srv_lower_case_table_names;

/* Mutex for locking srv_monitor_file */
extern mutex_t	srv_monitor_file_mutex;
/* Temporary file for innodb monitor output */
extern FILE*	srv_monitor_file;
/* Mutex for locking srv_dict_tmpfile.
This mutex has a very high rank; threads reserving it should not
be holding any InnoDB latches. */
extern mutex_t	srv_dict_tmpfile_mutex;
/* Temporary file for output from the data dictionary */
extern FILE*	srv_dict_tmpfile;
/* Mutex for locking srv_misc_tmpfile.
This mutex has a very low rank; threads reserving it should not
acquire any further latches or sleep before releasing this one. */
extern mutex_t	srv_misc_tmpfile_mutex;
/* Temporary file for miscellanous diagnostic output */
extern FILE*	srv_misc_tmpfile;

/* Server parameters which are read from the initfile */

extern char*	srv_data_home;
#ifdef UNIV_LOG_ARCHIVE
extern char*	srv_arch_dir;
#endif /* UNIV_LOG_ARCHIVE */

/** store to its own file each table created by an user; data
dictionary tables are in the system tablespace 0 */
#ifndef UNIV_HOTBACKUP
extern my_bool	srv_file_per_table;
#else
extern ibool	srv_file_per_table;
#endif /* UNIV_HOTBACKUP */

/** Do DROP TABLE processing in the background to reduce time for
which LOCK_open is locked */
extern my_bool	srv_background_drop_table;

/** If true, always log the images of compressed pages when the page is
recompressed */
extern my_bool srv_log_compressed_pages;
extern uint srv_simulate_comp_failures;

/** Number of page size samples collected from pages that fail to compress to
determine the ideal page size that won't fail to compress. */
extern uint srv_comp_fail_samples;
/** Size of the red black tree for computing the average page size
for pages that fail to compress. */
extern uint srv_comp_fail_tree_size;
/** If the compression failure rate of a table is greater than this number
 then the padding will continue to increase even after srv_comp_fail_samples */
extern double srv_comp_fail_max_fail_rate;

/** The file format to use on new *.ibd files. */
extern ulint	srv_file_format;
/** Whether to check file format during startup.  A value of
DICT_TF_FORMAT_MAX + 1 means no checking ie. FALSE.  The default is to
set it to the highest format we support. */
extern ulint	srv_check_file_format_at_startup;
/** Place locks to records only i.e. do not use next-key locking except
on duplicate key checking and foreign key checking */
extern ibool	srv_locks_unsafe_for_binlog;
#endif /* !UNIV_HOTBACKUP */

extern ulint	srv_n_data_files;
extern char**	srv_data_file_names;
extern ulint*	srv_data_file_sizes;
extern ulint*	srv_data_file_is_raw_partition;

extern ulint	srv_use_purge_thread;

extern my_bool     srv_drop_table_phase1;
extern double      srv_drop_table_phase1_secs;
extern double      srv_drop_table_phase2_secs;

extern my_bool     srv_fake_changes_locks;

extern ulint	srv_read_wait_usecs;

extern ulint	srv_uncache_table_batch;

extern my_bool	srv_b62037;

extern ibool	srv_auto_extend_last_data_file;
extern ulint	srv_last_file_size_max;
extern char**	srv_log_group_home_dirs;
#ifndef UNIV_HOTBACKUP
extern ulong	srv_auto_extend_increment;

extern ibool	srv_created_new_raw;

/* Detect deadlocks on lock wait */
extern my_bool	srv_deadlock_detect;

extern ulint	srv_n_log_groups;
extern ulint	srv_n_log_files;
extern ulint	srv_log_file_size;
extern ulint	srv_log_buffer_size;
extern ulong	srv_flush_log_at_trx_commit;
extern char	srv_adaptive_flushing;


/* The sort order table of the MySQL latin1_swedish_ci character set
collation */
extern const byte*	srv_latin1_ordering;
#ifndef UNIV_HOTBACKUP
extern my_bool	srv_use_sys_malloc;
#else
extern ibool	srv_use_sys_malloc;
#endif /* UNIV_HOTBACKUP */
extern ulint	srv_buf_pool_size;	/*!< requested size in bytes */
extern ulint	srv_buf_pool_old_size;	/*!< previously requested size */
extern ulint	srv_buf_pool_curr_size;	/*!< current size in bytes */
extern ulint	srv_mem_pool_size;
extern ulint	srv_lock_table_size;

extern ulint	srv_n_file_io_threads;
extern ulong	srv_read_ahead_threshold;
extern ulint	srv_n_read_io_threads;
extern ulint	srv_n_write_io_threads;

/* Number of IO operations per second the server can do */
extern ulong    srv_io_capacity;
/* Returns the number of IO operations that is X percent of the
capacity. PCT_IO(5) -> returns the number of IO operations that
is 5% of the max where max is srv_io_capacity.  */
#define PCT_IO(p) ((ulong) (srv_io_capacity * ((double) p / 100.0)))

extern long	srv_ibuf_max_pct_of_buffer_pool;
extern long	srv_ibuf_max_pct_of_io_capacity;
extern long	srv_ibuf_max_iops_when_below_limit;

#ifdef UNIV_LOG_ARCHIVE
extern ibool	srv_log_archive_on;
extern ibool	srv_archive_recovery;
extern dulint	srv_archive_recovery_limit_lsn;
#endif /* UNIV_LOG_ARCHIVE */

extern char*	srv_file_flush_method_str;
extern ulint	srv_unix_file_flush_method;
extern ulint	srv_win_file_flush_method;

extern ulint	srv_max_n_open_files;

extern ulint	srv_max_dirty_pages_pct;

extern ulint	srv_force_recovery;
extern ulong	srv_thread_concurrency;
extern my_bool	srv_thread_lifo;

extern ulint	srv_max_n_threads;

extern lint	srv_conc_n_threads;

extern ulint	srv_fast_shutdown;	 /* If this is 1, do not do a
					 purge and index buffer merge.
					 If this 2, do not even flush the
					 buffer pool to data files at the
					 shutdown: we effectively 'crash'
					 InnoDB (but lose no committed
					 transactions). */
extern ibool	srv_innodb_status;

extern unsigned long long	srv_stats_sample_pages;

extern ibool	srv_use_doublewrite_buf;
extern ibool	srv_use_checksums;
extern my_bool	srv_use_fast_checksums;

extern my_bool	srv_extra_checksums;
extern ulong	srv_unzip_LRU_pct;
extern ulong	srv_lru_io_to_unzip_factor;

extern ibool	srv_set_thread_priorities;
extern int	srv_query_thread_priority;

extern double	srv_max_buf_pool_modified_pct;
extern ulong	srv_max_purge_lag;

extern ulong	srv_replication_delay;
/*-------------------------------------------*/

extern ulint	srv_n_rows_inserted;
extern ulint	srv_n_rows_updated;
extern ulint	srv_n_rows_deleted;
extern ulint	srv_n_rows_read;

extern ibool	srv_print_innodb_monitor;
extern ibool	srv_print_innodb_lock_monitor;
extern ibool	srv_print_innodb_tablespace_monitor;
extern ibool	srv_print_verbose_log;
extern ibool	srv_print_innodb_table_monitor;

extern ibool	srv_lock_timeout_active;
extern ibool	srv_monitor_active;
extern ibool	srv_error_monitor_active;

extern ulong	srv_n_spin_wait_rounds;
extern ulong	srv_n_free_tickets_to_enter;
extern ulong	srv_thread_sleep_delay;
extern ulong	srv_spin_wait_delay;
extern ibool	srv_priority_boost;

extern	ulint	srv_mem_pool_size;
extern	ulint	srv_lock_table_size;

#ifdef UNIV_DEBUG
extern	ibool	srv_print_thread_releases;
extern	ibool	srv_print_lock_waits;
extern	ibool	srv_print_buf_io;
extern	ibool	srv_print_log_io;
extern	ibool	srv_print_latch_waits;
#else /* UNIV_DEBUG */
# define srv_print_thread_releases	FALSE
# define srv_print_lock_waits		FALSE
# define srv_print_buf_io		FALSE
# define srv_print_log_io		FALSE
# define srv_print_latch_waits		FALSE
#endif /* UNIV_DEBUG */

extern ulint	srv_activity_count;
extern ulint	srv_fatal_semaphore_wait_threshold;
extern ulint	srv_dml_needed_delay;

/** Pages flushed from foreground thread to maintain non-dirty pages on free list */
extern ulint	srv_n_flushed_free_margin_fg;

/** Pages flushed from background thread to maintain non-dirty pages on free list */
extern ulint	srv_n_flushed_free_margin_bg;

/** Pages flushed for adaptive flushing */
extern ulint	srv_n_flushed_adaptive;

/** Pages flushed to enforce innodb_max_dirty_pages_pct */
extern ulint	srv_n_flushed_max_dirty;

/** Pages flushed at the start of a DML statement to maintain clean
pages in the buffer pool */
extern ulint	srv_n_flushed_preflush;

/** Pages flushed by background checkpoint */
extern ulint	srv_n_flushed_background_checkpoint;

/** Pages flushed by foreground checkpoint */
extern ulint	srv_n_flushed_foreground_checkpoint;

/** Pages flushed for other reasons */
extern ulint	srv_n_flushed_other;

/** Number of extra writes done in buf_flush_try_neighbors from LRU list */
extern ulint srv_neighbors_flushed_lru;

/** Number of extra writes done in buf_flush_try_neighbors from flush list */
extern ulint srv_neighbors_flushed_list;

extern my_bool	srv_read_ahead_linear;

/** The loop in srv_master_thread should run once per this number of usecs.
If work finishes faster, it will sleep. */
extern long	srv_background_thread_interval_usecs;

/** Seconds doing a checkpoint */
extern double	srv_checkpoint_secs;

/** Seconds in buf_flush_free_margin from a foreground thread */
extern double	srv_free_margin_fg_secs;

/** Seconds in buf_flush_free_margin from a background thread */
extern double	srv_free_margin_bg_secs;

/** Seconds in insert buffer */
extern double	srv_ibuf_contract_secs;

/** Seconds in buf_flush_batch */
extern double	srv_buf_flush_secs;

/** Seconds in trx_purge */
extern double	srv_purge_secs;

/** Seconds in log_checkpoint_margin_background */
extern double	srv_background_checkpoint_secs;

/** Seconds in log_checkpoint_margin */
extern double	srv_foreground_checkpoint_secs;

/** Seconds that srv_master_thread sleeps */
extern double	srv_main_sleep_secs;

/** Number of deadlocks */
extern ulint	srv_lock_deadlocks;

/** Number of lock wait timeouts */
extern ulint	srv_lock_wait_timeouts;

/** Main background thread does flushes for fuzzy checkpoint */
extern my_bool	srv_background_checkpoint;

extern mutex_t*	kernel_mutex_temp;/* mutex protecting the server, trx structs,
				query threads, and lock table: we allocate
				it from dynamic memory to get it to the
				same DRAM page as other hotspot semaphores */
#define kernel_mutex (*kernel_mutex_temp)

#define SRV_MAX_N_IO_THREADS	130

/* Array of English strings describing the current state of an
i/o handler thread */
extern const char* srv_io_thread_op_info[];
extern const char* srv_io_thread_function[];

/* the number of the log write requests done */
extern ulint srv_log_write_requests;

/* the number of physical writes to the log performed */
extern ulint srv_log_writes;

/* amount of data written to the log files in bytes */
extern ulint srv_os_log_written;

/* amount of writes being done to the log files */
extern ulint srv_os_log_pending_writes;

/* we increase this counter, when there we don't have enough space in the
log buffer and have to flush it */
extern ulint srv_log_waits;

/* variable that counts amount of data read in total (in bytes) */
extern ulint srv_data_read;

/* here we count the amount of data written in total (in bytes) */
extern ulint srv_data_written;

/* this variable counts the amount of times, when the doublewrite buffer
was flushed */
extern ulint srv_dblwr_writes;

/* here we store the number of pages that have been flushed to the
doublewrite buffer */
extern ulint srv_dblwr_pages_written;

/* in this variable we store the number of write requests issued */
extern ulint srv_buf_pool_write_requests;

/* here we store the number of times when we had to wait for a free page
in the buffer pool. It happens when the buffer pool is full and we need
to make a flush, in order to be able to read or create a page. */
extern ulint srv_buf_pool_wait_free;

/* variable to count the number of pages that were written from the
buffer pool to disk */
extern ulint srv_buf_pool_flushed;

/** Number of buffer pool reads that led to the
reading of a disk page */
extern ulint srv_buf_pool_reads;

/** Time in seconds between automatic buffer pool dumps */
extern uint srv_auto_lru_dump;

/** Maximum number of LRU entries to restore
 * Consecutive pages are merged and only count as one, so you will probably
 * load more pages than this number of LRU entries. */
extern ulint srv_lru_load_max_entries;

/** If enabled, will also dump old pages from the LRU */
extern my_bool srv_lru_dump_old_pages;

/** Number of buffer pool pages already restored */
extern ulint srv_lru_restore_loaded_pages;

/** Number of buffer pool pages in restore list */
extern ulint srv_lru_restore_total_pages;

/* When TRUE, flush dirty neighbor pages on checkpoint. */
extern my_bool srv_flush_neighbors_on_checkpoint;

/* When TRUE, flush dirty neighbor pages on LRU list flush (BUF_FLUSH_LRU). */
extern my_bool srv_flush_neighbors_for_LRU;

/* When TRUE, use buf_flush_free_margin_fast instead of buf_flush_free_margin */
extern my_bool srv_fast_free_list;

/* Option to retry reads and writes one time when they fail with EIO */
extern my_bool srv_retry_io_on_error;

/* Number of reads retried with srv_retry_io_on_error */
extern ulint srv_data_retried_reads;

/* Number of writes retried with srv_retry_io_on_error */
extern ulint srv_data_retried_writes;

/* When TRUE allow the adaptive hash rw-lock to be cached across calls
   to InnoDB functions. TRUE is the original behavior. */
extern my_bool srv_adaptive_hash_latch_cache;

/** Number of commits */
extern ulint srv_n_commit_all;

/** Number of commits for which undo was generated */
extern ulint srv_n_commit_with_undo;

/** Number of full rollbacks */
extern ulint srv_n_rollback_total;

/** Number of partial rollbacks */
extern ulint srv_n_rollback_partial;

/** Number of times secondary index block visibility check returned TRUE */
extern ulint srv_sec_rec_read_sees;

/** Number of times secondary index block visibility check was done */
extern ulint srv_sec_rec_read_check;

/** Status variables to be passed to MySQL */
typedef struct export_var_struct export_struc;

/** Status variables to be passed to MySQL */
extern export_struc export_vars;

/** The server system */
typedef struct srv_sys_struct	srv_sys_t;

/** The server system */
extern srv_sys_t*	srv_sys;
#endif /* !UNIV_HOTBACKUP */

/** Should prepare_commit_mutex be aquired before writing to binlog? */
extern my_bool innobase_prepare_commit_mutex;

/** Release locks in prepare step */
extern my_bool innobase_release_locks_early;

/** See the sync_checkpoint_limit user variable declaration in ha_innodb.cc */
extern ulong srv_sync_checkpoint_limit;

/** Number of pages processed by trx_purge */
extern ulint srv_purged_pages;

extern ulint srv_expand_import;


/** Types of raw partitions in innodb_data_file_path */
enum {
	SRV_NOT_RAW = 0,	/*!< Not a raw partition */
	SRV_NEW_RAW,		/*!< A 'newraw' partition, only to be
				initialized */
	SRV_OLD_RAW		/*!< An initialized raw partition */
};

/** Alternatives for the file flush option in Unix; see the InnoDB manual
about what these mean */
enum {
	SRV_UNIX_FSYNC = 1,	/*!< fsync, the default */
	SRV_UNIX_O_DSYNC,	/*!< open log files in O_SYNC mode */
	SRV_UNIX_LITTLESYNC,	/*!< do not call os_file_flush()
				when writing data files, but do flush
				after writing to log files */
	SRV_UNIX_NOSYNC,	/*!< do not flush after writing */
	SRV_UNIX_O_DIRECT,	/*!< invoke os_file_set_nocache() on
				data files */
	SRV_UNIX_ALL_O_DIRECT	/*!< O_DIRECT for data and log files */
};

/** Alternatives for file i/o in Windows */
enum {
	SRV_WIN_IO_NORMAL = 1,	/*!< buffered I/O */
	SRV_WIN_IO_UNBUFFERED	/*!< unbuffered I/O; this is the default */
};

/** Alternatives for srv_force_recovery. Non-zero values are intended
to help the user get a damaged database up so that he can dump intact
tables and rows with SELECT INTO OUTFILE. The database must not otherwise
be used with these options! A bigger number below means that all precautions
of lower numbers are included. */
enum {
	SRV_FORCE_IGNORE_CORRUPT = 1,	/*!< let the server run even if it
					detects a corrupt page */
	SRV_FORCE_NO_BACKGROUND	= 2,	/*!< prevent the main thread from
					running: if a crash would occur
					in purge, this prevents it */
	SRV_FORCE_NO_TRX_UNDO = 3,	/*!< do not run trx rollback after
					recovery */
	SRV_FORCE_NO_IBUF_MERGE = 4,	/*!< prevent also ibuf operations:
					if they would cause a crash, better
					not do them */
	SRV_FORCE_NO_UNDO_LOG_SCAN = 5,	/*!< do not look at undo logs when
					starting the database: InnoDB will
					treat even incomplete transactions
					as committed */
	SRV_FORCE_NO_LOG_REDO = 6	/*!< do not do the log roll-forward
					in connection with recovery */
};

#ifndef UNIV_HOTBACKUP
/** Types of threads existing in the system. */
enum srv_thread_type {
	SRV_COM = 1,	/**< threads serving communication and queries */
	SRV_CONSOLE,	/**< thread serving console */
	SRV_WORKER,	/**< threads serving parallelized queries and
			queries released from lock wait */
#if 0
	/* Utility threads */
	SRV_BUFFER,	/**< thread flushing dirty buffer blocks */
	SRV_RECOVERY,	/**< threads finishing a recovery */
	SRV_INSERT,	/**< thread flushing the insert buffer to disk */
#endif
	SRV_PURGE,	/* thread purging undo records */
	SRV_PURGE_WORKER,	/* thread purging undo records */
	SRV_MASTER	/**< the master thread, (whose type number must
			be biggest) */
};

/*********************************************************************//**
Boots Innobase server.
@return	DB_SUCCESS or error code */
UNIV_INTERN
ulint
srv_boot(void);
/*==========*/
/*********************************************************************//**
Initializes the server. */
UNIV_INTERN
void
srv_init(void);
/*==========*/
/*********************************************************************//**
Frees the data structures created in srv_init(). */
UNIV_INTERN
void
srv_free(void);
/*==========*/
/*********************************************************************//**
Initializes the synchronization primitives, memory system, and the thread
local storage. */
UNIV_INTERN
void
srv_general_init(void);
/*==================*/
/*********************************************************************//**
Gets the number of threads in the system.
@return	sum of srv_n_threads[] */
UNIV_INTERN
ulint
srv_get_n_threads(void);
/*===================*/
/*********************************************************************//**
Returns the calling thread type.
@return	SRV_COM, ... */

enum srv_thread_type
srv_get_thread_type(void);
/*=====================*/
/*********************************************************************//**
Sets the info describing an i/o thread current state. */
UNIV_INTERN
void
srv_set_io_thread_op_info(
/*======================*/
	ulint		i,	/*!< in: the 'segment' of the i/o thread */
	const char*	str);	/*!< in: constant char string describing the
				state */
/*********************************************************************//**
Releases threads of the type given from suspension in the thread table.
NOTE! The server mutex has to be reserved by the caller!
@return number of threads released: this may be less than n if not
enough threads were suspended at the moment */
UNIV_INTERN
ulint
srv_release_threads(
/*================*/
	enum srv_thread_type	type,	/*!< in: thread type */
	ulint			n);	/*!< in: number of threads to release */
/*********************************************************************//**
The master thread controlling the server.
@return	a dummy parameter */
UNIV_INTERN
os_thread_ret_t
srv_master_thread(
/*==============*/
	void*	arg);	/*!< in: a dummy parameter required by
			os_thread_create */
/*************************************************************************
The undo purge thread. */
UNIV_INTERN
os_thread_ret_t
srv_purge_thread(
/*=============*/
	void*	arg);	/* in: a dummy parameter required by
			os_thread_create */
/*************************************************************************
The undo purge thread. */
UNIV_INTERN
os_thread_ret_t
srv_purge_worker_thread(
/*====================*/
	void*	arg);
/*******************************************************************//**
Tells the Innobase server that there has been activity in the database
and wakes up the master thread if it is suspended (not sleeping). Used
in the MySQL interface. Note that there is a small chance that the master
thread stays suspended (we do not protect our operation with the kernel
mutex, for performace reasons). */
UNIV_INTERN
void
srv_active_wake_master_thread(void);
/*===============================*/
/*******************************************************************//**
Wakes up the master thread if it is suspended or being suspended. */
UNIV_INTERN
void
srv_wake_master_thread(void);
/*========================*/
/*********************************************************************//**
Puts an OS thread to wait if there are too many concurrent threads
(>= srv_thread_concurrency) inside InnoDB. The threads wait in a FIFO queue. */
UNIV_INTERN
void
srv_conc_enter_innodb(
/*==================*/
	trx_t*	trx);	/*!< in: transaction object associated with the
			thread */
/*********************************************************************//**
This lets a thread enter InnoDB regardless of the number of threads inside
InnoDB. This must be called when a thread ends a lock wait. */
UNIV_INTERN
void
srv_conc_force_enter_innodb(
/*========================*/
	trx_t*	trx);	/*!< in: transaction object associated with the
			thread */
/*********************************************************************//**
This must be called when a thread exits InnoDB in a lock wait or at the
end of an SQL statement. */
UNIV_INTERN
void
srv_conc_force_exit_innodb(
/*=======================*/
	trx_t*	trx);	/*!< in: transaction object associated with the
			thread */
/*********************************************************************//**
This must be called when a thread exits InnoDB. */
UNIV_INTERN
void
srv_conc_exit_innodb(
/*=================*/
	trx_t*	trx);	/*!< in: transaction object associated with the
			thread */
/***************************************************************//**
Puts a MySQL OS thread to wait for a lock to be released. If an error
occurs during the wait trx->error_state associated with thr is
!= DB_SUCCESS when we return. DB_LOCK_WAIT_TIMEOUT and DB_DEADLOCK
are possible errors. DB_DEADLOCK is returned if selective deadlock
resolution chose this transaction as a victim. */
UNIV_INTERN
void
srv_suspend_mysql_thread(
/*=====================*/
	que_thr_t*	thr);	/*!< in: query thread associated with the MySQL
				OS thread */
/********************************************************************//**
Releases a MySQL OS thread waiting for a lock to be released, if the
thread is already suspended. */
UNIV_INTERN
void
srv_release_mysql_thread_if_suspended(
/*==================================*/
	que_thr_t*	thr);	/*!< in: query thread associated with the
				MySQL OS thread	 */
/*********************************************************************//**
A thread which wakes up threads whose lock wait may have lasted too long.
@return	a dummy parameter */
UNIV_INTERN
os_thread_ret_t
srv_lock_timeout_thread(
/*====================*/
	void*	arg);	/*!< in: a dummy parameter required by
			os_thread_create */
/*********************************************************************//**
A thread which prints the info output by various InnoDB monitors.
@return	a dummy parameter */
UNIV_INTERN
os_thread_ret_t
srv_monitor_thread(
/*===============*/
	void*	arg);	/*!< in: a dummy parameter required by
			os_thread_create */
/*************************************************************************
A thread which prints warnings about semaphore waits which have lasted
too long. These can be used to track bugs which cause hangs.
@return	a dummy parameter */
UNIV_INTERN
os_thread_ret_t
srv_error_monitor_thread(
/*=====================*/
	void*	arg);	/*!< in: a dummy parameter required by
			os_thread_create */
/*********************************************************************//**
A thread which restores the buffer pool from a dump file on startup and does
periodic buffer pool dumps.
@return	a dummy parameter */
UNIV_INTERN
os_thread_ret_t
srv_LRU_dump_restore_thread(
/*====================*/
	void*	arg);	/*!< in: a dummy parameter required by
			os_thread_create */
/******************************************************************//**
Outputs to a file the output of the InnoDB Monitor.
@return FALSE if not all information printed
due to failure to obtain necessary mutex */
UNIV_INTERN
ibool
srv_printf_innodb_monitor(
/*======================*/
	FILE*	file,		/*!< in: output stream */
	ibool	nowait,		/*!< in: whether to wait for kernel mutex */
	ibool	include_trxs);	/*!< in: include per-transaction output */

/**********************************************************************
Output for SHOW INNODB TRANSACTION STATUS */

void
srv_printf_innodb_transaction(
/*======================*/
	FILE*	file);		/* in: output stream */

/******************************************************************//**
Function to pass InnoDB status variables to MySQL */
UNIV_INTERN
void
srv_export_innodb_status(void);
/*==========================*/

/** Thread slot in the thread table */
typedef struct srv_slot_struct	srv_slot_t;

/** Thread table is an array of slots */
typedef srv_slot_t	srv_table_t;

/** Status variables to be passed to MySQL */
struct export_var_struct{
	ib_int64_t innodb_checkpoint_lsn;	/*!< last_checkpoint_lsn */
	ib_int64_t innodb_checkpoint_diff;	/*!< lsn - last_checkpoint_lsn */
	ulint innodb_data_pending_reads;	/*!< Pending reads */
	ulint innodb_data_pending_writes;	/*!< Pending writes */
	ulint innodb_data_pending_fsyncs;	/*!< Pending fsyncs */
	ulint innodb_data_fsyncs;		/*!< Number of fsyncs so far */
	double innodb_data_fsync_secs;		/*!< Seconds performing fsync */
	ulint innodb_data_read;			/*!< Data bytes read */
	ulint innodb_data_writes;		/*!< I/O write requests */
	ulint innodb_data_written;		/*!< Data bytes written */
	ulint innodb_data_reads;		/*!< I/O read requests */
	ulint innodb_data_retried_reads;	/*!< I/O reads retried on EIO */
	ulint innodb_data_retried_writes;	/*!< I/O writes retried on EIO */
	ulint innodb_data_async_read_bytes;	/*!< #bytes for async reads */
	ulint innodb_data_async_read_requests;	/*!< #requests for async reads */
	double innodb_data_async_read_svc_secs;	/*!< service time for async reads */
	ulint innodb_data_sync_read_bytes;	/*!< #bytes for sync reads */
	ulint innodb_data_sync_read_requests;	/*!< #requests for sync reads */
	double innodb_data_sync_read_svc_secs;	/*!< service time for sync reads */
	ulint innodb_data_async_write_bytes;	/*!< #bytes for async writes */
	ulint innodb_data_async_write_requests;	/*!< #requests for async writes */
	double innodb_data_async_write_svc_secs;/*!< service time for async writes */
	ulint innodb_data_sync_write_bytes;	/*!< #bytes for sync writes */
	ulint innodb_data_sync_write_requests;	/*!< #requests for sync writes */
	double innodb_data_sync_write_svc_secs;	/*!< service time for sync writes */
	ulint innodb_data_log_write_bytes;	/*!< #bytes for log writes */
	ulint innodb_data_log_write_requests;	/*!< #requests for log writes */
	double innodb_data_log_write_svc_secs;	/*!< service time for log writes */
	ulint innodb_data_double_write_bytes;	/*!< #bytes for double writes */
	ulint innodb_data_double_write_requests;/*!< #requests for double writes */
	double innodb_data_double_write_svc_secs;/*!< service time for double writes */
	ulint background_drop_table_queue;	/*!< #tables in background drop queue */
	ulint innodb_buffer_pool_flushed_lru;	/*!< #pages flushed from LRU */
	ulint innodb_buffer_pool_flushed_list;	/*!< #pages flushed from flush list */
	ulint innodb_buffer_pool_flushed_page;	/*!< #pages flushed from other */
	ulint innodb_buffer_pool_pages_total;	/*!< Buffer pool size */
	ulint innodb_buffer_pool_pages_data;	/*!< Data pages */
	ulint innodb_buffer_pool_pages_dirty;	/*!< Dirty data pages */
	ulint innodb_buffer_pool_pages_unzip;	/*!< #pages on buf_pool->unzip_LRU */
	ulint innodb_buffer_pool_pages_misc;	/*!< Miscellanous pages */
	ulint innodb_buffer_pool_pages_free;	/*!< Free pages */
#ifdef UNIV_DEBUG
	ulint innodb_buffer_pool_pages_latched;	/*!< Latched pages */
#endif /* UNIV_DEBUG */
	ulint innodb_buffer_pool_pages_lru_old;	/*!< Number of old pages in LRU */
	ulint innodb_buffer_pool_pct_dirty;	/*!< Percent of pages dirty */
	ulint innodb_buffer_pool_read_requests;	/*!< buf_pool->stat.n_page_gets */
	ulint innodb_buffer_pool_reads;		/*!< srv_buf_pool_reads */
	ulint innodb_buffer_pool_wait_free;	/*!< srv_buf_pool_wait_free */
	ulint innodb_buffer_pool_pages_flushed;	/*!< srv_buf_pool_flushed */
	ulint innodb_buffer_pool_write_requests;/*!< srv_buf_pool_write_requests */
	ulint innodb_buffer_pool_read_ahead;	/*!< srv_read_ahead */
	ulint innodb_buffer_pool_read_ahead_evicted;/*!< srv_read_ahead evicted*/
	ulint innodb_buffer_pool_flushed_adaptive;/*!< srv_n_flushed_adaptive */
	ulint innodb_buffer_pool_flushed_free_margin_fg;/*!< srv_n_flushed_free_margin_fg */
	ulint innodb_buffer_pool_flushed_free_margin_bg;/*!< srv_n_flushed_free_margin_bg */
	ulint innodb_buffer_pool_flushed_max_dirty;/*!< srv_n_flushed_max_dirty */
	ulint innodb_buffer_pool_flushed_other;/*!< srv_n_flushed_other */
	ulint innodb_buffer_pool_flushed_preflush;/*!< srv_n_flushed_preflush */
	ulint innodb_buffer_pool_flushed_background_checkpoint;
						/*!< srv_n_flushed_background_checkpoint */
	ulint innodb_buffer_pool_flushed_foreground_checkpoint;
						/*!< srv_n_flushed_foreground_checkpoint */
        ulint innodb_buffer_pool_neighbors_flushed_list;/*!< srv_neighbors_flushed_list */
        ulint innodb_buffer_pool_neighbors_flushed_lru;/*!< srv_neighbors_flushed_lru */
	ulint innodb_dblwr_pages_written;	/*!< srv_dblwr_pages_written */
	ulint innodb_dblwr_writes;		/*!< srv_dblwr_writes */
	ulint innodb_hash_nonsearches;		/*!< btr_cur_n_sea */
	ulint innodb_hash_searches;		/*!< btr_cur_n_non_sea */
	ulint innodb_hash_pages_added;		/*!< btr_search_n_pages_added */
	ulint innodb_hash_pages_removed;	/*!< btr_search_n_pages_removed */
	ulint innodb_hash_rows_added;		/*!< btr_search_n_rows_added */
	ulint innodb_hash_rows_removed;		/*!< btr_search_n_rows_removed */
	ulint innodb_hash_rows_updated;		/*!< btr_search_n_rows_updated */
	ibool innodb_have_atomic_builtins;	/*!< HAVE_ATOMIC_BUILTINS */
	ulint innodb_ibuf_inserts;		/*!< ibuf->n_inserts */
	ulint innodb_ibuf_merged_records;	/*!< ibuf->n_merged_recs */
	ulint innodb_ibuf_merges;		/*!< ibuf->n_merges */
	ulint innodb_ibuf_size;			/*!< ibuf->size */
	ulint innodb_lock_deadlocks;		/*!< srv_lock_deadlocks */
	ulint innodb_lock_wait_timeouts;	/*!< srv_lock_wait_timeouts */
	ulint innodb_log_checkpoints;
	ulint innodb_log_syncs;
	ulint innodb_log_waits;			/*!< srv_log_waits */
	ulint innodb_log_write_requests;	/*!< srv_log_write_requests */
	ulint innodb_log_writes;		/*!< srv_log_writes */
	ulint  innodb_log_write_archive;
	ulint  innodb_log_write_background_async;
	ulint  innodb_log_write_background_sync;
	ulint  innodb_log_write_checkpoint_async;
	ulint  innodb_log_write_checkpoint_sync;
	ulint  innodb_log_write_commit_async;
	ulint  innodb_log_write_commit_sync;
	ulint  innodb_log_write_flush_dirty;
	ulint  innodb_log_write_other;
	ulint  innodb_log_sync_archive;
	ulint  innodb_log_sync_background_async;
	ulint  innodb_log_sync_background_sync;
	ulint  innodb_log_sync_checkpoint_async;
	ulint  innodb_log_sync_checkpoint_sync;
	ulint  innodb_log_sync_commit_async;
	ulint  innodb_log_sync_commit_sync;
	ulint  innodb_log_sync_flush_dirty;
	ulint  innodb_log_sync_other;
	ib_int64_t innodb_lsn_current;		/*!< log_sys->lsn */
	ib_int64_t innodb_lsn_diff;		/*!< lsn_current - lsn_oldest */
	ib_int64_t innodb_lsn_oldest;		/*!< log_buf_pool_get_oldest_modification */
	ulint innodb_mutex_os_waits;		/*!< mutex_os_wait_count */
	ulint innodb_mutex_spin_rounds;		/*!< mutex_spin_round_count */
	ulint innodb_mutex_spin_waits;		/*!< mutex_spin_wait_count */
	ulint innodb_os_log_written;		/*!< srv_os_log_written */
	ulint innodb_os_log_fsyncs;		/*!< fil_n_log_flushes */
	ulint innodb_os_log_pending_writes;	/*!< srv_os_log_pending_writes */
	ulint innodb_os_log_pending_fsyncs;	/*!< fil_n_pending_log_flushes */
	ulint innodb_page_size;			/*!< UNIV_PAGE_SIZE */
	ulint innodb_pages_created;		/*!< buf_pool->stat.n_pages_created */
	ulint innodb_pages_read;		/*!< buf_pool->stat.n_pages_read */
	ulint innodb_pages_written;		/*!< buf_pool->stat.n_pages_written */
	ulint innodb_preflush_async_limit;	/*!< max_modified_age_async */
	ulint innodb_preflush_sync_limit;	/*!< max_modified_age_sync */
	ulint innodb_preflush_async_margin;	/*!< age - max_modified_age_async */
	ulint innodb_preflush_sync_margin;	/*!< age - max_modified_age_sync */
	ulint innodb_purge_pending;		/*!< trx_sys->rseg_history_len */
	ulint innodb_purged_pages;		/*!< srv_purged_pages */
	ulint innodb_row_lock_waits;		/*!< srv_n_lock_wait_count */
	ulint innodb_row_lock_current_waits;	/*!< srv_n_lock_wait_current_count */
	ib_int64_t innodb_row_lock_time;	/*!< srv_n_lock_wait_time
						/ 1000 */
	ulint innodb_row_lock_time_avg;		/*!< srv_n_lock_wait_time
						/ 1000
						/ srv_n_lock_wait_count */
	ulint innodb_row_lock_time_max;		/*!< srv_n_lock_max_wait_time
						/ 1000 */
	ulint innodb_rows_read;			/*!< srv_n_rows_read */
	ulint innodb_rows_inserted;		/*!< srv_n_rows_inserted */
	ulint innodb_rows_updated;		/*!< srv_n_rows_updated */
	ulint innodb_rows_deleted;		/*!< srv_n_rows_deleted */
	ulint innodb_rwlock_s_os_waits;		/*!< rw_s_os_wait_count */
	ulint innodb_rwlock_s_spin_rounds;	/*!< rw_s_spin_round_count */
	ulint innodb_rwlock_s_spin_waits;	/*!< rw_s_spin_wait_count */
	ulint innodb_rwlock_x_os_waits;		/*!< rw_x_os_wait_count */
	ulint innodb_rwlock_x_spin_rounds;	/*!< rw_x_spin_round_count */
	ulint innodb_rwlock_x_spin_waits;	/*!< rw_x_spin_wait_count */
	double innodb_srv_checkpoint_secs;	/*!< srv_checkpoint_secs */
	double innodb_srv_free_margin_fg_secs;	/*!< srv_free_margin_fg_secs */
	double innodb_srv_free_margin_bg_secs;	/*!< srv_free_margin_bg_secs */
	double innodb_srv_ibuf_contract_secs;	/*!< srv_ibuf_contract_secs */
	double innodb_srv_buf_flush_secs;	/*!< srv_ibuf_flush_secs */
	double innodb_srv_purge_secs;		/*!< srv_purge_secs */
	double innodb_srv_background_checkpoint_secs;
						/*!< srv_background_checkpoint_secs */
	double innodb_srv_foreground_checkpoint_secs;
						/*!< srv_foreground_checkpoint_secs */
	double innodb_srv_main_sleep_secs;	/*!< srv_main_sleep_secs */
	ulint innodb_trx_doublewrite_page_no;	/*!< trx_doublewrite->block1 */
	ulint innodb_trx_n_commit_all; /*!< srv_n_commit_with_undo */
	ulint innodb_trx_n_commit_with_undo; /*!< srv_n_commit_with_undo */
	ulint innodb_trx_n_rollback_partial; /*!< srv_n_rollback_partial */
	ulint innodb_trx_n_rollback_total; /*!< srv_n_rollback_total */

	ulint innodb_lru_restore_loaded_pages;	/*!< srv_lru_restore_loaded_pages */
	ulint innodb_lru_restore_total_pages;	/*!< srv_lru_restore_total_pages */

	ulint innodb_sec_rec_read_sees;		/*!< srv_sec_rec_read_sees */
	ulint innodb_sec_rec_read_check;	/*!< srv_sec_rec_read_check */
	/* The following are per-page size stats from page_zip_stat */
	ulint		zip1024_compressed;
	ulint		zip1024_compressed_ok;
	ib_int64_t	zip1024_compressed_usec;
	ib_int64_t	zip1024_compressed_ok_usec;
	ulint		zip1024_compressed_primary;
	ulint		zip1024_compressed_primary_ok;
	ib_int64_t	zip1024_compressed_primary_usec;
	ib_int64_t	zip1024_compressed_primary_ok_usec;
	ulint		zip1024_compressed_secondary;
	ulint		zip1024_compressed_secondary_ok;
	ib_int64_t	zip1024_compressed_secondary_usec;
	ib_int64_t	zip1024_compressed_secondary_ok_usec;
	ulint		zip1024_decompressed;
	ib_int64_t	zip1024_decompressed_usec;
	ulint		zip1024_decompressed_primary;
	ib_int64_t	zip1024_decompressed_primary_usec;
	ulint		zip1024_decompressed_secondary;
	ib_int64_t	zip1024_decompressed_secondary_usec;
	ulint		zip2048_compressed;
	ulint		zip2048_compressed_ok;
	ib_int64_t	zip2048_compressed_usec;
	ib_int64_t	zip2048_compressed_ok_usec;
	ulint		zip2048_compressed_primary;
	ulint		zip2048_compressed_primary_ok;
	ib_int64_t	zip2048_compressed_primary_usec;
	ib_int64_t	zip2048_compressed_primary_ok_usec;
	ulint		zip2048_compressed_secondary;
	ulint		zip2048_compressed_secondary_ok;
	ib_int64_t	zip2048_compressed_secondary_usec;
	ib_int64_t	zip2048_compressed_secondary_ok_usec;
	ulint		zip2048_decompressed;
	ib_int64_t	zip2048_decompressed_usec;
	ulint		zip2048_decompressed_primary;
	ib_int64_t	zip2048_decompressed_primary_usec;
	ulint		zip2048_decompressed_secondary;
	ib_int64_t	zip2048_decompressed_secondary_usec;
	ulint		zip4096_compressed;
	ulint		zip4096_compressed_ok;
	ib_int64_t	zip4096_compressed_usec;
	ib_int64_t	zip4096_compressed_ok_usec;
	ulint		zip4096_compressed_primary;
	ulint		zip4096_compressed_primary_ok;
	ib_int64_t	zip4096_compressed_primary_usec;
	ib_int64_t	zip4096_compressed_primary_ok_usec;
	ulint		zip4096_compressed_secondary;
	ulint		zip4096_compressed_secondary_ok;
	ib_int64_t	zip4096_compressed_secondary_usec;
	ib_int64_t	zip4096_compressed_secondary_ok_usec;
	ulint		zip4096_decompressed;
	ib_int64_t	zip4096_decompressed_usec;
	ulint		zip4096_decompressed_primary;
	ib_int64_t	zip4096_decompressed_primary_usec;
	ulint		zip4096_decompressed_secondary;
	ib_int64_t	zip4096_decompressed_secondary_usec;
	ulint		zip8192_compressed;
	ulint		zip8192_compressed_ok;
	ib_int64_t	zip8192_compressed_usec;
	ib_int64_t	zip8192_compressed_ok_usec;
	ulint		zip8192_compressed_primary;
	ulint		zip8192_compressed_primary_ok;
	ib_int64_t	zip8192_compressed_primary_usec;
	ib_int64_t	zip8192_compressed_primary_ok_usec;
	ulint		zip8192_compressed_secondary;
	ulint		zip8192_compressed_secondary_ok;
	ib_int64_t	zip8192_compressed_secondary_usec;
	ib_int64_t	zip8192_compressed_secondary_ok_usec;
	ulint		zip8192_decompressed;
	ib_int64_t	zip8192_decompressed_usec;
	ulint		zip8192_decompressed_primary;
	ib_int64_t	zip8192_decompressed_primary_usec;
	ulint		zip8192_decompressed_secondary;
	ib_int64_t	zip8192_decompressed_secondary_usec;
	ulint		zip16384_compressed;
	ulint		zip16384_compressed_ok;
	ib_int64_t	zip16384_compressed_usec;
	ib_int64_t	zip16384_compressed_ok_usec;
	ulint		zip16384_compressed_primary;
	ulint		zip16384_compressed_primary_ok;
	ib_int64_t	zip16384_compressed_primary_usec;
	ib_int64_t	zip16384_compressed_primary_ok_usec;
	ulint		zip16384_compressed_secondary;
	ulint		zip16384_compressed_secondary_ok;
	ib_int64_t	zip16384_compressed_secondary_usec;
	ib_int64_t	zip16384_compressed_secondary_ok_usec;
	ulint		zip16384_decompressed;
	ib_int64_t	zip16384_decompressed_usec;
	ulint		zip16384_decompressed_primary;
	ib_int64_t	zip16384_decompressed_primary_usec;
	ulint		zip16384_decompressed_secondary;
	ib_int64_t	zip16384_decompressed_secondary_usec;
	double		drop_table_phase1_secs;
	double		drop_table_phase2_secs;
	ulint 		innodb_malloc_cache_hits_compress;
	ulint 		innodb_malloc_cache_misses_compress;
	ulint 		innodb_malloc_cache_hits_decompress;
	ulint 		innodb_malloc_cache_misses_decompress;
	ulint 		innodb_malloc_cache_block_size_compress;
	ulint 		innodb_malloc_cache_block_size_decompress;
};

/** The server system struct */
struct srv_sys_struct{
	srv_table_t*	threads;	/*!< server thread table */
	UT_LIST_BASE_NODE_T(que_thr_t)
			tasks;		/*!< task queue */
};

extern ulint	srv_n_threads_active[];
#else /* !UNIV_HOTBACKUP */
# define srv_use_checksums			TRUE
# define srv_use_adaptive_hash_indexes		FALSE
# define srv_force_recovery			0UL
# define srv_set_io_thread_op_info(t,info)	((void) 0)
# define srv_is_being_started			0
# define srv_win_file_flush_method		SRV_WIN_IO_UNBUFFERED
# define srv_unix_file_flush_method		SRV_UNIX_O_DSYNC
# define srv_start_raw_disk_in_use		0
# define srv_file_per_table			1
#endif /* !UNIV_HOTBACKUP */

#endif
