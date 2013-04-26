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
 * 2005-01-15	Paul McCullagh
 *
 * H&G2JCtL
 */

#include "xt_config.h"

#ifdef DRIZZLED
#include <bitset>
#endif

#include <string.h>
#include <stdio.h>
#include <signal.h>

#include "pthread_xt.h"
#include "hashtab_xt.h"
#include "filesys_xt.h"
#include "database_xt.h"
#include "memory_xt.h"
#include "heap_xt.h"
#include "datalog_xt.h"
#include "strutil_xt.h"
#include "util_xt.h"
#include "trace_xt.h"
#include "myxt_xt.h"

#ifdef DEBUG
//#define XT_TEST_XACT_OVERFLOW
#endif

#ifndef NAME_MAX
#define NAME_MAX 128
#endif

/*
 * -----------------------------------------------------------------------
 * GLOBALS
 */

xtPublic XTDatabaseHPtr		pbxt_database = NULL;		// The global open database

xtPublic xtLogOffset		xt_db_log_file_threshold;
xtPublic size_t				xt_db_log_buffer_size;
xtPublic size_t				xt_db_transaction_buffer_size;
xtPublic size_t				xt_db_checkpoint_frequency;
xtPublic off_t				xt_db_data_log_threshold;
xtPublic size_t				xt_db_data_file_grow_size;
xtPublic size_t				xt_db_row_file_grow_size;
xtPublic size_t				xt_db_record_write_threshold;
xtPublic int				xt_db_garbage_threshold;
xtPublic int				xt_db_log_file_count;
xtPublic int				xt_db_auto_increment_mode;		/* 0 = MySQL compatible, 1 = PrimeBase Compatible. */
xtPublic int				xt_db_offline_log_function;		/* 0 = recycle logs, 1 = delete logs, 2 = keep logs */
xtPublic int				xt_db_sweeper_priority;			/* 0 = low (default), 1 = normal, 2 = high */
/* Buggy at the moment.
 * For example, a large alter table hangs.
 */
xtPublic int				xt_db_rewrite_flushing = 0;		/* 0 = Normal fsync, 1 = Re-write flushing. */
xtPublic int				xt_db_index_dirty_threshold;
xtPublic int				xt_db_flush_log_at_trx_commit;	/* 0 = no-write/no-flush, 1 = yes, 2 = write/no-flush */

xtPublic XTSortedListPtr	xt_db_open_db_by_id = NULL;
xtPublic XTHashTabPtr		xt_db_open_databases = NULL;
xtPublic time_t				xt_db_approximate_time = 0;		/* A "fast" alternative timer (not too accurate). */

static xtDatabaseID				db_next_id = 1;
static volatile XTOpenFilePtr	db_lock_file = NULL;

/*
 * -----------------------------------------------------------------------
 * LOCK/UNLOCK INSTALLATION
 */

xtPublic void xt_lock_installation(XTThreadPtr self, char *installation_path)
{
	char			file_path[PATH_MAX];
	char			buffer[101];
	size_t			red_size;
	llong			pid;
	xtBool			cd = pbxt_crash_debug;

	xt_strcpy(PATH_MAX, file_path, installation_path);
	xt_add_pbxt_file(PATH_MAX, file_path, "no-debug");
	if (xt_fs_exists(file_path))
		pbxt_crash_debug = FALSE;
	xt_strcpy(PATH_MAX, file_path, installation_path);
	xt_add_pbxt_file(PATH_MAX, file_path, "crash-debug");
	if (xt_fs_exists(file_path))
		pbxt_crash_debug = TRUE;

	if (pbxt_crash_debug != cd) {
		if (pbxt_crash_debug)
			xt_logf(XT_NT_WARNING, "Crash debugging has been turned on ('crash-debug' file exists)\n");
		else
			xt_logf(XT_NT_WARNING, "Crash debugging has been turned off ('no-debug' file exists)\n");
	}
	else if (pbxt_crash_debug)
		xt_logf(XT_NT_WARNING, "Crash debugging is enabled\n");

	/* Moved the lock file out of the pbxt directory so that
	 * it is possible to drop the pbxt database!
	 */
	xt_strcpy(PATH_MAX, file_path, installation_path);
	xt_add_dir_char(PATH_MAX, file_path);
	xt_strcat(PATH_MAX, file_path, "pbxt-lock");
	db_lock_file = xt_open_file(self, file_path, XT_FT_STANDARD, XT_FS_CREATE | XT_FS_MAKE_PATH, 0);

	try_(a) {
		if (!xt_lock_file(self, db_lock_file)) {
			xt_logf(XT_NT_ERROR, "A server appears to already be running\n");
			xt_logf(XT_NT_ERROR, "The file: %s, is locked\n", file_path);
			xt_throw_xterr(XT_CONTEXT, XT_ERR_SERVER_RUNNING);
		}
		if (!xt_pread_file(db_lock_file, 0, 100, 0, buffer, &red_size, &self->st_statistics.st_rec, self))
			xt_throw(self);
		if (red_size > 0) {
			buffer[red_size] = 0;
#ifdef XT_WIN
			pid = (llong) _atoi64(buffer);
#else
			pid = atoll(buffer);
#endif
			/* Problem with this code is, after a restart
			 * the process ID's are reused.
			 * If some system process grabs the proc id that
			 * the server had on the last run, then
			 * the database will not start.
			if (xt_process_exists((xtProcID) pid)) {
				xt_logf(XT_NT_ERROR, "A server appears to already be running, process ID: %lld\n", pid);
				xt_logf(XT_NT_ERROR, "Remove the file: %s, if this is not the case\n", file_path);
				xt_throw_xterr(XT_CONTEXT, XT_ERR_SERVER_RUNNING);
			}
			*/
			xt_logf(XT_NT_INFO, "The server was not shutdown correctly, recovery required\n");
#ifdef XT_BACKUP_BEFORE_RECOVERY
			if (pbxt_crash_debug) {
				/* The server was not shut down correctly. Make a backup before
				 * we start recovery.
				 */
				char extension[100];

				for (int i=1;;i++) {
					xt_strcpy(PATH_MAX, file_path, installation_path);
					xt_remove_dir_char(file_path);
					sprintf(extension, "-recovery-%d", i);
					xt_strcat(PATH_MAX, file_path, extension);
					if (!xt_fs_exists(file_path))
						break;
				}
				xt_logf(XT_NT_INFO, "In order to reproduce recovery errors a backup of the installation\n");
				xt_logf(XT_NT_INFO, "will be made to:\n");
				xt_logf(XT_NT_INFO, "%s\n", file_path);
				xt_logf(XT_NT_INFO, "Copy in progress...\n");
				xt_fs_copy_dir(self, installation_path, file_path);
				xt_logf(XT_NT_INFO, "Copy OK\n");
			}
#endif
		}

		sprintf(buffer, "%lld", (llong) xt_getpid());
		xt_set_eof_file(self, db_lock_file, 0);
		if (!xt_pwrite_file(db_lock_file, 0, strlen(buffer), buffer, &self->st_statistics.st_rec, self))
			xt_throw(self);
	}
	catch_(a) {
		xt_close_file_ns(db_lock_file);
		db_lock_file = NULL;
		xt_throw(self);
	}
	cont_(a);
}

xtPublic void xt_unlock_installation(XTThreadPtr self, char *installation_path)
{
	if (db_lock_file) {
		char lock_file[PATH_MAX];

		xt_unlock_file(NULL, db_lock_file);
		xt_close_file_ns(db_lock_file);
		db_lock_file = NULL;

		xt_strcpy(PATH_MAX, lock_file, installation_path);
		xt_add_dir_char(PATH_MAX, lock_file);
		xt_strcat(PATH_MAX, lock_file, "pbxt-lock");
		xt_fs_delete(self, lock_file);
	}
}

int *xt_bad_pointer = 0;

void xt_crash_me(void)
{
	if (pbxt_crash_debug)
		*xt_bad_pointer = 123;
}

/*
 * -----------------------------------------------------------------------
 * INIT/EXIT DATABASE
 */

static xtBool db_hash_comp(void *key, void *data)
{
	XTDatabaseHPtr	db = (XTDatabaseHPtr) data;

	return strcmp((char *) key, db->db_name) == 0;
}

static xtHashValue db_hash(xtBool is_key, void *key_data)
{
	XTDatabaseHPtr	db = (XTDatabaseHPtr) key_data;

	if (is_key)
		return xt_ht_hash((char *) key_data);
	return xt_ht_hash(db->db_name);
}

static xtBool db_hash_comp_ci(void *key, void *data)
{
	XTDatabaseHPtr	db = (XTDatabaseHPtr) data;

	return strcasecmp((char *) key, db->db_name) == 0;
}

static xtHashValue db_hash_ci(xtBool is_key, void *key_data)
{
	XTDatabaseHPtr	db = (XTDatabaseHPtr) key_data;

	if (is_key)
		return xt_ht_casehash((char *) key_data);
	return xt_ht_casehash(db->db_name);
}

static void db_hash_free(XTThreadPtr self, void *data)
{
	xt_heap_release(self, (XTDatabaseHPtr) data);
}

static int db_cmp_db_id(struct XTThread *XT_UNUSED(self), register const void *XT_UNUSED(thunk), register const void *a, register const void *b)
{
	xtDatabaseID	db_id = *((xtDatabaseID *) a);
	XTDatabaseHPtr	*db_ptr = (XTDatabaseHPtr *) b;

	if (db_id == (*db_ptr)->db_id)
		return 0;
	if (db_id < (*db_ptr)->db_id)
		return -1;
	return 1;
}

xtPublic void xt_init_databases(XTThreadPtr self)
{
	if (pbxt_ignore_case)
		xt_db_open_databases = xt_new_hashtable(self, db_hash_comp_ci, db_hash_ci, db_hash_free, TRUE, TRUE);
	else
		xt_db_open_databases = xt_new_hashtable(self, db_hash_comp, db_hash, db_hash_free, TRUE, TRUE);
	xt_db_open_db_by_id = xt_new_sortedlist(self, sizeof(XTDatabaseHPtr), 20, 10, db_cmp_db_id, NULL, NULL, FALSE, FALSE);
}

xtPublic void xt_stop_database_threads(XTThreadPtr self, xtBool sync)
{
	u_int			len = 0;
	XTDatabaseHPtr	*dbptr;
	XTDatabaseHPtr	db = NULL;
	
	if (xt_db_open_db_by_id)
		len = xt_sl_get_size(xt_db_open_db_by_id);
	for (u_int i=0; i<len; i++) {
		if ((dbptr = (XTDatabaseHPtr *) xt_sl_item_at(xt_db_open_db_by_id, i))) {
			db = *dbptr;

			if (sync) {
				/* Wait for the sweeper: */
				xt_wait_for_sweeper(self, db, 16);
				
				/* Wait for the writer: */
				xt_wait_for_writer(self, db);

				/* Wait for the checkpointer: */
				xt_wait_for_checkpointer(self, db);
			}
			xt_stop_flusher(self, db);
			xt_stop_checkpointer(self, db);
			xt_stop_writer(self, db);
			xt_stop_sweeper(self, db);
			xt_stop_compactor(self, db);
			xt_db_stop_pool_threads(self, db);
		}
	}
}

xtPublic void xt_exit_databases(XTThreadPtr self)
{
	if (xt_db_open_databases) {
		xt_free_hashtable(self, xt_db_open_databases);
		xt_db_open_databases = NULL;
	}
	if (xt_db_open_db_by_id) {
		xt_free_sortedlist(self, xt_db_open_db_by_id);
		xt_db_open_db_by_id = NULL;
	}
}

xtPublic void xt_create_database(XTThreadPtr self, char *path)
{
	xt_fs_mkdir(self, path);
}

static void db_finalize(XTThreadPtr self, void *x)
{
	XTDatabaseHPtr	db = (XTDatabaseHPtr) x;

	xt_stop_flusher(self, db);
	xt_stop_checkpointer(self, db);
	xt_stop_compactor(self, db);
	xt_stop_sweeper(self, db);
	xt_stop_writer(self, db);
	xt_db_thread_pool_exit(self, db);

	xt_sl_delete(self, xt_db_open_db_by_id, &db->db_id);
	/* 
	 * Important is that xt_db_pool_exit() is called
	 * before xt_xn_exit_db() because xt_xn_exit_db()
	 * frees the checkpoint information which
	 * may be required to shutdown the tables, which
	 * flushes tables, and therefore does a checkpoint.
	 */
	/* This was the previous order of shutdown:
	xt_xn_exit_db(self, db);
	xt_dl_exit_db(self, db);
	xt_db_pool_exit(self, db);
	db->db_indlogs.ilp_exit(self);
	*/

	xt_db_pool_exit(self, db);
	db->db_indlogs.ilp_exit(self); 
	xt_dl_exit_db(self, db);
	xt_xn_exit_db(self, db);
	xt_tab_exit_db(self, db);
	if (db->db_name) {
		xt_free(self, db->db_name);
		db->db_name = NULL;
	}
	if (db->db_main_path) {
		xt_free(self, db->db_main_path);
		db->db_main_path = NULL;
	}
	xt_free_mutex(&db->db_init_sweep_lock);
}

static void db_onrelease(void *XT_UNUSED(x))
{
	/* Signal threads waiting for exclusive use of the database: */
	if (xt_db_open_databases)	// The database may already be closed.
		xt_ht_signal(NULL, xt_db_open_databases);
}

xtPublic void xt_add_pbxt_file(size_t size, char *path, const char *file)
{
	xt_add_dir_char(size, path);
	xt_strcat(size, path, "pbxt");
	xt_add_dir_char(size, path);
	xt_strcat(size, path, file);
}

xtPublic void xt_add_location_file(size_t size, char *path)
{
	xt_add_dir_char(size, path);
	xt_strcat(size, path, "pbxt");
	xt_add_dir_char(size, path);
	xt_strcat(size, path, "location");
}

xtPublic void xt_add_tables_file(size_t size, char *path)
{
	xt_add_dir_char(size, path);
	xt_strcat(size, path, "pbxt");
	xt_add_dir_char(size, path);
	xt_strcat(size, path, "tables");
}

xtPublic void xt_add_pbxt_dir(size_t size, char *path)
{
	xt_add_dir_char(size, path);
	xt_strcat(size, path, "pbxt");
}

xtPublic void xt_add_system_dir(size_t size, char *path)
{
	xt_add_dir_char(size, path);
	xt_strcat(size, path, "pbxt");
	xt_add_dir_char(size, path);
	xt_strcat(size, path, "system");
}

xtPublic void xt_add_data_dir(size_t size, char *path)
{
	xt_add_dir_char(size, path);
	xt_strcat(size, path, "pbxt");
	xt_add_dir_char(size, path);
	xt_strcat(size, path, "data");
}

/*
 * I have a problem here. I cannot rely on the path given to xt_get_database() to be
 * consistant. When called from ha_create_table() the path is not modified.
 * However when called from ha_open() the path is first transformed by a call to
 * fn_format(). I have given an example from a stack trace below.
 *
 * In this case the odd path comes from the option:
 * --tmpdir=/Users/build/Development/mysql/debug-mysql/mysql-test/var//tmp
 *
 * #3  0x001a3818 in ha_pbxt::create(char const*, st_table*, st_ha_create_information*) 
 *     (this=0x2036898, table_path=0xf0060bd0 "/users/build/development/mysql/debug-my
 *     sql/mysql-test/var//tmp/#sql5718_1_0.frm", table_arg=0xf00601c0,
 *     create_info=0x2017410) at ha_pbxt.cc:2323
 * #4  0x00140d74 in ha_create_table(char const*, st_ha_create_information*, bool) 
 *     (name=0xf0060bd0 "/users/build/development/mysql/debug-mysql/mysql-te
 *     st/var//tmp/#sql5718_1_0.frm", create_info=0x2017410, 
 *     update_create_info=false) at handler.cc:1387
 *
 * #4  0x0013f7a4 in handler::ha_open(char const*, int, int) (this=0x203ba98, 
 *     name=0xf005eb70 "/users/build/development/mysql/debug-mysql/mysql-te
 *     st/var/tmp/#sql5718_1_1", mode=2, test_if_locked=2) at handler.cc:993
 * #5  0x000cd900 in openfrm(char const*, char const*, unsigned, unsigned, 
 *     unsigned, st_table*) (name=0xf005f260 "/users/build/development/mys
 *     ql/debug-mysql/mysql-test/var//tmp/#sql5718_1_1.frm", 
 *     alias=0xf005fb90 "#sql-5718_1", db_stat=7, prgflag=44, 
 *     ha_open_flags=0, outparam=0x2039e18) at table.cc:771
 *
 * As a result, I no longer use the entire path as the key to find a database.
 * Just the last component of the path (i.e. the database name) should be
 * sufficient!?
 */
xtPublic XTDatabaseHPtr xt_get_database(XTThreadPtr self, char *path, xtBool multi_path)
{
	XTDatabaseHPtr	db = NULL;
	char			db_path[PATH_MAX];
	char			db_name[NAME_MAX];
	xtBool			multi_path_db = FALSE;

	/* A database may not be in use when this is called. */
	ASSERT(!self->st_database);
	xt_ht_lock(self, xt_db_open_databases);
	pushr_(xt_ht_unlock, xt_db_open_databases);

	xt_strcpy(PATH_MAX, db_path, path);
	xt_add_location_file(PATH_MAX, db_path);
	if (multi_path || xt_fs_exists(db_path))
		multi_path_db = TRUE;

	xt_strcpy(PATH_MAX, db_path, path);
	xt_remove_dir_char(db_path);
	xt_strcpy(NAME_MAX, db_name, xt_last_directory_of_path(db_path));

	db = (XTDatabaseHPtr) xt_ht_get(self, xt_db_open_databases, db_name);
	if (!db) {
		pushsr_(db, xt_heap_release, (XTDatabaseHPtr) xt_heap_new(self, sizeof(XTDatabaseRec), db_finalize));
		xt_heap_set_release_callback(db, db_onrelease);
		xt_init_mutex_with_autoname(self, &db->db_init_sweep_lock);
		db->db_id = db_next_id++;
		db->db_name = xt_dup_string(self, db_name);
		db->db_main_path = xt_dup_string(self, db_path);
		db->db_multi_path = multi_path_db;
#ifdef XT_TEST_XACT_OVERFLOW
		/* Test transaction ID overflow: */
		db->db_xn_curr_id = 0xFFFFFFFF - 30;
#endif
		xt_db_pool_init(self, db);
		xt_tab_init_db(self, db);
		xt_dl_init_db(self, db);

		/* Initialize the index logs: */
		db->db_indlogs.ilp_init(self, db, XT_INDEX_WRITE_BUFFER_SIZE); 

		/* Recover in xt_xn_init_db() may use background threads!: */
		xt_db_thread_pool_init(self, db);

		xt_xn_init_db(self, db);
		xt_sl_insert(self, xt_db_open_db_by_id, &db->db_id, &db);

		xt_start_sweeper(self, db);
		xt_start_compactor(self, db);
		xt_start_writer(self, db);
		xt_start_checkpointer(self, db);
		if (xt_db_flush_log_at_trx_commit == 0 || xt_db_flush_log_at_trx_commit == 2)
			xt_start_flusher(self, db);

		popr_();
		xt_ht_put(self, xt_db_open_databases, db);

		/* The recovery process could attach parts of the open
		 * database to the thread!
		 */
		xt_unuse_database(self, self);
	}
	xt_heap_reference(self, db);
	freer_();

	/* {INDEX-RECOV_ROWID}
	 * Wait for sweeper to finish processing possibly
	 * unswept transactions after recovery.
	 * This is required because during recovery for
	 * all index entries written the row_id is set.
	 *
	 * When the row ID is set, this means that the row
	 * is "clean". i.e. visible to all transactions.
	 *
	 * Obviously this is not necessary the case for all
	 * index entries recovered. For example, 
	 * transactions that still need to be swept may be
	 * rolled back.
	 *
	 * As a result, we have to wait the the sweeper
	 * to complete. Only then can we be sure that
	 * all index entries that are not visible have
	 * been removed.
	 *
	 * REASON WHY WE SET ROWID ON RECOVERY:
	 * The row ID is set on recovery because the
	 * change to the index may be lost after a crash.
	 * The change to the index is done by the sweeper, and
	 * there is no record of this change in the log.
	 * The sweeper will not "re-sweep" all transations
	 * that are recovered. As a result, this upadte
	 * of the index by the sweeper may be lost.
	 *
	 * {OPEN-DB-SWEEPER-WAIT}
	 * This has been moved to after the release of the open
	 * database lock because:
	 *
	 * - We are waiting for the sweeper which may run out of
	 * record cache.
	 * - If it runs out of cache it well wait
	 * for the freeer thread.
	 * - For the freeer thread to be able to work it needs
	 * to open the database.
	 * - To open the database it needs the open database
	 * lock.
	 */
	/*
	 * This has been moved, see: {WAIT-FOR-SW-AFTER-RECOV}
	pushr_(xt_heap_release, db);
	xt_wait_for_sweeper(self, db, 0);
	popr_();
	*/

	return db;
}

xtPublic XTDatabaseHPtr xt_get_database_by_id(XTThreadPtr self, xtDatabaseID db_id)
{
	XTDatabaseHPtr	*dbptr;
	XTDatabaseHPtr	db = NULL;

	xt_ht_lock(self, xt_db_open_databases);
	pushr_(xt_ht_unlock, xt_db_open_databases);
	if ((dbptr = (XTDatabaseHPtr *) xt_sl_find(self, xt_db_open_db_by_id, &db_id))) {
		db = *dbptr;
		xt_heap_reference(self, db);
	}
	freer_(); // xt_ht_unlock(xt_db_open_databases)
	return db;
}

xtPublic void xt_drop_database(XTThreadPtr self, XTDatabaseHPtr	db)
{
	char			path[PATH_MAX];
	char			db_name[NAME_MAX];
	XTOpenDirPtr	od;
	char			*file;
	XTTablePathPtr	*tp_ptr;

	xt_ht_lock(self, xt_db_open_databases);
	pushr_(xt_ht_unlock, xt_db_open_databases);

	/* Shutdown the database daemons: */
	xt_stop_flusher(self, db);
	xt_stop_checkpointer(self, db);
	xt_stop_sweeper(self, db);
	xt_stop_compactor(self, db);
	xt_stop_writer(self, db);
	xt_db_thread_pool_exit(self, db);

	/* Remove the database from the directory: */
	xt_strcpy(NAME_MAX, db_name, db->db_name);
	xt_ht_del(self, xt_db_open_databases, db_name);

	/* Release the lock on the database directory: */
	freer_(); // xt_ht_unlock(xt_db_open_databases)

	/* Delete the transaction logs: */
	xt_xlog_delete_logs(self, db);

	/* Delete the data logs: */
	xt_dl_delete_logs(self, db);

	for (u_int i=0; i<xt_sl_get_size(db->db_table_paths); i++) {

		tp_ptr = (XTTablePathPtr *) xt_sl_item_at(db->db_table_paths, i);

		xt_strcpy(PATH_MAX, path, (*tp_ptr)->tp_path);

		/* Delete all files in the database: */
		pushsr_(od, xt_dir_close, xt_dir_open(self, path, NULL));
		while (xt_dir_next(self, od)) {
			file = xt_dir_name(self, od);
			if (xt_ends_with(file, ".xtr") ||
				xt_ends_with(file, ".xtd") ||
				xt_ends_with(file, ".xti") ||
				xt_ends_with(file, ".xt"))
			{
				xt_add_dir_char(PATH_MAX, path);
				xt_strcat(PATH_MAX, path, file);
				xt_fs_delete(self, path);
				xt_remove_last_name_of_path(path);
			}
		}
		freer_(); // xt_dir_close(od)
		
	}
	if (!db->db_multi_path) {
		xt_strcpy(PATH_MAX, path, db->db_main_path);
		xt_add_pbxt_dir(PATH_MAX, path);
		if (!xt_fs_rmdir(NULL, path))
			xt_log_and_clear_exception(self);
	}
}

/*
 * Open/use a database.
 */
xtPublic void xt_open_database(XTThreadPtr self, char *path, xtBool multi_path)
{
	XTDatabaseHPtr db;

	/* We cannot get a database, without unusing the current
	 * first. The reason is that the restart process will
	 * partially set the current database!
	 */
	xt_unuse_database(self, self);
	db = xt_get_database(self, path, multi_path);
	pushr_(xt_heap_release, db);
	xt_use_database(self, db, XT_FOR_USER);
	freer_();	// xt_heap_release(self, db);	
}

/* This function can only be called if you do not already have a database in
 * use. This is because to get a database pointer you are not allowed
 * to have a database in use!
 */
xtPublic void xt_use_database(XTThreadPtr self, XTDatabaseHPtr db, int what_for)
{
	/* Check if a transaction is in progress. If so,
	 * we cannot change the database!
	 */
	if (self->st_xact_data || self->st_database)
		xt_throw_xterr(XT_CONTEXT, XT_ERR_CANNOT_CHANGE_DB);

	xt_heap_reference(self, db);
	self->st_database = db;
#ifdef XT_WAIT_FOR_CLEANUP
	self->st_last_xact = 0;
	for (int i=0; i<XT_MAX_XACT_BEHIND; i++) {
		self->st_prev_xact[i] = db->db_xn_curr_id;
	}
#endif
	xt_xn_init_thread(self, what_for);
}

xtPublic void xt_unuse_database(XTThreadPtr self, XTThreadPtr other_thr)
{
	XTTask *tk;

	/* Wait for any asynchronous tasks to complete: */
	xt_wait_for_async_tasks(self);

	/* Free the results, if any: */
	while ((tk = (XTTask *) xt_get_task_result(self)))
		tk->tk_release();

	/* Abort the transacion if it belongs exclusively to this thread. */
	xt_lock_mutex(self, &other_thr->t_lock);
	pushr_(xt_unlock_mutex, &other_thr->t_lock);

	xt_xn_exit_thread(other_thr);
	if (other_thr->st_database) {
		xt_heap_release(self, other_thr->st_database);
		other_thr->st_database = NULL;
	}
	
	freer_();
}

xtPublic void xt_db_init_thread_ns(XTThreadPtr XT_UNUSED(new_thread))
{
#ifdef XT_IMPLEMENT_NO_ACTION
	memset(&new_thread->st_restrict_list, 0, sizeof(XTBasicListRec));
	new_thread->st_restrict_list.bl_item_size = sizeof(XTRestrictItemRec);
#endif
}

xtPublic void xt_db_exit_thread(XTThreadPtr self)
{
#ifdef XT_IMPLEMENT_NO_ACTION
	xt_bl_free(NULL, &self->st_restrict_list);
#endif
	xt_unuse_database(self, self);
}

/*
 * -----------------------------------------------------------------------
 * OPEN TABLE POOL
 */

#ifdef UNUSED_CODE
static void check_free_list(XTDatabaseHPtr db)
{
	XTOpenTablePtr	ot;
	u_int			cnt = 0;

	ot = db->db_ot_pool.otp_mr_used;
	if (ot)
		ASSERT_NS(!ot->ot_otp_mr_used);
	ot = db->db_ot_pool.otp_lr_used;
	if (ot)
		ASSERT_NS(!ot->ot_otp_lr_used);
	while (ot) {
		cnt++;
		ot = ot->ot_otp_mr_used;
	}
	ASSERT_NS(cnt == db->db_ot_pool.otp_total_free);
}
#endif

xtPublic void xt_db_pool_init(XTThreadPtr self, XTDatabaseHPtr db)
{
	memset(&db->db_ot_pool, 0, sizeof(XTAllTablePoolsRec));
	xt_init_mutex_with_autoname(self, &db->db_ot_pool.opt_lock);
	xt_init_cond(self, &db->db_ot_pool.opt_cond);
}

xtPublic void xt_db_pool_exit(XTThreadPtr self, XTDatabaseHPtr db)
{
	XTOpenTablePoolPtr	table_pool, tmp;
	XTOpenTablePtr		ot, tmp_ot;

	xt_free_mutex(&db->db_ot_pool.opt_lock);
	xt_free_cond(&db->db_ot_pool.opt_cond);
	
	for (u_int i=0; i<XT_OPEN_TABLE_POOL_HASH_SIZE; i++) {
		table_pool = db->db_ot_pool.otp_hash[i];
		while (table_pool) {
			tmp = table_pool->opt_next_hash;
			ot = table_pool->opt_free_list;
			while (ot) {
				tmp_ot = ot->ot_otp_next_free;
				ot->ot_thread = self;
				xt_close_table(ot, TRUE, FALSE);
				ot = tmp_ot;
			}
			xt_free(self, table_pool);
			table_pool = tmp;
		}
	}
}

static XTOpenTablePoolPtr db_get_open_table_pool(XTDatabaseHPtr db, xtTableID tab_id)
{
	XTOpenTablePoolPtr	table_pool;
	u_int				hash;

	hash = tab_id % XT_OPEN_TABLE_POOL_HASH_SIZE;
	table_pool = db->db_ot_pool.otp_hash[hash];
	while (table_pool) {
		if (table_pool->opt_tab_id == tab_id)
			return table_pool;
		table_pool = table_pool->opt_next_hash;
	}
	
	if (!(table_pool = (XTOpenTablePoolPtr) xt_malloc_ns(sizeof(XTOpenTablePoolRec))))
		return NULL;

	table_pool->opt_db = db;
	table_pool->opt_tab_id = tab_id;
	table_pool->opt_total_open = 0;
	table_pool->opt_locked = XT_TABLE_NOT_LOCKED;
	table_pool->opt_free_list = NULL;
	table_pool->opt_next_hash = db->db_ot_pool.otp_hash[hash];
	db->db_ot_pool.otp_hash[hash] = table_pool;
	
	return table_pool;
}

static void db_free_open_table_pool(XTThreadPtr self, XTOpenTablePoolPtr table_pool)
{
	if (!table_pool->opt_locked && !table_pool->opt_total_open) {
		XTOpenTablePoolPtr	ptr, pptr = NULL;
		u_int				hash;

		hash = table_pool->opt_tab_id % XT_OPEN_TABLE_POOL_HASH_SIZE;
		ptr = table_pool->opt_db->db_ot_pool.otp_hash[hash];
		while (ptr) {
			if (ptr == table_pool)
				break;
			pptr = ptr;
			ptr = ptr->opt_next_hash;
		}
		
		if (ptr == table_pool) {
			if (pptr)
				pptr->opt_next_hash = table_pool->opt_next_hash;
			else
				table_pool->opt_db->db_ot_pool.otp_hash[hash] = table_pool->opt_next_hash;
		}

		xt_free(self, table_pool);
	}
}

static XTOpenTablePoolPtr db_lock_table_pool(XTThreadPtr self, XTDatabaseHPtr db, xtTableID tab_id, xtBool flush_table)
{
	XTOpenTablePoolPtr	table_pool;
	XTOpenTablePtr		ot, tmp_ot;

	xt_lock_mutex(self, &db->db_ot_pool.opt_lock);
	pushr_(xt_unlock_mutex, &db->db_ot_pool.opt_lock);

	if (!(table_pool = db_get_open_table_pool(db, tab_id)))
		xt_throw(self);

	/* Wait for the lock: */
	while (table_pool->opt_locked) {
		xt_timed_wait_cond(self, &db->db_ot_pool.opt_cond, &db->db_ot_pool.opt_lock, 2000);
		if (!(table_pool = db_get_open_table_pool(db, tab_id)))
			xt_throw(self);
	}

	/* Enter locking phase 1: */
	table_pool->opt_locked = XT_TABLE_LOCK_WAITING;

	if (flush_table) {
		/* Don't know if this is interesting as a phase, but anyway... */
		table_pool->opt_locked = XT_TABLE_LOCK_FLUSHING;
		freer_(); // xt_unlock_mutex(db_ot_pool.opt_lock)

		pushr_(xt_db_unlock_table_pool, table_pool);
		/* During this time, background processes can use the
		 * pool!
		 *
		 * May also do a flush, but this is now taken care
		 * of here {FLUSH-BUG}
		 */
		if ((ot = xt_db_open_pool_table(self, db, tab_id, NULL, TRUE))) {
			pushr_(xt_db_return_table_to_pool, ot);
			xt_sync_flush_table(self, ot, 0);
			freer_(); //xt_db_return_table_to_pool_foreground(ot);
		}

		popr_(); // Discard xt_db_unlock_table_pool_no_lock(table_pool)

		xt_lock_mutex(self, &db->db_ot_pool.opt_lock);
		pushr_(xt_unlock_mutex, &db->db_ot_pool.opt_lock);
	}
	
	/* Free all open tables not in use: */
	ot = table_pool->opt_free_list;
	table_pool->opt_free_list = NULL;
	while (ot) {
		tmp_ot = ot->ot_otp_next_free;

		/* Remove from MRU list: */
		if (db->db_ot_pool.otp_lr_used == ot)
			db->db_ot_pool.otp_lr_used = ot->ot_otp_mr_used;
		if (db->db_ot_pool.otp_mr_used == ot)
			db->db_ot_pool.otp_mr_used = ot->ot_otp_lr_used;
		if (ot->ot_otp_lr_used)
			ot->ot_otp_lr_used->ot_otp_mr_used = ot->ot_otp_mr_used;
		if (ot->ot_otp_mr_used)
			ot->ot_otp_mr_used->ot_otp_lr_used = ot->ot_otp_lr_used;

		if (db->db_ot_pool.otp_lr_used)
			db->db_ot_pool.otp_free_time = db->db_ot_pool.otp_lr_used->ot_otp_free_time;
		
		ASSERT_NS(db->db_ot_pool.otp_total_free > 0);
		db->db_ot_pool.otp_total_free--;

		/* Close the table: */
		ASSERT(table_pool->opt_total_open > 0);
		table_pool->opt_total_open--;

		ot->ot_thread = self;
		xt_close_table(ot, table_pool->opt_total_open == 0, FALSE);

		/* Go to the next: */
		ot = tmp_ot;
	}

	/* Wait for other to close: */
	while (table_pool->opt_total_open > 0) {
		xt_timed_wait_cond_ns(&db->db_ot_pool.opt_cond, &db->db_ot_pool.opt_lock, 2000);
	}

	/* 2nd phase, now the table is really locked: */
	table_pool->opt_locked = XT_TABLE_LOCKED;

	freer_(); // xt_unlock_mutex(db_ot_pool.opt_lock)
	return table_pool;
}

/*
 * This function locks a particular table by locking the table directory
 * and waiting for all open tables handles to close.
 *
 * Things are a bit complicated because the sweeper must be turned off before
 * the table directory is locked.
 */
xtPublic XTOpenTablePoolPtr xt_db_lock_table_pool_by_name(XTThreadPtr self, XTDatabaseHPtr db, XTPathStrPtr tab_name, xtBool no_load, xtBool flush_table, xtBool missing_ok, XTTableHPtr *ret_tab)
{
	XTOpenTablePoolPtr	table_pool;
	XTTableHPtr			tab;
	xtTableID			tab_id;

	pushsr_(tab, xt_heap_release, xt_use_table(self, tab_name, no_load, missing_ok));
	if (!tab) {
		freer_(); // xt_heap_release(tab)
		return NULL;
	}

	tab_id = tab->tab_id;

	if (ret_tab) {
		*ret_tab = tab;
		table_pool = db_lock_table_pool(self, db, tab_id, flush_table);
		popr_(); // Discard xt_heap_release(tab)
		return table_pool;
	}

	freer_(); // xt_heap_release(tab)
	return db_lock_table_pool(self, db, tab_id, flush_table);
}

xtPublic void xt_db_unlock_table_pool(XTThreadPtr self, XTOpenTablePoolPtr table_pool)
{
	XTDatabaseHPtr db;

	if (!table_pool)
		return;

	db = table_pool->opt_db;
	xt_lock_mutex(self, &db->db_ot_pool.opt_lock);
	pushr_(xt_unlock_mutex, &db->db_ot_pool.opt_lock);

	table_pool->opt_locked = XT_TABLE_NOT_LOCKED;
	xt_broadcast_cond(self, &db->db_ot_pool.opt_cond);
	db_free_open_table_pool(NULL, table_pool);

	freer_(); // xt_unlock_mutex(db_ot_pool.opt_lock)
}

xtPublic XTOpenTablePtr xt_db_open_table_using_tab(XTTableHPtr tab, XTThreadPtr thread)
{
	XTDatabaseHPtr		db = tab->tab_db;
	XTOpenTablePoolPtr	table_pool;
	XTOpenTablePtr		ot;

	xt_lock_mutex_ns(&db->db_ot_pool.opt_lock);

	if (!(table_pool = db_get_open_table_pool(db, tab->tab_id)))
		goto failed;

	while (table_pool->opt_locked) {
		if (!xt_timed_wait_cond_ns(&db->db_ot_pool.opt_cond, &db->db_ot_pool.opt_lock, 2000))
			goto failed_1;
		if (!(table_pool = db_get_open_table_pool(db, tab->tab_id)))
			goto failed;
	}

	if ((ot = table_pool->opt_free_list)) {
		/* Remove from the free list: */
		table_pool->opt_free_list = ot->ot_otp_next_free;
		
		/* Remove from MRU list: */
		if (db->db_ot_pool.otp_lr_used == ot)
			db->db_ot_pool.otp_lr_used = ot->ot_otp_mr_used;
		if (db->db_ot_pool.otp_mr_used == ot)
			db->db_ot_pool.otp_mr_used = ot->ot_otp_lr_used;
		if (ot->ot_otp_lr_used)
			ot->ot_otp_lr_used->ot_otp_mr_used = ot->ot_otp_mr_used;
		if (ot->ot_otp_mr_used)
			ot->ot_otp_mr_used->ot_otp_lr_used = ot->ot_otp_lr_used;

		if (db->db_ot_pool.otp_lr_used)
			db->db_ot_pool.otp_free_time = db->db_ot_pool.otp_lr_used->ot_otp_free_time;

		ASSERT_NS(db->db_ot_pool.otp_total_free > 0);
		db->db_ot_pool.otp_total_free--;

		ot->ot_thread = thread;
		goto done_ok;
	}

	if ((ot = xt_open_table(tab))) {
		ot->ot_thread = thread;
		table_pool->opt_total_open++;
	}

	done_ok:
	db_free_open_table_pool(NULL, table_pool);
	xt_unlock_mutex_ns(&db->db_ot_pool.opt_lock);
	return ot;

	failed_1:
	db_free_open_table_pool(NULL, table_pool);

	failed:
	xt_unlock_mutex_ns(&db->db_ot_pool.opt_lock);
	return NULL;
}

xtPublic xtBool xt_db_open_pool_table_ns(XTOpenTablePtr *ret_ot, XTDatabaseHPtr db, xtTableID tab_id)
{
	XTThreadPtr	self = xt_get_self();
	xtBool		ok = TRUE;

	try_(a) {
		*ret_ot = xt_db_open_pool_table(self, db, tab_id, NULL, FALSE);
	}
	catch_(a) {
		ok = FALSE;
	}
	cont_(a);
	return ok;
}

xtPublic XTOpenTablePtr xt_db_open_pool_table(XTThreadPtr self, XTDatabaseHPtr db, xtTableID tab_id, int *result, xtBool i_am_background)
{
	XTOpenTablePtr		ot;
	XTOpenTablePoolPtr	table_pool;
	XTTableHPtr			tab;

	xt_lock_mutex(self, &db->db_ot_pool.opt_lock);
	pushr_(xt_unlock_mutex, &db->db_ot_pool.opt_lock);

	if (!(table_pool = db_get_open_table_pool(db, tab_id)))
		xt_throw(self);

	/* Background processes do not have to wait while flushing!
	 *
	 * I think I did this so that the background process would
	 * not hang during flushing. Exact reason currently
	 * unknown (maybe not any more see {HANG-ON-FREEER} below).
	 *
	 * NOTE: I have taken out check:
	 * && !(i_am_background && table_pool->opt_flushing) below.
	 * To which the text above refers. The reason is because it
	 * has been replaced by the more general rule (described
	 * below). This was used to ensure that background
	 * threads do not hang when the table is flushed
	 * in db_lock_table_pool(). Now (see below) I make
	 * sure that a background thread does not hang during a
	 * lock table, as long as the locker is still waiting!
	 *
	 * The fact that a background thread can get a open table
	 * handle while a user thread is locking and flushing a table
	 * led to the situation that the checkpointer
	 * could flush at the same time as a user process
	 * which was flushing due to a rename.
	 *
	 * This led to the situation described here: {FLUSH-BUG},
	 * which is now fixed.
	 *
	 * {HANG-ON-FREEER}
	 * 
	 * This error occurred during count_distinct3
	 *
	 * The sweeper is waiting for the free'er, but the sweeper has a table
	 * open (Table ./test/t2 test, ID 2)
	 *
	 * if (!xt_timed_wait_cond_ns(&dcg->tcm_freeer_cond, &dcg->tcm_freeer_lock, 30000)) {
	 * 	dcg->tcm_threads_waiting--;
	 * 	break;
	 * }
	 * #3	0x00e2a5b6 in xt_p_cond_timedwait at pthread_xt.cc:697
	 * #4	0x00e51543 in xt_timed_wait_cond at thread_xt.cc:2053
	 * #5	0x00e38fac in XTTabCache::tc_fetch at tabcache_xt.cc:682
	 * #6	0x00e3974c in XTTabCache::xt_tc_write_cond at tabcache_xt.cc:279
	 * #7	0x00e5ce31 in xn_sw_cleanup_variation at xaction_xt.cc:2101
	 * #8	0x00e5d619 in xn_sw_cleanup_xact at xaction_xt.cc:2327
	 * #9	0x00e5dddd in xn_sw_main at xaction_xt.cc:2608
	 * #10	0x00e5e123 in xn_sw_run_thread at xaction_xt.cc:2741
	 * #11	0x00e50640 in thr_main at thread_xt.cc:1081
	 * #12	0x91fdb155 in _pthread_start
	 * #13	0x91fdb012 in thread_start
	 *
	 * The user thread is trying to drop the table but the table has 1 open table
	 * (Table ./test/t2 test, ID 2)
	 *
	 * while (table_pool->opt_total_open > 0) {
	 * 	xt_timed_wait_cond(self, &db->db_ot_pool.opt_cond, &db->db_ot_pool.opt_lock, 2000);
	 * }
	 * #3	0x00e2a5b6 in xt_p_cond_timedwait at pthread_xt.cc:697
	 * #4	0x00e51543 in xt_timed_wait_cond at thread_xt.cc:2053
	 * But fix: xt_db_wait_for_open_tables() has been removed!
	 * #5	0x00de88a2 in xt_db_wait_for_open_tables at database_xt.cc:966
	 * #6	0x00e3e983 in tab_lock_table at table_xt.cc:1557
	 * #7	0x00e3eb1e in xt_drop_table at table_xt.cc:1941
	 * #8	0x00e0e78e in ha_pbxt::delete_table at ha_pbxt.cc:4848
	 * #9	0x00243a14 in handler::ha_delete_table at handler.cc:3311
	 * #10	0x00243b7e in ha_delete_table at handler.cc:1954
	 * #11	0x0025c990 in mysql_rm_table_part2 at sql_table.cc:1724
	 * #12	0x0025cee8 in mysql_rm_table at sql_table.cc:1518
	 * #13	0x0011256c in mysql_execute_command at sql_parse.cc:3357
	 * #14	0x001188ed in mysql_parse at sql_parse.cc:5929
	 * #15	0x00119663 in dispatch_command at sql_parse.cc:1216
	 * #16	0x0011a960 in do_command at sql_parse.cc:857
	 * #17	0x00105a3c in handle_one_connection at sql_connect.cc:1115
	 * #18	0x91fdb155 in _pthread_start
	 * #19	0x91fdb012 in thread_start
	 * 
	 * The free'er would like to free some memory but cannot becuase it requires
	 * an open table handle, but the pool is locked by the user thread which
	 * wants to drop the table.
	 *
	 * // Free'er wants to get an open table, but the pool is locked (by the renamer)
	 * while (table_pool->opt_locked && !(i_am_background && table_pool->opt_flushing)) {
	 * 	xt_timed_wait_cond(self, &db->db_ot_pool.opt_cond, &db->db_ot_pool.opt_lock, 2000);
	 * 	if (!(table_pool = db_get_open_table_pool(db, tab_id)))
	 * 		xt_throw(self);
	 * }
	 * #0	0x91fb146e in __semwait_signal
	 * #1	0x91fdc3e6 in _pthread_cond_wait
	 * #2	0x920019f8 in pthread_cond_timedwait$UNIX2003
	 * #3	0x00e2a5b6 in xt_p_cond_timedwait at pthread_xt.cc:697
	 * #4	0x00e51543 in xt_timed_wait_cond at thread_xt.cc:2053
	 * #5	0x00de8d8a in xt_db_open_pool_table at database_xt.cc:1091
	 * #6	0x00e39ce3 in tabc_get_table at tabcache_xt.cc:983
	 * #7	0x00e39deb in tabc_free_page at tabcache_xt.cc:1028
	 * #8	0x00e3a5d2 in tabc_fr_main at tabcache_xt.cc:1227
	 * #9	0x00e3a954 in tabc_fr_run_thread at tabcache_xt.cc:1290
	 * #10	0x00e50640 in thr_main at thread_xt.cc:1081
	 * #11	0x91fdb155 in _pthread_start
	 * #12	0x91fdb012 in thread_start
	 *
	 * My proposed solution:
	 *
	 * Firstly, It can be assumed that background (system processes) will eventually
	 * stop activity, once all work has been done.
	 * So allowing them to proceed, even when the table is locked will
	 * not lead to a livelock.
	 *
	 * This means that we should allow background processes to proceed with a
	 * locked table, as long as the locker is waiting.
	 */
	while (table_pool->opt_locked) {
		if (i_am_background && table_pool->opt_locked != XT_TABLE_LOCKED) {
			/* Background processes can proceed, if the locker is not in the
			 * final lock phase!
			 */
			break;
		}
		xt_timed_wait_cond(self, &db->db_ot_pool.opt_cond, &db->db_ot_pool.opt_lock, 2000);
		if (!(table_pool = db_get_open_table_pool(db, tab_id)))
			xt_throw(self);
	}

	/* Moved from above, because db_get_open_table_pool() may return a different
	 * pool on each call!
	*/
	pushr_(db_free_open_table_pool, table_pool);	
	
	if ((ot = table_pool->opt_free_list)) {
		/* Remove from the free list: */
		table_pool->opt_free_list = ot->ot_otp_next_free;
		
		/* Remove from MRU list: */
		if (db->db_ot_pool.otp_lr_used == ot)
			db->db_ot_pool.otp_lr_used = ot->ot_otp_mr_used;
		if (db->db_ot_pool.otp_mr_used == ot)
			db->db_ot_pool.otp_mr_used = ot->ot_otp_lr_used;
		if (ot->ot_otp_lr_used)
			ot->ot_otp_lr_used->ot_otp_mr_used = ot->ot_otp_mr_used;
		if (ot->ot_otp_mr_used)
			ot->ot_otp_mr_used->ot_otp_lr_used = ot->ot_otp_lr_used;

		if (db->db_ot_pool.otp_lr_used)
			db->db_ot_pool.otp_free_time = db->db_ot_pool.otp_lr_used->ot_otp_free_time;

		ASSERT(db->db_ot_pool.otp_total_free > 0);
		db->db_ot_pool.otp_total_free--;

		freer_(); // db_free_open_table_pool(table_pool)
		freer_(); // xt_unlock_mutex(&db->db_ot_pool.opt_lock)
		ot->ot_thread = self;
		return ot;
	}

	if (!(tab = xt_use_table_by_id(self, db, tab_id, result))) {
		/* The table no longer exists, ignore the change: */
		freer_(); // db_free_open_table_pool(table_pool)
		freer_(); // xt_unlock_mutex(&db->db_ot_pool.opt_lock)
		return NULL;
	}

	/* xt_use_table_by_id returns a referenced tab! */
	pushr_(xt_heap_release, tab);
	if ((ot = xt_open_table(tab))) {
		ot->ot_thread = self;
		table_pool->opt_total_open++;
	}
	freer_(); // xt_release_heap(tab)

	freer_(); // db_free_open_table_pool(table_pool)
	freer_(); // xt_unlock_mutex(&db->db_ot_pool.opt_lock)
	return ot;
}

xtPublic void xt_db_return_table_to_pool(XTThreadPtr XT_UNUSED(self), XTOpenTablePtr ot)
{
	xt_db_return_table_to_pool_ns(ot);
}

xtPublic void xt_db_return_table_to_pool_ns(XTOpenTablePtr ot)
{
	XTOpenTablePoolPtr	table_pool;
	XTDatabaseHPtr		db = ot->ot_table->tab_db;
	xtBool				flush_table = TRUE;

	/* No open table returned to the pool should still
	 * have a cache handle!
	 */
	ASSERT_NS(!ot->ot_ind_rhandle);
	xt_lock_mutex_ns(&db->db_ot_pool.opt_lock);

	if (!(table_pool = db_get_open_table_pool(db, ot->ot_table->tab_id)))
		goto failed;

	if (table_pool->opt_locked) {
		/* Table will be closed below, because the table is
		 * locked: */
		if (table_pool->opt_total_open > 1)
			flush_table = FALSE;
	}
	else {
		/* Put it on the free list: */
		db->db_ot_pool.otp_total_free++;

		ot->ot_otp_next_free = table_pool->opt_free_list;
		table_pool->opt_free_list = ot;

		/* This is the time the table was freed: */
		ot->ot_otp_free_time = xt_db_approximate_time;

		/* Add to most recently used: */
		if ((ot->ot_otp_lr_used = db->db_ot_pool.otp_mr_used))
			db->db_ot_pool.otp_mr_used->ot_otp_mr_used = ot;
		ot->ot_otp_mr_used = NULL;
		db->db_ot_pool.otp_mr_used = ot;
		if (!db->db_ot_pool.otp_lr_used) {
			db->db_ot_pool.otp_lr_used = ot;
			db->db_ot_pool.otp_free_time = ot->ot_otp_free_time;
		}

		ot = NULL;
	}

	if (ot) {
		xt_unlock_mutex_ns(&db->db_ot_pool.opt_lock);
		xt_close_table(ot, flush_table, FALSE);

		/* assume that table_pool cannot be invalidated in between as we have table_pool->opt_total_open > 0 */
		xt_lock_mutex_ns(&db->db_ot_pool.opt_lock);
		table_pool->opt_total_open--;
	}

	db_free_open_table_pool(NULL, table_pool);

	if (!xt_broadcast_cond_ns(&db->db_ot_pool.opt_cond))
		goto failed;
	xt_unlock_mutex_ns(&db->db_ot_pool.opt_lock);
	
	return;

	failed:
	xt_unlock_mutex_ns(&db->db_ot_pool.opt_lock);
	if (ot)
		xt_close_table(ot, TRUE, FALSE);
	xt_log_and_clear_exception_ns();
}

//#define TEST_FREE_OPEN_TABLES

#ifdef DEBUG
#undef XT_OPEN_TABLE_FREE_TIME
#define XT_OPEN_TABLE_FREE_TIME			5
#endif

xtPublic void xt_db_free_unused_open_tables(XTThreadPtr self, XTDatabaseHPtr db)
{
	XTOpenTablePoolPtr	table_pool;
	size_t				count;
	XTOpenTablePtr		ot;
	xtBool				flush_table = TRUE;
	u_int				table_count;

	/* A quick check of the oldest free table: */
	if (xt_db_approximate_time < db->db_ot_pool.otp_free_time + XT_OPEN_TABLE_FREE_TIME)
		return;

	table_count = db->db_table_by_id ? xt_sl_get_size(db->db_table_by_id) : 0;
	count = table_count * 3;
	if (count < 20)
		count = 20;
#ifdef TEST_FREE_OPEN_TABLES
	count = 10;
#endif
	if (db->db_ot_pool.otp_total_free > count) {
		XTOpenTablePtr	ptr, pptr;

		count = table_count * 2;
		if (count < 10)
			count = 10;
#ifdef TEST_FREE_OPEN_TABLES
		count = 5;
#endif
		xt_lock_mutex(self, &db->db_ot_pool.opt_lock);
		pushr_(xt_unlock_mutex, &db->db_ot_pool.opt_lock);

		while (db->db_ot_pool.otp_total_free > count) {
			ASSERT_NS(db->db_ot_pool.otp_lr_used);
			if (!(ot = db->db_ot_pool.otp_lr_used))
				break;

			/* Check how long the open table has been free: */
			if (xt_db_approximate_time < ot->ot_otp_free_time + XT_OPEN_TABLE_FREE_TIME)
				break;

			ot->ot_thread = self;

			/* Remove from MRU list: */
			db->db_ot_pool.otp_lr_used = ot->ot_otp_mr_used;
			if (db->db_ot_pool.otp_mr_used == ot)
				db->db_ot_pool.otp_mr_used = ot->ot_otp_lr_used;
			if (ot->ot_otp_lr_used)
				ot->ot_otp_lr_used->ot_otp_mr_used = ot->ot_otp_mr_used;
			if (ot->ot_otp_mr_used)
				ot->ot_otp_mr_used->ot_otp_lr_used = ot->ot_otp_lr_used;

			if (db->db_ot_pool.otp_lr_used)
				db->db_ot_pool.otp_free_time = db->db_ot_pool.otp_lr_used->ot_otp_free_time;

			ASSERT(db->db_ot_pool.otp_total_free > 0);
			db->db_ot_pool.otp_total_free--;

			if (!(table_pool = db_get_open_table_pool(db, ot->ot_table->tab_id)))
				xt_throw(self);

			/* Find the open table in the table pool,
			 * and remove it from the list:
			 */
			pptr = NULL;
			ptr = table_pool->opt_free_list;
			while (ptr) {
				if (ptr == ot)
					break;
				pptr = ptr;
				ptr = ptr->ot_otp_next_free;
			}

			ASSERT_NS(ptr == ot);
			if (ptr == ot) {
				if (pptr)
					pptr->ot_otp_next_free = ot->ot_otp_next_free;
				else
					table_pool->opt_free_list = ot->ot_otp_next_free;
			}

			ASSERT_NS(table_pool->opt_total_open > 0);
			table_pool->opt_total_open--;
			if (table_pool->opt_total_open > 0)
				flush_table = FALSE;
			else
				flush_table = TRUE;

			db_free_open_table_pool(self, table_pool);

			freer_();

			/* Close the table, but not
			 * while holding the lock.
			 */
			xt_close_table(ot, flush_table, FALSE);

			xt_lock_mutex(self, &db->db_ot_pool.opt_lock);
			pushr_(xt_unlock_mutex, &db->db_ot_pool.opt_lock);
		}

		freer_();
	}
}

/*
 * -----------------------------------------------------------------------
 * THE THREAD POOL
 */

static void db_notify_all(XTTask *tk)
{
	XTThreadPtr target;

	for (u_int i=0; ; i++) {
		if (!(target = (XTThreadPtr) tk->tk_notify_threads.pl_get_pointer(i)))
			break;

		/* The task is on the thread's to-do list: */
		xt_lock_thread(target);
		if (target->st_tasks_todo.pl_remove_pointer(tk)) {
			/* Notify the thread that there are no more tasks to do... */
			if (target->st_tasks_todo.pl_is_empty())
				xt_signal_thread(target);
			/* Release tasks that were on the to-do list: */
			tk->tk_release();
		}
		xt_unlock_thread(target);
	}
	tk->tk_notify_threads.pl_clear();
}

static void db_thread_pool_main(XTThreadPtr self)
{
	XTDatabaseHPtr	db = self->st_database;
	XTTask			*tk;
	XTThreadPtr		target;
	xtBool			job_done = FALSE;

	for (;;) {
		xt_lock_mutex(self, &db->db_pool_lock);
		pushr_(xt_unlock_mutex, &db->db_pool_lock);

		if (job_done) {
			db->db_pool_job_count--;
			job_done = FALSE;
		}
		
		if (self->t_quit) {
			freer_();
			break;
		}

		while (!self->t_quit && !(tk = db->db_task_queue_front)) {
			/* Wait for 1/10 second (to ensure we quit on time): */
			xt_timed_wait_cond(self, &db->db_pool_cond, &db->db_pool_lock, 100);
		}

		if (self->t_quit) {
			freer_();
			break;
		}

		db->db_task_queue_front = tk->tk_task_list_next;
		if (!db->db_task_queue_front)
			db->db_task_queue_back = NULL;

		freer_();

		/* Perform the task: */
		job_done = TRUE;
		if (!tk->tk_task(self)) {
			/* Transfer error to the task: */
			tk->tk_success = false;
			if ((tk->tk_exception = (XTExceptionPtr) xt_malloc_ns(sizeof(XTExceptionRec))))
				*tk->tk_exception = self->t_exception;
			else
				tk->tk_out_of_memory = true;
		}
		else
			tk->tk_success = true;

		tk->tk_lock();
		tk->tk_running = FALSE;

		/* Notify any there were forgotten: */
		db_notify_all(tk);

		/* The task is done: */
		if (tk->tk_waiting_threads.pl_is_empty()) {
			/* No waiting tasks, log the error: */
			if (!tk->tk_success)
				xt_log_and_clear_exception(self);
		}
		else {
			for (u_int i=0; ; i++) {
				if (!(target = (XTThreadPtr) tk->tk_waiting_threads.pl_get_pointer(i)))
					break;

				/* The task is on the thread's to-do list: */
				xt_lock_thread(target);
				if (target->st_tasks_todo.pl_remove_pointer(tk)) {
					/* Add to the done list: */
					if (!target->st_tasks_done.pl_add_pointer(tk)) {
						tk->tk_release();
						xt_log_and_clear_exception(self);
					}

					/* Notify the thread that there are no more tasks to do... */
					if (target->st_tasks_todo.pl_is_empty())
						xt_signal_thread(target);
				}
				xt_unlock_thread(target);
			}
			tk->tk_waiting_threads.pl_clear();
		}

		tk->tk_unlock();
		/* We assume the reference is required to take the lock!
		 */
		tk->tk_release();
	}
}

typedef struct DBThreadData {
	XTDatabaseHPtr	td_db;
} DBThreadDataRec, *DBThreadDataPtr;

static void *db_thread_pool_run_thread(XTThreadPtr self)
{
	DBThreadDataPtr	td = (DBThreadDataPtr) self->t_data;
	XTDatabaseHPtr	db = td->td_db;
	int				i = 0;

	/* Note, the MySQL thread will be free when the this
	 * thread quits.
	 */
	if (!myxt_create_thread())
		xt_throw(self);

	while (!self->t_quit && i<10) {
		try_(a) {
			/* Use the database: */
			xt_use_database(self, db, XT_FOR_POOL);

			/* {BACKGROUND-RELEASE-DB} */
			xt_heap_release(self, self->st_database);

			db_thread_pool_main(self);
		}
		catch_(a) {
			/* This error is "normal"! */
			if (self->t_exception.e_xt_err != XT_ERR_NO_DICTIONARY &&
				!(self->t_exception.e_xt_err == XT_SIGNAL_CAUGHT &&
				self->t_exception.e_sys_err == SIGTERM))
				xt_log_and_clear_exception(self);
		}
		cont_(a);

		/* Avoid releasing the database (done above) */
		self->st_database = NULL;
		xt_unuse_database(self, self);

		if (self->t_quit)
			break;

		/* Pause, in case the error repeats: */
		xt_sleep_milli_second(10000);
		i++;
	}

	return NULL;
}

static void db_free_pool_thread(XTThreadPtr self, void *data)
{
	DBThreadDataPtr		td = (DBThreadDataPtr) data;
	XTDatabaseHPtr		db = td->td_db;
	XTThreadPtr			thread, pthread;

	xt_free(self, data);

	xt_lock_mutex(self, &db->db_pool_lock);
	pushr_(xt_unlock_mutex, &db->db_pool_lock);

	/* Remove the thread from the pool: */
	pthread = NULL;
	thread = db->db_thread_pool;
	while (thread != self) {
		pthread = thread;
		thread = thread->st_pool_next;
	}
	if (thread == self) {
		db->db_pool_thread_count--;
		if (pthread)
			pthread->st_pool_next = thread->st_pool_next;
		else
			db->db_thread_pool = thread->st_pool_next;
	}

	freer_(); // xt_unlock_mutex(&db->db_pool_lock)
}

static XTThreadPtr db_create_pool_thread(XTDatabaseHPtr db)
{
	DBThreadDataPtr	td;
	char			name[PATH_MAX];
	XTThreadPtr		thread;


	/* Note, this is just a test to see if we can create a MySQL thread.
	 * On shutdown, this is sometimes not possible!
	 */
	if (!myxt_create_thread_possible())
		return NULL;

	if (!(td = (DBThreadDataPtr) xt_malloc_ns(sizeof(DBThreadDataRec))))
		return NULL;
	td->td_db = db;

	db->db_pool_thread_count++;
	sprintf(name, "I/O-%s", xt_last_directory_of_path(db->db_main_path));
	xt_remove_dir_char(name);
	xt_strcat(PATH_MAX, name, "-");
	xt_strcati(PATH_MAX, name, db->db_pool_thread_count);
	if (!(thread = xt_create_daemon_ns(name)))
		goto failed;

	thread->st_pool_next = db->db_thread_pool;
	db->db_thread_pool = thread;

	xt_set_thread_data(thread, td, db_free_pool_thread);

	return thread;

	failed:
	xt_free_ns(td);
	return NULL;
}

/*
 * Create a pool thread.
 *
 * This ensures that there is at least one pool thread.
 *
 * The advantage is that if, for some reason, no more threads
 * can be created, then we have one that can do all the work.
 */
static void db_create_pool_thread(XTThreadPtr self, XTDatabaseHPtr db)
{
	XTThreadPtr	pool_thread = NULL;

	if (!(pool_thread = db_create_pool_thread(db)))
		xt_throw(self);

	xt_run_thread(self, pool_thread, db_thread_pool_run_thread);
}

xtPublic void xt_db_thread_pool_init(XTThreadPtr self, XTDatabaseHPtr db)
{
	xt_init_mutex_with_autoname(self, &db->db_pool_lock);
	xt_init_cond(self, &db->db_pool_cond);
	db_create_pool_thread(self, db);
}

xtPublic void xt_db_thread_pool_exit(XTThreadPtr self, XTDatabaseHPtr db)
{
	xt_db_stop_pool_threads(self, db);
	xt_free_mutex(&db->db_pool_lock);
	xt_free_cond(&db->db_pool_cond);
}

xtPublic void xt_db_stop_pool_threads(XTThreadPtr self, XTDatabaseHPtr db)
{
	XTThreadPtr	thread;
	xtThreadID	tid;

	if (db->db_thread_pool) {
		xt_lock_mutex(self, &db->db_pool_lock);
		pushr_(xt_unlock_mutex, &db->db_pool_lock);

		while ((thread = db->db_thread_pool)) {
			tid = thread->t_id;

			xt_terminate_thread(self, thread);
			xt_broadcast_cond(self, &db->db_pool_cond);

			freer_(); // xt_unlock_mutex(&db->db_pool_lock)

			xt_wait_for_thread_to_exit(tid, FALSE);

			xt_lock_mutex(self, &db->db_pool_lock);
			pushr_(xt_unlock_mutex, &db->db_pool_lock);
		}

		freer_(); // xt_unlock_mutex(&db->db_pool_lock)
	}
}

/*
 * notify_complete means the thread will wait for the task to complete, and process the result.
 * notify_early means the thread will wait for a notification from the task itself.
 *
 * Note. we assume the caller has a reference to the task. The caller is also responsible for
 * freeing the reference!
 *
 * This function will take extra references as required!
 */
xtPublic xtBool xt_run_async_task(XTTask *tk, xtBool notify_complete, xtBool notify_early, XTThreadPtr thread, XTDatabaseHPtr db)
{
	tk->tk_lock();
	if (notify_complete || notify_early) {
		xt_lock_thread(thread);
		/* Count the reference is the to-do list of the thread. */
		tk->tk_reference();
		if (!thread->st_tasks_todo.pl_add_pointer(tk)) {
			tk->tk_release();
			xt_unlock_thread(thread);
			goto failed_0;
		}
		xt_unlock_thread(thread);
		
		if (notify_complete) {
			if (!tk->tk_waiting_threads.pl_add_pointer(thread))
				goto failed_1;
		}
		else {
			if (!tk->tk_notify_threads.pl_add_pointer(thread))
				goto failed_1;
		}
	}

	if (!tk->tk_running) {
		XTThreadPtr	pool_thread = NULL;

		/* Note, the caller should already have a reference,
		 * otherwise it would not be safe to access the task.
		 *
		 * When running, we add one more reference. The reference
		 * is owned by the running thread.
		 *
		 * The reference will be released, when the task exection
		 * is complete.
		 */
		tk->tk_reference();
		tk->tk_running = TRUE;
		xt_lock_mutex_ns(&db->db_pool_lock);

		/* Check if we need to start a new thread: */
		db->db_pool_job_count++;
		if (db->db_pool_job_count > db->db_pool_thread_count && db->db_pool_thread_count < XT_ASYNC_THREAD_COUNT) {
			if (!(pool_thread = db_create_pool_thread(db))) {
				if (thread->t_exception.e_xt_err == XT_ERR_MYSQL_NO_THREAD ||
					thread->t_exception.e_xt_err == XT_ERR_MYSQL_ERROR) {
					/* We can ignore this error if error:
					 * XT_ERR_MYSQL_NO_THREAD can occur on shutdown. If we already have
					 * pool threads, then this is no problem!
					 */
					if (db->db_pool_thread_count > 0)
						goto ignore_create_thread_error;
				}
				xt_unlock_mutex_ns(&db->db_pool_lock);
				goto failed_2;
			}
		}

		ignore_create_thread_error:
		if (db->db_task_queue_back)
			db->db_task_queue_back->tk_task_list_next = tk;
		else
			db->db_task_queue_front = tk;
		tk->tk_task_list_next = NULL;
		db->db_task_queue_back = tk;
		xt_signal_cond(NULL, &db->db_pool_cond);

		xt_unlock_mutex_ns(&db->db_pool_lock);

		if (pool_thread) {
			if (!xt_run_thread_ns(pool_thread, db_thread_pool_run_thread))
				goto failed_2;
		}
	}

	tk->tk_unlock();
	
	return OK;

	failed_2:
	if (notify_complete)
		tk->tk_waiting_threads.pl_remove_pointer(thread);
	else
		tk->tk_notify_threads.pl_remove_pointer(thread);

	failed_1:
	xt_lock_thread(thread);
	if (thread->st_tasks_todo.pl_remove_pointer(tk))
		tk->tk_release();
	xt_unlock_thread(thread);

	failed_0:
	tk->tk_unlock();
	tk->tk_release();
	return FAILED;
}

xtPublic void xt_wait_for_async_tasks(XTThreadPtr thread)
{
	if (!thread->st_tasks_todo.pl_is_empty()) {
		xt_lock_thread(thread);
		while (!thread->st_tasks_todo.pl_is_empty()) {
			xt_timed_wait_thread(thread, 100);
#ifdef DEBUG
			XTTask *tk;

			for (int i=0; i++; ) {
				if (!(tk = (XTTask *) thread->st_tasks_todo.pl_get_pointer(i)))
					break;
				ASSERT_NS(tk->tk_running);
			}
#endif
		}
		xt_unlock_thread(thread);
	}
}

xtPublic xtBool xt_wait_for_async_task_results(XTThreadPtr thread)
{
	XTTask *tk;
	xtBool ok = TRUE;

	/* Wait for the task to finish: */
	xt_wait_for_async_tasks(thread);

	/* Collect the results: */
	while ((tk = xt_get_task_result(thread))) {
		if (!tk->tk_success) {
			XTThreadPtr self = xt_get_self();

			if (ok) {
				/* Transfer the first error to this thread... */
				if (tk->tk_exception)
					self->t_exception = *tk->tk_exception;
				else
					xt_register_errno(XT_REG_CONTEXT, ENOMEM);
				ok = FALSE;
			}
			else {
				/* Log all other errors: */
				if (tk->tk_exception)
					xt_log_exception(self, tk->tk_exception, XT_LOG_ERROR);
			}
		}

		tk->tk_release();
	}
	
	return ok;
}

/* After waiting, call this function to collect the results.
 * NOTE: Returns a references task!
 */
xtPublic XTTask *xt_get_task_result(XTThreadPtr thread)
{
	XTTask *tk;

	xt_lock_thread(thread);
	if ((tk = (XTTask *) thread->st_tasks_done.pl_get_pointer(thread->st_tasks_done.pl_size() - 1)))
		thread->st_tasks_done.pl_remove_pointer(tk);
	xt_unlock_thread(thread);
	return tk;
}

/*
 * This function will wake up a waiting thread so that the 
 * task continues without a waiting thread.
 */
xtPublic void xt_async_task_notify(XTTask *tk)
{
	if (!tk->tk_notify_threads.pl_is_empty()) {
		tk->tk_lock();
		db_notify_all(tk);
		tk->tk_unlock();
	}
}

class XTTestTask : public XTTask {
	public:
	XTTestTask() : XTTask(),
		tt_sleep_time(3),
		tt_ref_count(0)
	{ }

	virtual void	tk_delete() { delete this; }
	virtual xtBool	tk_task(XTThreadPtr thread);

	int				tt_sleep_time;
	int				tt_ref_count;

	virtual void	tk_reference();
	virtual void	tk_release();
};

xtBool XTTestTask::tk_task(XTThreadPtr thread)
{
	sleep(tt_sleep_time);
	return OK;
}

static void db_multi_async_test(XTThreadPtr self, int count)
{
	XTTestTask *tt;

	if (!self->st_database) {
		xt_logf(XT_NT_WARNING, "Open database required to run this test\n");
		return;
	}

	for (int i=0; i<count; i++) {
		if (!(tt = new XTTestTask()))
			xt_throw_errno(XT_CONTEXT, ENOMEM);

		tt->tt_sleep_time = count;

		/* Run the task: */
		tt->tk_reference();
		if (!xt_run_async_task(tt, TRUE, FALSE, self, self->st_database)) {
			tt->tk_release();
			xt_throw(self);
		}
		tt->tk_release();
	}

	/* Wait for the task to finish: */
	xt_wait_for_async_tasks(self);

	/* Collect the results: */
	while ((tt = (XTTestTask *) xt_get_task_result(self)))
		tt->tk_release();
}

void XTTestTask::tk_reference()
{
	tt_ref_count++;
}

void XTTestTask::tk_release()
{
	tt_ref_count--;
	if (!tt_ref_count)
		delete this;
}

xtPublic void xt_unit_test_async_task(XTThreadPtr self)
{
	db_multi_async_test(self, 1);
	//db_multi_async_test(self, 10, true);
	//db_multi_async_test(self, 5);
}

