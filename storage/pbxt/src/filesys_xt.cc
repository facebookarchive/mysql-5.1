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
 * 2005-01-12	Paul McCullagh
 *
 * H&G2JCtL
 */

#include "xt_config.h"

#ifndef XT_WIN
#include <unistd.h>
#include <dirent.h>
#include <sys/mman.h>
#endif
#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/types.h>
#include <ctype.h>
#include <string.h>
#include <errno.h>

#include "strutil_xt.h"
#include "pthread_xt.h"
#include "thread_xt.h"
#include "filesys_xt.h"
#include "memory_xt.h"
#include "cache_xt.h"
#include "sortedlist_xt.h"
#include "trace_xt.h"

#ifdef DEBUG
//#define DEBUG_TRACE_IO
//#define DEBUG_TRACE_AROUND
//#define DEBUG_TRACE_MAP_IO
//#define DEBUG_TRACE_FILES
//#define INJECT_WRITE_REMAP_ERROR
/* This is required to make testing on the Mac faster: */
/* It turns of full file sync. */
#define DEBUG_FAST_MAC
#endif

#if defined(DEBUG_TRACE_AROUND) || defined(DEBUG_TRACE_FILES)
//#define PRINTF		xt_ftracef
//#define PRINTF		xt_trace
#define PRINTF		printf
#endif

#ifdef INJECT_WRITE_REMAP_ERROR
#define INJECT_REMAP_FILE_SIZE			1000000
#define INJECT_REMAP_FILE_TYPE			"xtd"
#endif

#ifdef INJECT_FLUSH_FILE_ERROR
#define INJECT_FLUSH_FILE_SIZE			10000000
#define INJECT_FLUSH_FILE_TYPE			"dlog"
#endif

#ifdef INJECT_WRITE_FILE_ERROR
#define INJECT_WRITE_FILE_SIZE			10000000
#define INJECT_WRITE_FILE_TYPE			"dlog"
#define INJECT_ONCE_OFF
static xtBool	error_returned;
#endif

static off_t fs_seek_eof(XTThreadPtr self, XT_FD fd, XTFilePtr file);
static xtBool fs_map_file(XTFileMemMapPtr mm, XTFilePtr file, xtBool grow);
static xtBool fs_remap_file(XTOpenFilePtr map, off_t offset, size_t size, XTIOStatsPtr stat);
static xtBool fs_delete_heap_file(XTThreadPtr self, char *file_name);
static void fs_delete_all_heap_files(XTThreadPtr self);
static XTFileType fs_heap_file_exists(char *file_name, XTThreadPtr thread);
static xtBool fs_rename_heap_file(XTThreadPtr self, xtBool *all_ok, char *from_name, char *to_name);

/* ----------------------------------------------------------------------
 * Globals
 */

typedef struct FsGlobals {
	xt_mutex_type		*fsg_lock;						/* The xtPublic cache lock. */
	u_int				fsg_current_id;
	XTSortedListPtr		fsg_open_files;
} FsGlobalsRec;

static FsGlobalsRec	fs_globals;

#ifdef XT_WIN
static int fs_get_win_error()
{
	return (int) GetLastError();
}

xtPublic void xt_get_win_message(char *buffer, size_t size, int err)
{
	FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM, NULL, err,
		MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
        buffer,
        size, NULL);
}
#endif

/* ----------------------------------------------------------------------
 * REWRITE UTLITIES
 */

static int rewrite_opt_comp(void *k, void *r)
{
	register off_t				*key = (off_t *) k;
	register RewriteBlockPtr	rec = (RewriteBlockPtr) r;

	if (*key == rec->rb_offset)
		return 0;
	if (*key < rec->rb_offset)
		return -1;
	return 1;
}

static void *fs_rewrite_bsearch(void *key, register const void *base, size_t count, size_t size, size_t *idx, int (*compare)(void *key, void *rec))
{
	register size_t		i;
	register size_t		guess;
	register int		r;

	i = 0;
	while (i < count) {
		guess = (i + count - 1) >> 1;
		r = compare(key, ((char *) base) + guess * size);
		if (r == 0) {
			*idx = guess;
			return ((char *) base) + guess * size;
		}
		if (r < 0)
			count = guess;
		else
			i = guess + 1;
	}

	*idx = i;
	return NULL;
}

/* ----------------------------------------------------------------------
 * Open file list
 */

static XTFilePtr fs_new_file(char *file, XTFileType type)
{
	XTFilePtr file_ptr;

	if (!(file_ptr = (XTFilePtr) xt_calloc_ns(sizeof(XTFileRec))))
		return NULL;

	file_ptr->fil_type = type;
	if (!(file_ptr->fil_path = xt_dup_string(NULL, file))) {
		xt_free_ns(file_ptr);
		return NULL;
	}

	switch (file_ptr->fil_type) {
		case XT_FT_NONE:
			break;
		case XT_FT_REWRITE_FLUSH:
			if (!(file_ptr->x.fil_rewrite = (XTRewriteFlushPtr) xt_calloc_ns(sizeof(XTRewriteFlushRec)))) {
				xt_free_ns(file_ptr->fil_path);
				xt_free_ns(file_ptr);
			}
			xt_spinlock_init_with_autoname(NULL, &file_ptr->x.fil_rewrite->rf_lock);
			RR_FLUSH_INIT_LOCK(NULL, &file_ptr->x.fil_rewrite->rf_write_lock);
			xt_init_mutex_with_autoname(NULL, &file_ptr->x.fil_rewrite->rf_flush_lock);
			file_ptr->x.fil_rewrite->rf_flush_offset_lo = 0xFFFFFFFF;
			file_ptr->x.fil_rewrite->rf_flush_offset_hi = 0xFFFFFFFF;
			break;
		case XT_FT_STANDARD:
		case XT_FT_MEM_MAP:
		case XT_FT_HEAP:
			break;
	}
	
	file_ptr->fil_id = fs_globals.fsg_current_id++;
#ifdef DEBUG_TRACE_FILES
	PRINTF("%s: allocated file: (%d) %s\n", xt_get_self()->t_name, (int) file_ptr->fil_id, xt_last_2_names_of_path(file_ptr->fil_path));
#endif
	if (!fs_globals.fsg_current_id)
		fs_globals.fsg_current_id++;
	file_ptr->fil_filedes = XT_NULL_FD;
	file_ptr->fil_handle_count = 0;

	return file_ptr;
}

static void fs_close_fmap(XTThreadPtr self, XTFileMemMapPtr mm)
{
#ifdef XT_WIN
	if (mm->mm_start) {
		FlushViewOfFile(mm->mm_start, 0);
		UnmapViewOfFile(mm->mm_start);
		mm->mm_start = NULL;
	}
	if (mm->mm_mapdes != NULL) {
		CloseHandle(mm->mm_mapdes);
		mm->mm_mapdes = NULL;
	}
#else
	if (mm->mm_start) {
		msync( (char *)mm->mm_start, (size_t) mm->mm_length, MS_SYNC);
		munmap((caddr_t) mm->mm_start, (size_t) mm->mm_length);
		mm->mm_start = NULL;
	}
#endif
	FILE_MAP_FREE_LOCK(self, &mm->mm_lock);
	xt_free(self, mm);
}

static void fs_close_heap(XTThreadPtr self, XTFileHeapPtr fh)
{
	if (fh->fh_start) {
		xt_free(self, fh->fh_start);
		fh->fh_start = NULL;
		fh->fh_length = 0;
	}
	FILE_MAP_FREE_LOCK(self, &fh->fh_lock);
	xt_free(self, fh);
}

static void fs_free_file(XTThreadPtr self, void *XT_UNUSED(thunk), void *item)
{
	XTFilePtr	file_ptr = *((XTFilePtr *) item);

	if (file_ptr->fil_filedes != XT_NULL_FD) {
#ifdef DEBUG_TRACE_FILES
		PRINTF("%s: close file: (%d) %s\n", self->t_name, (int) file_ptr->fil_id, xt_last_2_names_of_path(file_ptr->fil_path));
#endif
#ifdef XT_WIN
		CloseHandle(file_ptr->fil_filedes);
#else
		close(file_ptr->fil_filedes);
#endif
		//PRINTF("close (FILE) %d %s\n", file_ptr->fil_filedes, file_ptr->fil_path);
		file_ptr->fil_filedes = XT_NULL_FD;
	}

	switch (file_ptr->fil_type) {
		case XT_FT_NONE:
			break;
		case XT_FT_REWRITE_FLUSH:
			if (file_ptr->x.fil_rewrite) {
				xt_spinlock_free(NULL, &file_ptr->x.fil_rewrite->rf_lock);
				RR_FLUSH_FREE_LOCK(NULL, &file_ptr->x.fil_rewrite->rf_write_lock);
				xt_free_mutex(&file_ptr->x.fil_rewrite->rf_flush_lock);
				xt_free(self, file_ptr->x.fil_rewrite);
				file_ptr->x.fil_rewrite = NULL;
			}
			break;
		case XT_FT_STANDARD:
			break;
		case XT_FT_MEM_MAP:
			break;
		case XT_FT_HEAP:
			if (file_ptr->x.fil_heap) {
				fs_close_heap(self, file_ptr->x.fil_heap);
				file_ptr->x.fil_heap = NULL;
			}
			break;
	}

#ifdef DEBUG_TRACE_FILES
	{
		XTThreadPtr thr = self;

		if (!thr)
			thr = xt_get_self();
		PRINTF("%s: free file: (%d) %s\n", thr->t_name, (int) file_ptr->fil_id, 
			file_ptr->fil_path ? xt_last_2_names_of_path(file_ptr->fil_path) : "?");
	}
#endif

	if (!file_ptr->fil_ref_count) {
		ASSERT_NS(!file_ptr->fil_handle_count);
		/* Flush any cache before this file is invalid: */
		if (file_ptr->fil_path) {
			xt_free(self, file_ptr->fil_path);
			file_ptr->fil_path = NULL;
		}

		xt_free(self, file_ptr);
	}
}

static int fs_comp_file(XTThreadPtr XT_UNUSED(self), register const void *XT_UNUSED(thunk), register const void *a, register const void *b)
{
	char		*file_name = (char *) a;
	XTFilePtr	file_ptr = *((XTFilePtr *) b);

	return strcmp(file_name, file_ptr->fil_path);
}

static int fs_comp_file_ci(XTThreadPtr XT_UNUSED(self), register const void *XT_UNUSED(thunk), register const void *a, register const void *b)
{
	char		*file_name = (char *) a;
	XTFilePtr	file_ptr = *((XTFilePtr *) b);

	return strcasecmp(file_name, file_ptr->fil_path);
}

/* ----------------------------------------------------------------------
 * init & exit
 */

xtPublic void xt_fs_init(XTThreadPtr self)
{
	fs_globals.fsg_open_files = xt_new_sortedlist(self,
		sizeof(XTFilePtr), 20, 20,
		pbxt_ignore_case ? fs_comp_file_ci : fs_comp_file,
		NULL, fs_free_file, TRUE, FALSE);
	fs_globals.fsg_lock = fs_globals.fsg_open_files->sl_lock;
	fs_globals.fsg_current_id = 1;
}

xtPublic void xt_fs_exit(XTThreadPtr self)
{
	if (fs_globals.fsg_open_files) {
		fs_delete_all_heap_files(self);
		xt_free_sortedlist(self, fs_globals.fsg_open_files);
		fs_globals.fsg_open_files = NULL;
	}
	fs_globals.fsg_lock = NULL;
	fs_globals.fsg_current_id = 0;
}

/* ----------------------------------------------------------------------
 * File operations
 */

static void fs_set_stats(XTThreadPtr self, char *path)
{
	char		super_path[PATH_MAX];
	struct stat	stats;
	char		*ptr;

	ptr = xt_last_name_of_path(path);
	if (ptr == path) 
		strcpy(super_path, ".");
	else {
		xt_strcpy(PATH_MAX, super_path, path);

		if ((ptr = xt_last_name_of_path(super_path)))
			*ptr = 0;
	}
	if (stat(super_path, &stats) == -1)
		xt_throw_ferrno(XT_CONTEXT, errno, super_path);

	if (chmod(path, stats.st_mode) == -1)
		xt_throw_ferrno(XT_CONTEXT, errno, path);

	/*chown(path, stats.st_uid, stats.st_gid);*/
}

xtPublic char *xt_file_path(XTOpenFilePtr of)
{
	return of->fr_file->fil_path;
}

static xtBool fs_exists(char *path, XTThreadPtr thread)
{
	int err;

	err = access(path, F_OK);
	if (err == -1) {
		if (fs_heap_file_exists(path, thread) == XT_FT_HEAP)
			return TRUE;
		return FALSE;
	}
	return TRUE;
}

xtBool xt_fs_exists(char *path)
{
	/*
	 * Do no use xt_get_self() because this function call is required before
	 * the self has been setup!
	 *
	 * #0  0x00002aaaab566d31 in xt_ha_thd_to_self (thd=0x11f5d7d0) at ha_pbxt.cc:669
	 * #1  0x00002aaaab58068a in myxt_get_self () at myxt_xt.cc:3211
	 * #2  0x00002aaaab5ab24b in xt_get_self () at thread_xt.cc:621
	 * #3  0x00002aaaab562d71 in fs_heap_file_exists (file_name=0x40ae88d0 "/usr/local/mysql/var/pbxt/no-debug") at filesys_xt.cc:665
	 * #4  0x00002aaaab562f52 in xt_fs_exists (path=0x40ae88d0 "/usr/local/mysql/var/pbxt/no-debug") at filesys_xt.cc:384
	 * #5  0x00002aaaab54d703 in xt_lock_installation (self=0x11fbb490, installation_path=0xd3c620 "/usr/local/mysql/var/") at database_xt.cc:107
	 * #6  0x00002aaaab5664c3 in pbxt_call_init (self=0x11fbb490) at ha_pbxt.cc:1013
	 * #7  0x00002aaaab566876 in pbxt_init (p=0x11f4b7c0) at ha_pbxt.cc:1223
	 * #8  0x00000000006a7f41 in ha_initialize_handlerton ()
	 * #9  0x000000000072bdea in plugin_initialize ()
	 * #10 0x000000000072eac6 in mysql_install_plugin ()
	 * #11 0x00000000005c39a7 in mysql_execute_command ()
	 * #12 0x00000000005cafd1 in mysql_parse ()
	 * #13 0x00000000005cb3d3 in dispatch_command ()
	 * #14 0x00000000005cc5d4 in do_command ()
	 * #15 0x00000000005bce77 in handle_one_connection ()
	 * #16 0x000000367a806367 in start_thread () from /lib64/libpthread.so.0
	 * #17 0x0000003679cd2f7d in clone () from /lib64/libc.so.6\
	 */
	return fs_exists(path, NULL);
}

/*
 * No error is generated if the file dose not exist.
 */
xtPublic xtBool xt_fs_delete(XTThreadPtr self, char *name)
{
#ifdef DEBUG_TRACE_FILES
	PRINTF("%s: DELETE FILE: %s\n", xt_get_self()->t_name, xt_last_2_names_of_path(name));
#endif
	/* {HEAP-FILE}
	 * This reference count is +1 for the file exists!
	 * It will be decremented on file delete!
	 */
	if (fs_delete_heap_file(self, name))
		return OK;

#ifdef XT_WIN
	//PRINTF("delete %s\n", name);
	if (!DeleteFile(name)) {
		int err = fs_get_win_error();

		if (!XT_FILE_NOT_FOUND(err)) {
			xt_throw_ferrno(XT_CONTEXT, err, name);
			return FAILED;
		}
	}
#else
	if (unlink(name) == -1) {
		int err = errno;

		if (err != ENOENT) {
			xt_throw_ferrno(XT_CONTEXT, err, name);
			return FAILED;
		}
	}
#endif
	return OK;
}

xtPublic xtBool xt_fs_file_not_found(int err)
{
#ifdef XT_WIN
	return XT_FILE_NOT_FOUND(err);
#else
	return err == ENOENT;
#endif
}

xtPublic void xt_fs_move(struct XTThread *self, char *from_path, char *to_path)
{
	int		err;
	xtBool	ok;

#ifdef DEBUG_TRACE_FILES
	PRINTF("%s: MOVE FILE: %s --> %s\n", xt_get_self()->t_name, xt_last_2_names_of_path(from_path), xt_last_2_names_of_path(to_path));
#endif

	if (fs_rename_heap_file(self, &ok, from_path, to_path))
		return;

#ifdef XT_WIN
	if (!MoveFile(from_path, to_path))
		xt_throw_ferrno(XT_CONTEXT, fs_get_win_error(), from_path);
#else
	if (link(from_path, to_path) == -1) {
		err = errno;
		xt_throw_ferrno(XT_CONTEXT, err, from_path);
	}

	if (unlink(from_path) == -1) {
		err = errno;
		unlink(to_path);
		xt_throw_ferrno(XT_CONTEXT, err, from_path);
	}
#endif
}

xtPublic xtBool xt_fs_rename(XTThreadPtr self, char *from_path, char *to_path)
{
	int		err;
	xtBool	ok;

#ifdef DEBUG_TRACE_FILES
	PRINTF("%s: RENAME FILE: %s --> %s\n", xt_get_self()->t_name, xt_last_2_names_of_path(from_path), xt_last_2_names_of_path(to_path));
#endif

	if (fs_rename_heap_file(self, &ok, from_path, to_path))
		return ok;

	if (rename(from_path, to_path) == -1) {
		err = errno;
		xt_throw_ferrno(XT_CONTEXT, err, from_path);
		return FAILED;
	}
	return OK;
}

xtPublic xtBool xt_fs_stat(XTThreadPtr self, char *path, off_t *size, struct timespec *mod_time)
{
#ifdef XT_WIN
	HANDLE						fh;
	BY_HANDLE_FILE_INFORMATION	info;
	SECURITY_ATTRIBUTES			sa = { sizeof(SECURITY_ATTRIBUTES), 0, 0 };

	fh = CreateFile(
		path,
		GENERIC_READ,
		FILE_SHARE_READ,
		&sa,
		OPEN_EXISTING,
		FILE_ATTRIBUTE_NORMAL,
		NULL);
	if (fh == INVALID_HANDLE_VALUE) {
		xt_throw_ferrno(XT_CONTEXT, fs_get_win_error(), path);
		return FAILED;
	}

	if (!GetFileInformationByHandle(fh, &info)) {
		CloseHandle(fh);
		xt_throw_ferrno(XT_CONTEXT, fs_get_win_error(), path);
		return FAILED;
	}

	CloseHandle(fh);
	if (size)
		*size = (off_t) info.nFileSizeLow | (((off_t) info.nFileSizeHigh) << 32);
	if (mod_time)
		mod_time->tv.ft = info.ftLastWriteTime;
#else
	struct stat sb;

	if (stat(path, &sb) == -1) {
		xt_throw_ferrno(XT_CONTEXT, errno, path);
		return FAILED;
	}
	if (size)
		*size = sb.st_size;
	if (mod_time) {
		mod_time->tv_sec = sb.st_mtime;
#ifdef XT_MAC
		/* This is the Mac OS X version: */
		mod_time->tv_nsec = sb.st_mtimespec.tv_nsec;
#else
#ifdef __USE_MISC
		/* This is the Linux version: */
		mod_time->tv_nsec = sb.st_mtim.tv_nsec;
#else
		/* Not supported? */
		mod_time->tv_nsec = 0;
#endif
#endif
	}
#endif
	return OK;
}

void xt_fs_mkdir(XTThreadPtr self, char *name)
{
	char path[PATH_MAX];

	xt_strcpy(PATH_MAX, path, name);
	xt_remove_dir_char(path);

#ifdef XT_WIN
	{
		SECURITY_ATTRIBUTES	sa = { sizeof(SECURITY_ATTRIBUTES), 0, 0 };

		if (!CreateDirectory(path, &sa))
			xt_throw_ferrno(XT_CONTEXT, fs_get_win_error(), path);
	}
#else
	if (mkdir(path, S_IRWXU | S_IRWXG | S_IRWXO) == -1)
		xt_throw_ferrno(XT_CONTEXT, errno, path);

	try_(a) {
		fs_set_stats(self, path);
	}
	catch_(a) {
		xt_fs_rmdir(NULL, name);
		throw_();
	}
	cont_(a);
#endif
}

void xt_fs_mkpath(XTThreadPtr self, char *path)
{
	char *ptr;

	if (fs_exists(path, self))
		return;

	if (!(ptr = (char *) xt_last_directory_of_path((c_char *) path)))
		return;
	if (ptr == path)
		return;
	ptr--;
	if (XT_IS_DIR_CHAR(*ptr)) {
		*ptr = 0;
		xt_fs_mkpath(self, path);
		*ptr = XT_DIR_CHAR;
		xt_fs_mkdir(self, path);
	}
}

xtBool xt_fs_rmdir(XTThreadPtr self, char *name)
{
	char path[PATH_MAX];

	xt_strcpy(PATH_MAX, path, name);
	xt_remove_dir_char(path);

#ifdef XT_WIN
	if (!RemoveDirectory(path)) {
		int err = fs_get_win_error();

		if (!XT_FILE_NOT_FOUND(err)) {
			xt_throw_ferrno(XT_CONTEXT, err, path);
			return FAILED;
		}
	}
#else
	if (rmdir(path) == -1) {
		int err = errno;

		if (err != ENOENT) {
			xt_throw_ferrno(XT_CONTEXT, err, path);
			return FAILED;
		}
	}
#endif
	return OK;
}

/* ----------------------------------------------------------------------
 * Open & Close operations
 */

xtPublic XTFilePtr xt_fs_get_file(XTThreadPtr self, char *file_name, XTFileType type)
{
	XTFilePtr	file_ptr, *file_pptr;

	xt_sl_lock(self, fs_globals.fsg_open_files);
	pushr_(xt_sl_unlock, fs_globals.fsg_open_files);

	if ((file_pptr = (XTFilePtr *) xt_sl_find(self, fs_globals.fsg_open_files, file_name)))
		file_ptr = *file_pptr;
	else {
		if (!(file_ptr = fs_new_file(file_name, type)))
			xt_throw(self);
		xt_sl_insert(self, fs_globals.fsg_open_files, file_name, &file_ptr);
	}
	file_ptr->fil_ref_count++;
	freer_(); // xt_sl_unlock(fs_globals.fsg_open_files)
	return file_ptr;
}

xtPublic void xt_fs_release_file(XTThreadPtr self, XTFilePtr file_ptr)
{
	xt_sl_lock(self, fs_globals.fsg_open_files);
	pushr_(xt_sl_unlock, fs_globals.fsg_open_files);

	file_ptr->fil_ref_count--;
	if (!file_ptr->fil_ref_count) {
		xt_sl_delete(self, fs_globals.fsg_open_files, file_ptr->fil_path);
	}

	freer_(); // xt_ht_unlock(fs_globals.fsg_open_files)
}

static XTFileType fs_heap_file_exists(char *file_name, XTThreadPtr thread)
{
	XTFilePtr	*file_pptr;
	XTFileType	type;

	xt_sl_lock_ns(fs_globals.fsg_open_files, thread);

	if ((file_pptr = (XTFilePtr *) xt_sl_find(NULL, fs_globals.fsg_open_files, file_name)))
		type = (*file_pptr)->fil_type;
	else
		type = XT_FT_NONE;

	xt_sl_unlock_ns(fs_globals.fsg_open_files);
	return type;
}

/*
 * Return TRUE if the heap file exists!
 */
static xtBool fs_rename_heap_file(struct XTThread *self, xtBool *all_ok, char *from_name, char *to_name)
{
	XTFilePtr	from_ptr, to_ptr, *file_pptr;

	xt_sl_lock_ns(fs_globals.fsg_open_files, self);

	if ((file_pptr = (XTFilePtr *) xt_sl_find(NULL, fs_globals.fsg_open_files, from_name))) {
		from_ptr = *file_pptr;
		if (from_ptr->fil_type == XT_FT_HEAP) {
			*all_ok = FALSE;

			if (from_ptr->fil_ref_count > 1) {
				xt_register_ferrno(XT_REG_CONTEXT, XT_FILE_IN_USE_ERR, from_name);
				goto file_found;
			}

			/* Add the new one: */
			if (!(to_ptr = fs_new_file(to_name, XT_FT_HEAP)))
				goto file_found;

			if (!xt_sl_insert(NULL, fs_globals.fsg_open_files, to_name, &to_ptr))
				goto file_found;

			/* Copy the data from to to: */
			to_ptr->x.fil_heap = from_ptr->x.fil_heap;
			from_ptr->x.fil_heap = NULL;
			to_ptr->fil_ref_count++;

			/* Remove the previous file: */
			from_ptr->fil_ref_count--;
			if (!from_ptr->fil_ref_count)
				xt_sl_delete(NULL, fs_globals.fsg_open_files, from_name);

			xt_sl_unlock_ns(fs_globals.fsg_open_files);
			*all_ok = TRUE;
			return TRUE;
		}
	}

	xt_sl_unlock_ns(fs_globals.fsg_open_files);
	return FALSE;

	file_found:
	xt_sl_unlock_ns(fs_globals.fsg_open_files);
	xt_throw(self);
	return TRUE;
}

/*
 * Return TRUE of this is a "heap" file.
 */
static xtBool fs_delete_heap_file(XTThreadPtr self, char *file_name)
{
	XTFilePtr	file_ptr, *file_pptr;

	xt_sl_lock_ns(fs_globals.fsg_open_files, self);

	if ((file_pptr = (XTFilePtr *) xt_sl_find(NULL, fs_globals.fsg_open_files, file_name))) {
		file_ptr = *file_pptr;
		if (file_ptr->fil_type == XT_FT_HEAP) {
			file_ptr->fil_ref_count--;
			if (!file_ptr->fil_ref_count)
				xt_sl_delete(NULL, fs_globals.fsg_open_files, file_ptr->fil_path);
			xt_sl_unlock_ns(fs_globals.fsg_open_files);
			return TRUE;
		}
	}

	xt_sl_unlock_ns(fs_globals.fsg_open_files);
	return FALSE;
}

static void fs_delete_all_heap_files(XTThreadPtr self)
{
	XTFilePtr	file_ptr, *file_pptr;
	size_t		i = 0;

	xt_sl_lock(self, fs_globals.fsg_open_files);
	pushr_(xt_sl_unlock, fs_globals.fsg_open_files);

	for (;;) {
		if (!(file_pptr = (XTFilePtr *) xt_sl_item_at(fs_globals.fsg_open_files, i)))
			break;
		file_ptr = *file_pptr;
		i++;
		if (file_ptr->fil_type == XT_FT_HEAP) {
			file_ptr->fil_ref_count--;
			if (!file_ptr->fil_ref_count) {
				xt_sl_delete(self, fs_globals.fsg_open_files, file_ptr->fil_path);
				i--;
			}
		}
	}

	freer_(); // xt_sl_unlock(fs_globals.fsg_open_files)
}

static xtBool fs_open_file(XTThreadPtr self, XT_FD *fd, XTFilePtr file, int mode)
{
	int retried = FALSE;

#ifdef DEBUG_TRACE_FILES
	PRINTF("%s: OPEN FILE: (%d) %s\n", self->t_name, (int) file->fil_id, xt_last_2_names_of_path(file->fil_path));
#endif
	retry:
#ifdef XT_WIN
	SECURITY_ATTRIBUTES	sa = { sizeof(SECURITY_ATTRIBUTES), 0, 0 };
	DWORD				flags;

	if (mode & XT_FS_EXCLUSIVE)
		flags = CREATE_NEW;
	else if (mode & XT_FS_CREATE)
		flags = OPEN_ALWAYS;
	else
		flags = OPEN_EXISTING;

	*fd = CreateFile(
		file->fil_path,
		mode & XT_FS_READONLY ? GENERIC_READ : (GENERIC_READ | GENERIC_WRITE),
		FILE_SHARE_READ | FILE_SHARE_WRITE,
		&sa,
		flags,
		FILE_FLAG_RANDOM_ACCESS,
		NULL);
	if (*fd == INVALID_HANDLE_VALUE) {
		int err = fs_get_win_error();

		if (!(mode & XT_FS_MISSING_OK) || !XT_FILE_NOT_FOUND(err)) {
			if (!retried && (mode & XT_FS_MAKE_PATH) && XT_FILE_NOT_FOUND(err)) {
				char path[PATH_MAX];

				xt_strcpy(PATH_MAX, path, file->fil_path);
				xt_remove_last_name_of_path(path);
				xt_fs_mkpath(self, path);
				retried = TRUE;
				goto retry;
			}

			xt_throw_ferrno(XT_CONTEXT, err, file->fil_path);
		}

		/* File is missing, but don't throw an error. */
		return FAILED;
	}
	//PRINTF("open %d %s\n", *fd, file->fil_path);
	return OK;
#else
	int flags = 0;

	if (mode & XT_FS_READONLY)
		flags = O_RDONLY;
	else
		flags = O_RDWR;
	if (mode & XT_FS_CREATE)
		flags |= O_CREAT;
	if (mode & XT_FS_EXCLUSIVE)
		flags |= O_EXCL;
#ifdef O_DIRECT
	if (mode & XT_FS_DIRECT_IO)
		flags |= O_DIRECT;
#endif

	*fd = open(file->fil_path, flags, XT_MASK);
	if (*fd == -1) {
		int err = errno;

		if (!(mode & XT_FS_MISSING_OK) || err != ENOENT) {
			if (!retried && (mode & XT_FS_MAKE_PATH) && err == ENOENT) {
				char path[PATH_MAX];

				xt_strcpy(PATH_MAX, path, file->fil_path);
				xt_remove_last_name_of_path(path);
				xt_fs_mkpath(self, path);
				retried = TRUE;
				goto retry;
			}

			xt_throw_ferrno(XT_CONTEXT, err, file->fil_path);
		}

		/* File is missing, but don't throw an error. */
		return FAILED;
	}
	///PRINTF("open %d %s\n", *fd, file->fil_path);
	return OK;
#endif
}

xtPublic XTOpenFilePtr xt_open_file(XTThreadPtr self, char *file, XTFileType type, int mode, size_t grow_size)
{
	XTOpenFilePtr	of;
	XTFilePtr		fp;

	pushsr_(of, xt_close_file, (XTOpenFilePtr) xt_calloc(self, sizeof(XTOpenFileRec)));
	fp = xt_fs_get_file(self, file, type);
	of->of_type = type;
	of->fr_file = fp;
	of->fr_id = fp->fil_id;

	switch (type) {
		case XT_FT_NONE:
			break;
		case XT_FT_REWRITE_FLUSH:
		case XT_FT_STANDARD:
			of->x.of_filedes = XT_NULL_FD;
#ifdef XT_WIN
			if (!fs_open_file(self, &of->of_filedes, fp, mode)) {
				xt_close_file(self, of);
				of = NULL;
			}
#else
			xtBool failed;

			failed = FALSE;
			if (fp->fil_filedes == -1) {
				xt_sl_lock(self, fs_globals.fsg_open_files);
				pushr_(xt_sl_unlock, fs_globals.fsg_open_files);
				if (fp->fil_filedes == -1) {
					if (!fs_open_file(self, &fp->fil_filedes, fp, mode))
						failed = TRUE;
				}
				freer_(); // xt_ht_unlock(fs_globals.fsg_open_files)
			}

			if (failed) {
				/* Close, but after we have release the fsg_open_files lock! */
				xt_close_file(self, of);
				of = NULL;
			}
			else
				of->x.of_filedes = fp->fil_filedes;
#endif
			break;
		case XT_FT_MEM_MAP:
			xt_sl_lock(self, fs_globals.fsg_open_files);
			pushr_(xt_sl_unlock, fs_globals.fsg_open_files);

			if (fp->fil_filedes == XT_NULL_FD) {
				if (!fs_open_file(self, &fp->fil_filedes, fp, mode)) {
					freer_(); // xt_ht_unlock(fs_globals.fsg_open_files)
					xt_close_file(self, of);
					of = NULL;
					goto exit;
				}
			}

			fp->fil_handle_count++;

			freer_(); // xt_ht_unlock(fs_globals.fsg_open_files)

			if (!fp->x.fil_memmap) {
				xt_sl_lock(self, fs_globals.fsg_open_files);
				pushr_(xt_sl_unlock, fs_globals.fsg_open_files);
				if (!fp->x.fil_memmap) {
					XTFileMemMapPtr mm;

					mm = (XTFileMemMapPtr) xt_calloc(self, sizeof(XTFileMemMapRec));
					pushr_(fs_close_fmap, mm);

#ifdef XT_WIN
					/* NULL is the value returned on error! */
					mm->mm_mapdes = NULL;
#endif
					FILE_MAP_INIT_LOCK(self, &mm->mm_lock);
					mm->mm_length = fs_seek_eof(self, fp->fil_filedes, fp);
					if (sizeof(size_t) == 4 && mm->mm_length >= (off_t) 0xFFFFFFFF)
						xt_throw_ixterr(XT_CONTEXT, XT_ERR_FILE_TOO_LONG, fp->fil_path);
					mm->mm_grow_size = grow_size;

					if (mm->mm_length < (off_t) grow_size) {
						mm->mm_length = (off_t) grow_size;
						if (!fs_map_file(mm, fp, TRUE))
							xt_throw(self);
					}
					else {
						if (!fs_map_file(mm, fp, FALSE))
							xt_throw(self);
					}

					popr_(); // Discard fs_close_fmap(mm)
					fp->x.fil_memmap = mm;
				}
				freer_(); // xt_ht_unlock(fs_globals.fsg_open_files)
			}
			of->x.mf_memmap = fp->x.fil_memmap;
			break;
		case XT_FT_HEAP:
			xt_sl_lock(self, fs_globals.fsg_open_files);
			pushr_(xt_sl_unlock, fs_globals.fsg_open_files);
#ifdef DEBUG_TRACE_FILES
			PRINTF("%s: OPEN HEAP: (%d) %s\n", self->t_name, (int) fp->fil_id, xt_last_2_names_of_path(fp->fil_path));
#endif
			if (!fp->x.fil_heap) {
				XTFileHeapPtr fh;

				if (!(mode & XT_FS_CREATE)) {
					freer_(); // xt_ht_unlock(fs_globals.fsg_open_files)
					if (!(mode & XT_FS_MISSING_OK))
						xt_throw_ferrno(XT_CONTEXT, XT_FILE_NOT_FOUND_ERR, fp->fil_path);
					xt_close_file(self, of);
					of = NULL;
					goto exit;
				}

				fh = (XTFileHeapPtr) xt_calloc(self, sizeof(XTFileHeapRec));
				pushr_(xt_free, fh);

				FILE_MAP_INIT_LOCK(self, &fh->fh_lock);
				fh->fh_grow_size = grow_size;

				popr_(); // Discard xt_free(fh)
				fp->x.fil_heap = fh;
				
				/* {HEAP-FILE}
				 * This reference count is +1 for the file exists!
				 * It will be decremented on file delete!
				 */
				fp->fil_ref_count++;
			}
			freer_(); // xt_ht_unlock(fs_globals.fsg_open_files)
			of->x.of_heap = fp->x.fil_heap;
			break;
	}

	exit:
	popr_(); // Discard xt_close_file(of)
	return of;
}

xtPublic XTOpenFilePtr xt_open_file_ns(char *file, XTFileType type, int mode, size_t grow_size)
{
	XTThreadPtr		self = xt_get_self();
	XTOpenFilePtr	of;

	try_(a) {
		of = xt_open_file(self, file, type, mode, grow_size);
	}
	catch_(a) {
		of = NULL;
	}
	cont_(a);
	return of;
}

xtPublic xtBool xt_open_file_ns(XTOpenFilePtr *fh, char *file, XTFileType type, int mode, size_t grow_size)
{
	XTThreadPtr		self = xt_get_self();
	xtBool			ok = TRUE;

	try_(a) {
		*fh = xt_open_file(self, file, type, mode, grow_size);
	}
	catch_(a) {
		ok = FALSE;
	}
	cont_(a);
	return ok;
}

xtPublic void xt_close_file(XTThreadPtr self, XTOpenFilePtr of)
{
	switch (of->of_type) {
		case XT_FT_NONE:
			break;
		case XT_FT_REWRITE_FLUSH:
		case XT_FT_STANDARD:
			if (of->x.of_filedes != XT_NULL_FD) {
#ifdef XT_WIN
				CloseHandle(of->of_filedes);
#ifdef DEBUG_TRACE_FILES
				PRINTF("%s: close file: (%d) %s\n", self->t_name, (int) of->fr_file->fil_id, xt_last_2_names_of_path(of->fr_file->fil_path));
#endif
#else
				if (!of->fr_file || of->x.of_filedes != of->fr_file->fil_filedes) {
					close(of->x.of_filedes);
#ifdef DEBUG_TRACE_FILES
					PRINTF("%s: close file: (%d) %s\n", self->t_name, (int) of->fr_file->fil_id, xt_last_2_names_of_path(of->fr_file->fil_path));
#endif
				}
#endif

				of->x.of_filedes = XT_NULL_FD;
			}

			if (of->fr_file) {
				xt_fs_release_file(self, of->fr_file);
				of->fr_file = NULL;
			}
			break;
		case XT_FT_MEM_MAP:
			ASSERT_NS(!of->mf_slock_count);
			if (of->fr_file) {
				if (of->fr_file->x.fil_memmap) {
					xt_sl_lock(self, fs_globals.fsg_open_files);
					pushr_(xt_sl_unlock, fs_globals.fsg_open_files);
					ASSERT_NS(of->fr_file->fil_handle_count > 0);		
					of->fr_file->fil_handle_count--;
					if (!of->fr_file->fil_handle_count) {
						fs_close_fmap(self, of->fr_file->x.fil_memmap);
						of->fr_file->x.fil_memmap = NULL;
					}
					freer_();
				}
				else {
					ASSERT_NS(of->fr_file->fil_handle_count == 0);		
					of->fr_file->fil_handle_count = 0;
				}
				
				xt_fs_release_file(self, of->fr_file);
				of->fr_file = NULL;
			}
			of->x.mf_memmap = NULL;
			break;
		case XT_FT_HEAP:
#ifdef DEBUG_TRACE_FILES
			PRINTF("%s: close heap: (%d) %s\n", self->t_name, (int) of->fr_file->fil_id, xt_last_2_names_of_path(of->fr_file->fil_path));
#endif
			if (of->fr_file) {
				xt_fs_release_file(self, of->fr_file);
				of->fr_file = NULL;
			}
			of->x.of_heap = NULL;
			break;
	}
	xt_free(self, of);
}

xtPublic xtBool xt_close_file_ns(XTOpenFilePtr of)
{
	XTThreadPtr self = xt_get_self();
	xtBool		failed = FALSE;

	try_(a) {
		xt_close_file(self, of);
	}
	catch_(a) {
		failed = TRUE;
	}
	cont_(a);
	return failed;
}

/* ----------------------------------------------------------------------
 * I/O operations
 */

xtPublic xtBool xt_lock_file(struct XTThread *self, XTOpenFilePtr of)
{
	if (of->of_type != XT_FT_STANDARD && of->of_type != XT_FT_REWRITE_FLUSH) {
		xt_throw_ixterr(XT_CONTEXT, XT_ERR_FILE_OP_NOT_SUPP, xt_file_path(of));
		return FAILED;
	}
#ifdef XT_WIN
	if (!LockFile(of->x.of_filedes, 0, 0, 512, 0)) {
		int err = fs_get_win_error();
		
		if (err == ERROR_LOCK_VIOLATION ||
			err == ERROR_LOCK_FAILED)
			return FAILED;
		
		xt_throw_ferrno(XT_CONTEXT, err, xt_file_path(of));
		return FAILED;
	}
	return OK;
#else
	if (lockf(of->x.of_filedes, F_TLOCK, 0) == 0)
		return OK;
	if (errno == EAGAIN)
		return FAILED;
	xt_throw_ferrno(XT_CONTEXT, errno, xt_file_path(of));
	return FAILED;
#endif
}

xtPublic void xt_unlock_file(struct XTThread *self, XTOpenFilePtr of)
{
	if (of->of_type != XT_FT_STANDARD && of->of_type != XT_FT_REWRITE_FLUSH) {
		xt_throw_ixterr(XT_CONTEXT, XT_ERR_FILE_OP_NOT_SUPP, xt_file_path(of));
		return;
	}
#ifdef XT_WIN
	if (!UnlockFile(of->x.of_filedes, 0, 0, 512, 0)) {
		int err = fs_get_win_error();
		
		if (err != ERROR_NOT_LOCKED)
			xt_throw_ferrno(XT_CONTEXT, err, xt_file_path(of));
	}
#else
	if (lockf(of->x.of_filedes, F_ULOCK, 0) == -1)
		xt_throw_ferrno(XT_CONTEXT, errno, xt_file_path(of));
#endif
}

static off_t fs_seek_eof(XTThreadPtr self, XT_FD fd, XTFilePtr file)
{
#ifdef XT_WIN
	DWORD			result;
	LARGE_INTEGER	lpFileSize;

	result = SetFilePointer(fd, 0, NULL, FILE_END);
	if (result == 0xFFFFFFFF) {
		xt_throw_ferrno(XT_CONTEXT, fs_get_win_error(), file->fil_path);
		return (off_t) -1;
	}

	if (!GetFileSizeEx(fd, &lpFileSize)) {
		xt_throw_ferrno(XT_CONTEXT, fs_get_win_error(), file->fil_path);
		return (off_t) -1;
	}

	return lpFileSize.QuadPart;
#else
	off_t off;

	off = lseek(fd, 0, SEEK_END);
	if (off == -1) {
		xt_throw_ferrno(XT_CONTEXT, errno, file->fil_path);
		return -1;
	}

     return off;
#endif
}

xtPublic off_t xt_seek_eof_file(XTThreadPtr self, XTOpenFilePtr of)
{
	switch (of->of_type) {
		case XT_FT_NONE:
			break;
		case XT_FT_REWRITE_FLUSH:
		case XT_FT_STANDARD:
			return fs_seek_eof(self, of->x.of_filedes, of->fr_file);
		case XT_FT_MEM_MAP:
			return of->x.mf_memmap->mm_length;
		case XT_FT_HEAP:
			return of->x.of_heap->fh_length;
	}
	return 0;
}

xtPublic xtBool xt_set_eof_file(XTThreadPtr self, XTOpenFilePtr of, off_t offset)
{
	switch (of->of_type) {
		case XT_FT_NONE:
			break;
		case XT_FT_REWRITE_FLUSH:
		case XT_FT_STANDARD:
#ifdef XT_WIN
			LARGE_INTEGER liDistanceToMove;
			
			liDistanceToMove.QuadPart = offset;
			if (!SetFilePointerEx(of->of_filedes, liDistanceToMove, NULL, FILE_BEGIN)) {
				xt_throw_ferrno(XT_CONTEXT, fs_get_win_error(), xt_file_path(of));
				return FAILED;
			}

			if (!SetEndOfFile(of->of_filedes)) {
				xt_throw_ferrno(XT_CONTEXT, fs_get_win_error(), xt_file_path(of));
				return FAILED;
			}
#else
			if (ftruncate(of->x.of_filedes, offset) == -1) {
				xt_throw_ferrno(XT_CONTEXT, errno, xt_file_path(of));
				return FAILED;
			}
#endif
			break;
		case XT_FT_MEM_MAP:
			xt_throw_ixterr(XT_CONTEXT, XT_ERR_FILE_OP_NOT_SUPP, xt_file_path(of));
			break;
		case XT_FT_HEAP:
			xt_throw_ixterr(XT_CONTEXT, XT_ERR_FILE_OP_NOT_SUPP, xt_file_path(of));
			break;
	}
	return OK;
}

static xtBool fs_rewrite_file(XTOpenFilePtr of, off_t offset, size_t size, void *data, XTIOStatsPtr stat, XTThreadPtr thread)
{
#ifdef XT_TIME_DISK_WRITES
	xtWord8		s;
#endif

#ifdef XT_WIN
	LARGE_INTEGER	liDistanceToMove;
	DWORD			result;
	
	liDistanceToMove.QuadPart = offset;
	if (!SetFilePointerEx(of->of_filedes, liDistanceToMove, NULL, FILE_BEGIN)) {
		xt_register_ferrno(XT_REG_CONTEXT, fs_get_win_error(), xt_file_path(of));
		return FAILED;
	}

	if (!ReadFile(of->of_filedes, data, size, &result, NULL)) {
		xt_register_ferrno(XT_REG_CONTEXT, fs_get_win_error(), xt_file_path(of));
		return FAILED;
	}

	if ((size_t) result < 4) {
		xt_register_ferrno(XT_REG_CONTEXT, ERROR_HANDLE_EOF, xt_file_path(of));
		return FAILED;
	}

	ASSERT_NS(offset + size == (offset + result + 1023) / 1024 * 1024);
	size = (size_t) result;

	stat->ts_read += (u_int) size;

#ifdef XT_TIME_DISK_WRITES
	stat->ts_write_start = xt_trace_clock();
#endif

	liDistanceToMove.QuadPart = offset;
	if (!SetFilePointerEx(of->of_filedes, liDistanceToMove, NULL, FILE_BEGIN)) {
		xt_register_ferrno(XT_REG_CONTEXT, fs_get_win_error(), xt_file_path(of));
		goto failed;
	}

	if (!WriteFile(of->of_filedes, data, size, &result, NULL)) {
		xt_register_ferrno(XT_REG_CONTEXT, fs_get_win_error(), xt_file_path(of));
		goto failed;
	}

	if (result != size) {
		xt_register_ferrno(XT_REG_CONTEXT, ERROR_HANDLE_EOF, xt_file_path(of));
		goto failed;
	}
#else
	ssize_t result_size;

	result_size = pread(of->x.of_filedes, data, size, offset);
	if (result_size == -1) {
		xt_register_ferrno(XT_REG_CONTEXT, errno, xt_file_path(of));
		return FAILED;
	}

	/* Read may not work because of rounding: */
	ASSERT_NS(offset + size == (offset + result_size + 1023) / 1024 * 1024);
	size = result_size;

	stat->ts_read += (u_int) size;

#ifdef XT_TIME_DISK_WRITES
	stat->ts_write_start = xt_trace_clock();
#endif

	result_size = pwrite(of->x.of_filedes, data, size, offset);
	if (result_size == -1) {
		xt_register_ferrno(XT_REG_CONTEXT, errno, xt_file_path(of));
		goto failed;
	}

	if ((size_t) result_size != size) {
		xt_register_ferrno(XT_REG_CONTEXT, ESPIPE, xt_file_path(of));
		goto failed;
	}
#endif

#ifdef XT_TIME_DISK_WRITES
	s = stat->ts_write_start;
	stat->ts_write_start = 0;
	stat->ts_write_time += xt_trace_clock() - s;
#endif
	stat->ts_write += (u_int) size;
	return OK;
	
	failed:
#ifdef XT_TIME_DISK_WRITES
	s = stat->ts_write_start;
	stat->ts_write_start = 0;
	stat->ts_write_time += xt_trace_clock() - s;
#endif
	return FAILED;
}

xtPublic xtBool xt_pwrite_file(XTOpenFilePtr of, off_t offset, size_t size, void *data, XTIOStatsPtr stat, XTThreadPtr thread)
{
	xtThreadID	thd_id;
#ifdef XT_TIME_DISK_WRITES
	xtWord8		s;
#endif

#if defined(DEBUG_TRACE_IO) || defined(DEBUG_TRACE_AROUND)
	char	timef[50];
	xtWord8	start = xt_trace_clock();
#endif
#ifdef DEBUG_TRACE_AROUND
	if (xt_is_extension(of->fr_file->fil_path, "xti"))
		PRINTF("%s %s write\n", xt_trace_clock_str(timef), thread->t_name);
#endif
#ifdef INJECT_WRITE_FILE_ERROR
	if ((xt_is_extension(of->fr_file->fil_path, INJECT_WRITE_FILE_TYPE) ||
		xt_starts_with(xt_last_name_of_path(of->fr_file->fil_path), INJECT_WRITE_FILE_TYPE))
#ifdef INJECT_ONCE_OFF
		&& !error_returned
#endif
		)
	{
		if (xt_seek_eof_file(NULL, of) > INJECT_WRITE_FILE_SIZE) {
			error_returned = TRUE;
			xt_register_ferrno(XT_REG_CONTEXT, 30, of->fr_file->fil_path);
			return FAILED;
		}
	}
#endif

	switch (of->of_type) {
		case XT_FT_NONE:
			break;
		case XT_FT_REWRITE_FLUSH:
			XTRewriteFlushPtr	rf;
			RewriteBlockPtr		rec;
			off_t				block_offset;
			off_t				block_size;
			size_t				idx;
			xtWord8				flush_offset;

			rf = of->fr_file->x.fil_rewrite;

			/* Round up and down by 1K */
			block_offset = offset / 1024 * 1024;
			block_size = ((offset + size + 1023) / 1024 * 1024) - block_offset;

			xt_spinlock_lock(&rf->rf_lock);
			if ((rec = (RewriteBlockPtr) fs_rewrite_bsearch(&block_offset, rf->rf_blocks, rf->rf_block_count, sizeof(RewriteBlockRec), &idx, rewrite_opt_comp))) {
				if (block_size > rec->rb_size)
					rec->rb_size = block_size;
				goto merge_right;
			}

			if (idx == 0) {
				/* The offset is before the first entry. */
				if (idx < rf->rf_block_count) {
					/* There is a first entry: */
					rec = rf->rf_blocks;
					if (rec->rb_offset - (block_offset + block_size) < XT_REWRITE_BLOCK_DISTANCE)
						goto add_to_right;
				}

				/* Add the first entry: */
				goto add_the_entry;
			}

			/* Not the first entry: */
			idx--;
			rec = rf->rf_blocks + idx;

			if (block_offset - (rec->rb_offset + rec->rb_size) < XT_REWRITE_BLOCK_DISTANCE) {
				/* Add to block on left: */
				block_size = (block_offset + block_size) - rec->rb_offset;
				if (block_size > rec->rb_size)
					rec->rb_size = block_size;
				goto merge_right;
			}
			
			idx++;
			rec = rf->rf_blocks + idx;

			if (idx < rf->rf_block_count && rec->rb_offset - (block_offset + block_size) < XT_REWRITE_BLOCK_DISTANCE)
				goto add_to_right;

			add_the_entry:
			ASSERT_NS(rf->rf_block_count < XT_REWRITE_MAX_BLOCKS);
			rec = rf->rf_blocks + idx;
			memmove(rec+1, rec, (rf->rf_block_count - idx) * sizeof(RewriteBlockRec));
			rec->rb_offset = block_offset;
			rec->rb_size = block_size;
			rf->rf_block_count++;
			goto continue_write;

			add_to_right:
			rec->rb_size += rec->rb_offset - block_offset;
			if (block_size > rec->rb_size)
				rec->rb_size = block_size;
			rec->rb_offset = block_offset;

			merge_right:
			if (idx+1 < rf->rf_block_count) {
				/* There is a record right: */
				if (rec->rb_offset + rec->rb_size + XT_REWRITE_BLOCK_DISTANCE > (rec+1)->rb_offset) {
					/* Merge and remove! */
					block_size = (rec+1)->rb_size + ((rec+1)->rb_offset - rec->rb_offset);
					if (block_size > rec->rb_size)
						rec->rb_size = block_size;
					rf->rf_block_count--;
					assert(rf->rf_block_count > idx);
					memmove(rec+1, rec+2, (rf->rf_block_count - idx - 1) * sizeof(RewriteBlockRec));
				}
			}
			continue_write:
			if (rf->rf_block_count == XT_REWRITE_MAX_BLOCKS) {
				/* Consolidate 2 blocks that are closest to each other in other to
				 * make space for another block:
				 */
				int		i;
				off_t	gap;
				off_t	min_gap = (off_t) -1;

				rec = rf->rf_blocks;
				for (i=0; i<rf->rf_block_count-1; i++) {
					gap = (rec+1)->rb_offset - (rec->rb_offset + rec->rb_size);
					if (min_gap == (off_t) -1 || gap < min_gap) {
						idx = i;
						min_gap = gap;
					}
					rec++;
				}

				/* Merge this with the next: */
				rec = rf->rf_blocks + idx;
				block_size = (rec+1)->rb_size + ((rec+1)->rb_offset - rec->rb_offset);
				if (block_size > rec->rb_size)
					rec->rb_size = block_size;
				rf->rf_block_count--;
				ASSERT_NS(rf->rf_block_count > idx);
				memmove(rec+1, rec+2, (rf->rf_block_count - idx - 1) * sizeof(RewriteBlockRec));
			}
			xt_spinlock_unlock(&rf->rf_lock);
			RR_FLUSH_READ_LOCK(&rf->rf_write_lock, thread->t_id);
			
			/* Wait for flush to pass this point: */
			for (;;) {
				flush_offset = ((xtWord8) rf->rf_flush_offset_hi << 32) | rf->rf_flush_offset_lo;
				if (offset < flush_offset)
					break;
				xt_critical_wait();
			}
#ifdef XT_TIME_DISK_WRITES
			stat->ts_write_start = xt_trace_clock();
#endif
#ifdef XT_WIN
			LARGE_INTEGER	liDistanceToMove;
			DWORD			result;
			
			liDistanceToMove.QuadPart = offset;
			if (!SetFilePointerEx(of->of_filedes, liDistanceToMove, NULL, FILE_BEGIN)) {
				RR_FLUSH_UNLOCK(&rf->rf_write_lock, thread->t_id);
				xt_register_ferrno(XT_REG_CONTEXT, fs_get_win_error(), xt_file_path(of));
				goto failed;
			}

			if (!WriteFile(of->of_filedes, data, size, &result, NULL)) {
				RR_FLUSH_UNLOCK(&rf->rf_write_lock, thread->t_id);
				xt_register_ferrno(XT_REG_CONTEXT, fs_get_win_error(), xt_file_path(of));
				goto failed;
			}

			if (result != size) {
				RR_FLUSH_UNLOCK(&rf->rf_write_lock, thread->t_id);
				xt_register_ferrno(XT_REG_CONTEXT, ERROR_HANDLE_EOF, xt_file_path(of));
				goto failed;
			}
#else
			ssize_t write_size;

			write_size = pwrite(of->x.of_filedes, data, size, offset);
			if (write_size == -1) {
				RR_FLUSH_UNLOCK(&rf->rf_write_lock, thread->t_id);
				xt_register_ferrno(XT_REG_CONTEXT, errno, xt_file_path(of));
				goto failed;
			}

			if ((size_t) write_size != size) {
				RR_FLUSH_UNLOCK(&rf->rf_write_lock, thread->t_id);
				xt_register_ferrno(XT_REG_CONTEXT, ESPIPE, xt_file_path(of));
				goto failed;
			}
#endif
			RR_FLUSH_UNLOCK(&rf->rf_write_lock, thread->t_id);
			break;
		case XT_FT_STANDARD:
#ifdef XT_TIME_DISK_WRITES
			stat->ts_write_start = xt_trace_clock();
#endif
#ifdef XT_WIN
			LARGE_INTEGER	liDistanceToMove;
			DWORD			result;
			
			liDistanceToMove.QuadPart = offset;
			if (!SetFilePointerEx(of->of_filedes, liDistanceToMove, NULL, FILE_BEGIN)) {
				xt_register_ferrno(XT_REG_CONTEXT, fs_get_win_error(), xt_file_path(of));
				goto failed;
			}

			if (!WriteFile(of->of_filedes, data, size, &result, NULL)) {
				xt_register_ferrno(XT_REG_CONTEXT, fs_get_win_error(), xt_file_path(of));
				goto failed;
			}

			if (result != size) {
				xt_register_ferrno(XT_REG_CONTEXT, ERROR_HANDLE_EOF, xt_file_path(of));
				goto failed;
			}
#else
			write_size = pwrite(of->x.of_filedes, data, size, offset);
			if (write_size == -1) {
				xt_register_ferrno(XT_REG_CONTEXT, errno, xt_file_path(of));
				goto failed;
			}

			if ((size_t) write_size != size) {
				xt_register_ferrno(XT_REG_CONTEXT, ESPIPE, xt_file_path(of));
				goto failed;
			}
#endif
			break;
		case XT_FT_MEM_MAP: {
			XTFileMemMapPtr mm = of->x.mf_memmap;

			thd_id  = thread->t_id;
			ASSERT_NS(!of->mf_slock_count);
			FILE_MAP_READ_LOCK(&mm->mm_lock, thd_id);
			if (!mm->mm_start || offset + (off_t) size > mm->mm_length) {
				FILE_MAP_UNLOCK(&mm->mm_lock, thd_id);

				FILE_MAP_WRITE_LOCK(&mm->mm_lock, thd_id);
				if (!fs_remap_file(of, offset, size, stat)) {
					FILE_MAP_UNLOCK(&mm->mm_lock, thd_id);
					return FAILED;
				}
			}

#ifdef XT_TIME_DISK_WRITES
			stat->ts_write_start = xt_trace_clock();
#endif
#ifdef XT_WIN
			__try
			{
				memcpy(mm->mm_start + offset, data, size);
			}
			// GetExceptionCode()== EXCEPTION_IN_PAGE_ERROR ? EXCEPTION_EXECUTE_HANDLER : EXCEPTION_CONTINUE_SEARCH
			__except(EXCEPTION_EXECUTE_HANDLER)
			{
				xt_register_ferrno(XT_REG_CONTEXT, GetExceptionCode(), xt_file_path(map));
				FILE_MAP_UNLOCK(&mm->mm_lock, thd_id);
				goto failed;
			}
#else
			memcpy(mm->mm_start + offset, data, size);
#endif

			FILE_MAP_UNLOCK(&mm->mm_lock, thd_id);
			break;
		}
		case XT_FT_HEAP: {
			XTFileHeapPtr	fh = of->x.of_heap;

#ifdef XT_TIME_DISK_WRITES
			stat->ts_write_start = xt_trace_clock();
#endif
			thd_id  = thread->t_id;
			ASSERT_NS(!of->mf_slock_count);
			FILE_MAP_READ_LOCK(&fh->fh_lock, thd_id);
			if (!fh->fh_start || offset + (off_t) size > fh->fh_length) {
				FILE_MAP_UNLOCK(&fh->fh_lock, thd_id);

				FILE_MAP_WRITE_LOCK(&fh->fh_lock, thd_id);
				off_t new_len;

				new_len = (((offset + size) / fh->fh_grow_size) + 1) * fh->fh_grow_size;
				if (!xt_realloc_ns((void **) &fh->fh_start, (size_t) new_len)) {
					FILE_MAP_UNLOCK(&fh->fh_lock, thd_id);
					goto failed;
				}
				fh->fh_length = new_len;
			}

			memcpy(fh->fh_start + offset, data, size);

			FILE_MAP_UNLOCK(&fh->fh_lock, thd_id);
			break;
		}
	}

#ifdef XT_TIME_DISK_WRITES
	s = stat->ts_write_start;
	stat->ts_write_start = 0;
	stat->ts_write_time += xt_trace_clock() - s;
#endif
	stat->ts_write += (u_int) size;
#ifdef DEBUG_TRACE_IO
	xt_trace("/* %s */ pbxt_file_writ(\"%s\", %lu, %lu);\n", xt_trace_clock_diff(timef, start), of->fr_file->fil_path, (u_long) offset, (u_long) size);
#endif
#ifdef DEBUG_TRACE_AROUND
	if (xt_is_extension(of->fr_file->fil_path, "xti"))
		PRINTF("%s %s write (%d)\n", xt_trace_clock_str(timef), thread->t_name, (int) (xt_trace_clock() - start));
#endif
	return OK;
	
	failed:
#ifdef XT_TIME_DISK_WRITES
	s = stat->ts_write_start;
	stat->ts_write_start = 0;
	stat->ts_write_time += xt_trace_clock() - s;
#endif
	return FAILED;
}

xtPublic xtBool xt_flush_file(XTOpenFilePtr of, XTIOStatsPtr stat, XTThreadPtr thread)
{
	xtWord8 s;

#if defined(DEBUG_TRACE_IO) || defined(DEBUG_TRACE_AROUND)
	char	timef[50];
	xtWord8	start = xt_trace_clock();
#endif
#ifdef DEBUG_TRACE_AROUND
	if (xt_is_extension(of->fr_file->fil_path, "xti"))
		PRINTF("%s %s FLUSH\n", xt_trace_clock_str(timef), thread->t_name);
#endif
#ifdef INJECT_FLUSH_FILE_ERROR
	if ((xt_is_extension(of->fr_file->fil_path, INJECT_FLUSH_FILE_TYPE) ||
		xt_starts_with(xt_last_name_of_path(of->fr_file->fil_path), INJECT_FLUSH_FILE_TYPE))) {
		if (xt_seek_eof_file(NULL, of) > INJECT_FLUSH_FILE_SIZE) {
			xt_register_ferrno(XT_REG_CONTEXT, 30, of->fr_file->fil_path);
			return FAILED;
		}
	}
#endif

	switch (of->of_type) {
		case XT_FT_NONE:
			break;
		case XT_FT_REWRITE_FLUSH:
			XTRewriteFlushPtr	rf;
			RewriteBlockPtr		rec;
			int					i;
			off_t				offset;
			off_t				size;
			off_t				tfer;

			rf = of->fr_file->x.fil_rewrite;

			xt_lock_mutex_ns(&rf->rf_flush_lock);

			/* Re-write all areas written: */
			xt_spinlock_lock(&rf->rf_lock);
			rf->rf_flush_block_count = rf->rf_block_count;
			memcpy(rf->rf_flush_blocks, rf->rf_blocks, rf->rf_flush_block_count * sizeof(RewriteBlockRec));
			rf->rf_block_count = 0;
			xt_spinlock_unlock(&rf->rf_lock);

			/* Update the flush offset: */
			/* This lock ensures that there are no more writers: */
			RR_FLUSH_WRITE_LOCK(&rf->rf_write_lock, thread->t_id);
			/* We use atomic operations to ensure write through cache: */
			xt_atomic_set4(&rf->rf_flush_offset_lo, 0);
			xt_atomic_set4(&rf->rf_flush_offset_hi, 0);
			RR_FLUSH_UNLOCK(&rf->rf_write_lock, thread->t_id);

			rec = rf->rf_flush_blocks;
			for (i=0; i<rf->rf_flush_block_count; i++) {
				size = rec->rb_size;
				offset = rec->rb_offset;
				while (size) {
					tfer = XT_REWRITE_BUFFER_SIZE;
					if (tfer > size)
						tfer = size;
					if (!fs_rewrite_file(of, offset, (size_t) tfer, rf->rf_flush_buffer, stat, thread)) {
						RR_FLUSH_WRITE_LOCK(&rf->rf_write_lock, thread->t_id);
						xt_atomic_set4(&rf->rf_flush_offset_lo, 0xFFFFFFFF);
						xt_atomic_set4(&rf->rf_flush_offset_hi, 0xFFFFFFFF);
						RR_FLUSH_UNLOCK(&rf->rf_write_lock, thread->t_id);
						xt_unlock_mutex_ns(&rf->rf_flush_lock);
						goto failed;
					}
					offset += tfer;
					/* Update the flush offset: */
					xt_atomic_set4(&rf->rf_flush_offset_lo, offset & 0xFFFFFFFF);
					if (sizeof(off_t) > 4)
						xt_atomic_set4(&rf->rf_flush_offset_hi, (xtWord4) (((xtWord8) offset >> 32) & 0xFFFFFFFF));
					
					size -= tfer;
				}
				rec++;
			}
			
			RR_FLUSH_WRITE_LOCK(&rf->rf_write_lock, thread->t_id);
			xt_atomic_set4(&rf->rf_flush_offset_lo, 0xFFFFFFFF);
			xt_atomic_set4(&rf->rf_flush_offset_hi, 0xFFFFFFFF);
			RR_FLUSH_UNLOCK(&rf->rf_write_lock, thread->t_id);
			xt_unlock_mutex_ns(&rf->rf_flush_lock);
			/* No break required. */
		case XT_FT_STANDARD:
			stat->ts_flush_start = xt_trace_clock();
#ifdef XT_WIN
			if (!FlushFileBuffers(of->x.of_filedes)) {
				xt_register_ferrno(XT_REG_CONTEXT, fs_get_win_error(), xt_file_path(of));
				goto failed;
			}
#else
			/* Mac OS X has problems with fsync. We had several cases of 
			 *index corruption presumably because fsync didn't really 
			 * flush index pages to disk. fcntl(F_FULLFSYNC) is considered 
			 * more effective in such case.
			 */
#if defined(F_FULLFSYNC) && !defined(DEBUG_FAST_MAC)
			if (fcntl(of->x.of_filedes, F_FULLFSYNC, 0) == -1) {
				xt_register_ferrno(XT_REG_CONTEXT, errno, xt_file_path(of));
				goto failed;
			}
#else
			if (fsync(of->x.of_filedes) == -1) {
				xt_register_ferrno(XT_REG_CONTEXT, errno, xt_file_path(of));
				goto failed;
			}

#endif
#endif
			break;
		case XT_FT_MEM_MAP: {
			XTFileMemMapPtr mm = of->x.mf_memmap;
#ifndef XT_NO_ATOMICS
			xtThreadID		thd_id = thread->t_id;
#endif

			if (!of->mf_slock_count)
				FILE_MAP_READ_LOCK(&mm->mm_lock, thd_id);
			if (!mm->mm_start) {
				FILE_MAP_UNLOCK(&mm->mm_lock, thd_id);
				ASSERT_NS(!of->mf_slock_count);
				FILE_MAP_WRITE_LOCK(&mm->mm_lock, thd_id);
				if (!fs_remap_file(of, 0, 0, stat)) {
					if (!of->mf_slock_count)
						FILE_MAP_UNLOCK(&mm->mm_lock, thd_id);
					return FAILED;
				}
			}
			stat->ts_flush_start = xt_trace_clock();
#ifdef XT_WIN
			if (!FlushViewOfFile(mm->mm_start, 0)) {
				xt_register_ferrno(XT_REG_CONTEXT, fs_get_win_error(), xt_file_path(of));
				if (!of->mf_slock_count)
					FILE_MAP_UNLOCK(&mm->mm_lock, thd_id);
				goto failed;
			}
#else
			if (msync( (char *)mm->mm_start, (size_t) mm->mm_length, MS_SYNC) == -1) {
				xt_register_ferrno(XT_REG_CONTEXT, errno, xt_file_path(of));
				if (!of->mf_slock_count)
					FILE_MAP_UNLOCK(&mm->mm_lock, thd_id);
				goto failed;
			}
#endif
			if (!of->mf_slock_count)
				FILE_MAP_UNLOCK(&mm->mm_lock, thd_id);
			break;
		}
		case XT_FT_HEAP:
			stat->ts_flush_start = xt_trace_clock();
			break;
	}

#ifdef DEBUG_TRACE_IO
	xt_trace("/* %s */ pbxt_file_sync(\"%s\");\n", xt_trace_clock_diff(timef, start), of->fr_file->fil_path);
#endif
#ifdef DEBUG_TRACE_AROUND
	if (xt_is_extension(of->fr_file->fil_path, "xti"))
		PRINTF("%s %s FLUSH (%d)\n", xt_trace_clock_str(timef), thread->t_name, (int) (xt_trace_clock() - start));
#endif
	s = stat->ts_flush_start;
	stat->ts_flush_start = 0;
	stat->ts_flush_time += xt_trace_clock() - s;
	stat->ts_flush++;
	return OK;

	failed:
	s = stat->ts_flush_start;
	stat->ts_flush_start = 0;
	stat->ts_flush_time += xt_trace_clock() - s;
	return FAILED;
}

xtPublic xtBool xt_pread_file_4(XTOpenFilePtr of, off_t offset, xtWord4 *value, XTIOStatsPtr stat, XTThreadPtr thread)
{
	xtThreadID	thd_id;
#ifdef XT_TIME_DISK_READS
	xtWord8		s;
#endif

#ifdef DEBUG_TRACE_MAP_IO
	xt_trace("/* %s */ pbxt_fmap_read_4(\"%s\", %lu, 4);\n", xt_trace_clock_diff(NULL), of->fr_file->fil_path, (u_long) offset);
#endif
	switch (of->of_type) {
		case XT_FT_NONE:
			break;
		case XT_FT_REWRITE_FLUSH:
		case XT_FT_STANDARD:
			xtWord1			data[4];
#ifdef XT_TIME_DISK_READS
			stat->ts_read_start = xt_trace_clock();
#endif
#ifdef XT_WIN
			LARGE_INTEGER	liDistanceToMove;
			DWORD			result;

			liDistanceToMove.QuadPart = offset;
			if (!SetFilePointerEx(of->of_filedes, liDistanceToMove, NULL, FILE_BEGIN)) {
				xt_register_ferrno(XT_REG_CONTEXT, fs_get_win_error(), xt_file_path(of));
				goto failed;
			}

			if (!ReadFile(of->of_filedes, data, 4, &result, NULL)) {
				xt_register_ferrno(XT_REG_CONTEXT, fs_get_win_error(), xt_file_path(of));
				goto failed;
			}

			if ((size_t) result < 4) {
				xt_register_ferrno(XT_REG_CONTEXT, ERROR_HANDLE_EOF, xt_file_path(of));
				goto failed;
			}
#else
			ssize_t read_size;

			read_size = pread(of->x.of_filedes, data, 4, offset);
			if (read_size == -1) {
				xt_register_ferrno(XT_REG_CONTEXT, errno, xt_file_path(of));
				goto failed;
			}

			/* Throw an error if read less than the minimum: */
			if ((size_t) read_size < 4) {
				xt_register_ferrno(XT_REG_CONTEXT, ESPIPE, xt_file_path(of));
				goto failed;
			}
#endif
			*value = XT_GET_DISK_4(data);
			break;
		case XT_FT_MEM_MAP: {
			XTFileMemMapPtr	mm = of->x.mf_memmap;

			thd_id = thread->t_id;
			if (!of->mf_slock_count)
				FILE_MAP_READ_LOCK(&mm->mm_lock, thd_id);
			if (!mm->mm_start) {
				ASSERT_NS(!of->mf_slock_count);
				FILE_MAP_UNLOCK(&mm->mm_lock, thd_id);
				FILE_MAP_WRITE_LOCK(&mm->mm_lock, thd_id);
				if (!fs_remap_file(of, 0, 0, stat)) {
					FILE_MAP_UNLOCK(&mm->mm_lock, thd_id);
					return FAILED;
				}
			}
			if (offset >= mm->mm_length)
				*value = 0;
			else {
				xtWord1 *data;

				data = mm->mm_start + offset;
#ifdef XT_TIME_DISK_READS
				stat->ts_read_start = xt_trace_clock();
#endif
#ifdef XT_WIN
				__try
				{
					*value = XT_GET_DISK_4(data);
					// GetExceptionCode()== EXCEPTION_IN_PAGE_ERROR ? EXCEPTION_EXECUTE_HANDLER : EXCEPTION_CONTINUE_SEARCH
				}
				__except(EXCEPTION_EXECUTE_HANDLER)
				{
					FILE_MAP_UNLOCK(&mm->mm_lock, thd_id);
					xt_register_ferrno(XT_REG_CONTEXT, GetExceptionCode(), xt_file_path(of));
					goto failed;
				}
#else
				*value = XT_GET_DISK_4(data);
#endif
			}

			if (!of->mf_slock_count)
				FILE_MAP_UNLOCK(&mm->mm_lock, thd_id);
			break;
		}
		case XT_FT_HEAP: {
			XTFileHeapPtr	fh = of->x.of_heap;

#ifdef XT_TIME_DISK_READS
			stat->ts_read_start = xt_trace_clock();
#endif
			if (!of->mf_slock_count)
				FILE_MAP_READ_LOCK(&fh->fh_lock, thd_id);
			if (!fh->fh_start) {
				ASSERT_NS(!of->mf_slock_count);
				FILE_MAP_UNLOCK(&fh->fh_lock, thd_id);
				FILE_MAP_WRITE_LOCK(&fh->fh_lock, thd_id);
				if (!fs_remap_file(of, 0, 0, stat)) {
					FILE_MAP_UNLOCK(&fh->fh_lock, thd_id);
					goto failed;
				}
			}
			if (offset >= fh->fh_length)
				*value = 0;
			else {
				xtWord1 *data;

				data = fh->fh_start + offset;
				*value = XT_GET_DISK_4(data);
			}
			if (!of->mf_slock_count)
				FILE_MAP_UNLOCK(&fh->fh_lock, thd_id);
			break;
		}
	}

#ifdef XT_TIME_DISK_READS
	s = stat->ts_read_start;
	stat->ts_read_start = 0;
	stat->ts_read_time += xt_trace_clock() - s;
#endif
	stat->ts_read += 4;
	return OK;

	failed:
#ifdef XT_TIME_DISK_READS
	s = stat->ts_read_start;
	stat->ts_read_start = 0;
	stat->ts_read_time += xt_trace_clock() - s;
#endif
	return FAILED;
}

xtBool xt_pread_file(XTOpenFilePtr of, off_t offset, size_t size, size_t min_size, void *data, size_t *red_size, XTIOStatsPtr stat, XTThreadPtr thread)
{
	xtThreadID	thd_id;
	size_t		tfer = 0;
#ifdef XT_TIME_DISK_READS
	xtWord8		s;
#endif

#if defined(DEBUG_TRACE_IO) || defined(DEBUG_TRACE_AROUND)
	char	timef[50];
	xtWord8	start = xt_trace_clock();
#endif
#ifdef DEBUG_TRACE_AROUND
	if (xt_is_extension(of->fr_file->fil_path, "xti"))
		PRINTF("%s %s read\n", xt_trace_clock_str(timef), thread->t_name);
#endif

	switch (of->of_type) {
		case XT_FT_NONE:
			break;
		case XT_FT_REWRITE_FLUSH:
		case XT_FT_STANDARD:
#ifdef XT_TIME_DISK_READS
			stat->ts_read_start = xt_trace_clock();
#endif
#ifdef XT_WIN
			LARGE_INTEGER	liDistanceToMove;
			DWORD			result;

			liDistanceToMove.QuadPart = offset;
			if (!SetFilePointerEx(of->of_filedes, liDistanceToMove, NULL, FILE_BEGIN)) {
				xt_register_ferrno(XT_REG_CONTEXT, fs_get_win_error(), xt_file_path(of));
				goto failed;
			}

			if (!ReadFile(of->of_filedes, data, size, &result, NULL)) {
				xt_register_ferrno(XT_REG_CONTEXT, fs_get_win_error(), xt_file_path(of));
				goto failed;
			}

			tfer = (size_t) result;
#else
			ssize_t			result;

			result = pread(of->x.of_filedes, data, size, offset);
			if (result == -1) {
				xt_register_ferrno(XT_REG_CONTEXT, errno, xt_file_path(of));
				goto failed;
			}

			tfer = (size_t) result;
#endif
			break;
		case XT_FT_MEM_MAP: {
			XTFileMemMapPtr	mm = of->x.mf_memmap;

			thd_id = thread->t_id;
			/* NOTE!! The file map may already be locked,
			 * by a call to xt_lock_fmap_ptr()!
			 *
			 * 20.05.2009: This problem should be fixed now with mf_slock_count!
			 *
			 * This can occur during a sequential scan:
			 * xt_pread_fmap()  Line 1330
			 * XTTabCache::tc_read_direct()  Line 361
			 * XTTabCache::xt_tc_read()  Line 220
			 * xt_tab_get_rec_data()
			 * tab_visible()  Line 2412
			 * xt_tab_seq_next()  Line 4068
			 *
			 * And occurs during the following test:
			 * create table t1 ( a int not null, b int not null) ;
			 * --disable_query_log
			 * insert into t1 values (1,1),(2,2),(3,3),(4,4);
			 * let $1=19;
			 * set @d=4;
			 * while ($1)
			 * {
			 *   eval insert into t1 select a+@d,b+@d from t1;
			 *   eval set @d=@d*2;
			 *   dec $1;
			 * }
			 * 
			 * --enable_query_log
			 * alter table t1 add index i1(a);
			 * delete from t1 where a > 2000000;
			 * create table t2 like t1;
			 * insert into t2 select * from t1;
			 *
			 * As a result, the slock must be able to handle
			 * nested calls to lock/unlock.
			 */
			if (!of->mf_slock_count)
				FILE_MAP_READ_LOCK(&mm->mm_lock, thd_id);
			tfer = size;
			if (!mm->mm_start) {
				FILE_MAP_UNLOCK(&mm->mm_lock, thd_id);
				ASSERT_NS(!of->mf_slock_count);
				FILE_MAP_WRITE_LOCK(&mm->mm_lock, thd_id);
				if (!fs_remap_file(of, 0, 0, stat)) {
					if (!of->mf_slock_count)
						FILE_MAP_UNLOCK(&mm->mm_lock, thd_id);
					return FAILED;
				}
			}
			if (offset >= mm->mm_length)
				tfer = 0;
			else {
				if (mm->mm_length - offset < (off_t) tfer)
					tfer = (size_t) (mm->mm_length - offset);
#ifdef XT_TIME_DISK_READS
				stat->ts_read_start = xt_trace_clock();
#endif
#ifdef XT_WIN
				__try
				{
					memcpy(data, mm->mm_start + offset, tfer);
					// GetExceptionCode()== EXCEPTION_IN_PAGE_ERROR ? EXCEPTION_EXECUTE_HANDLER : EXCEPTION_CONTINUE_SEARCH
				}
				__except(EXCEPTION_EXECUTE_HANDLER)
				{
					if (!of->mf_slock_count)
						FILE_MAP_UNLOCK(&mm->mm_lock, thd_id);
					xt_register_ferrno(XT_REG_CONTEXT, GetExceptionCode(), xt_file_path(of));
					goto failed;
				}
#else
				memcpy(data, mm->mm_start + offset, tfer);
#endif
			}
			if (!of->mf_slock_count)
				FILE_MAP_UNLOCK(&mm->mm_lock, thd_id);
			break;
		}
		case XT_FT_HEAP: {
			XTFileHeapPtr	fh = of->x.of_heap;

#ifdef XT_TIME_DISK_READS
			stat->ts_read_start = xt_trace_clock();
#endif
			thd_id  = thread->t_id;
			if (!of->mf_slock_count)
				FILE_MAP_READ_LOCK(&fh->fh_lock, thd_id);
			tfer = size;
			if (offset >= fh->fh_length)
				tfer = 0;
			else {
				if (fh->fh_length - offset < (off_t) tfer)
					tfer = (size_t) (fh->fh_length - offset);
				memcpy(data, fh->fh_start + offset, tfer);
			}
			if (!of->mf_slock_count)
				FILE_MAP_UNLOCK(&fh->fh_lock, thd_id);
			break;
		}
	}

#ifdef XT_TIME_DISK_READS
	s = stat->ts_read_start;
	stat->ts_read_start = 0;
	stat->ts_read_time += xt_trace_clock() - s;
#endif
	stat->ts_read += tfer;

	/* Throw an error if read less than the minimum: */
	if (tfer < min_size)
		return xt_register_ferrno(XT_REG_CONTEXT, ESPIPE, xt_file_path(of));
	if (red_size)
		*red_size = tfer;
#ifdef DEBUG_TRACE_IO
	xt_trace("/* %s */ pbxt_file_read(\"%s\", %lu, %lu);\n", xt_trace_clock_diff(timef, start), of->fr_file->fil_path, (u_long) offset, (u_long) size);
#endif
#ifdef DEBUG_TRACE_AROUND
	if (xt_is_extension(of->fr_file->fil_path, "xti"))
		PRINTF("%s %s read (%d)\n", xt_trace_clock_str(timef), thread->t_name, (int) (xt_trace_clock() - start));
#endif
	return OK;

	failed:
#ifdef XT_TIME_DISK_READS
	s = stat->ts_read_start;
	stat->ts_read_start = 0;
	stat->ts_read_time += xt_trace_clock() - s;
#endif
	return FAILED;
}

xtPublic xtBool xt_lock_file_ptr(XTOpenFilePtr of, xtWord1 **data, off_t offset, size_t size, XTIOStatsPtr stat, XTThreadPtr thread)
{
	switch (of->of_type) {
		case XT_FT_NONE:
			break;
		case XT_FT_REWRITE_FLUSH:
		case XT_FT_STANDARD:
			size_t red_size;
		
			if (!*data) {
				if (!(*data = (xtWord1 *) xt_malloc_ns(size)))
					return FAILED;
			}

			if (!xt_pread_file(of, offset, size, 0, *data, &red_size, stat, thread))
				return FAILED;
			
			//if (red_size < size)
			//	memset();
			break;
		case XT_FT_MEM_MAP: {
			XTFileMemMapPtr	mm = of->x.mf_memmap;
#ifndef XT_NO_ATOMICS
			xtThreadID		thd_id = thread->t_id;
#endif

			if (!of->mf_slock_count)
				FILE_MAP_READ_LOCK(&mm->mm_lock, thd_id);
			of->mf_slock_count++;
			if (!mm->mm_start) {
				FILE_MAP_UNLOCK(&mm->mm_lock, thd_id);
				ASSERT_NS(of->mf_slock_count == 1);
				FILE_MAP_WRITE_LOCK(&mm->mm_lock, thd_id);
				if (!fs_remap_file(of, 0, 0, stat)) {
					of->mf_slock_count--;
					if (!of->mf_slock_count)
						FILE_MAP_UNLOCK(&mm->mm_lock, thd_id);
					return FAILED;
				}
			}
			if (offset >= mm->mm_length) {
				of->mf_slock_count--;
				if (!of->mf_slock_count)
					FILE_MAP_UNLOCK(&mm->mm_lock, thd_id);
				return FAILED;
			}
			
			if (offset + (off_t) size > mm->mm_length)
				stat->ts_read += (u_int) (offset + (off_t) size - mm->mm_length);
			else
				stat->ts_read += size;
			*data = mm->mm_start + offset;
			break;
		}
		case XT_FT_HEAP: {
			XTFileHeapPtr	fh = of->x.of_heap;
#ifndef XT_NO_ATOMICS
			xtThreadID		thd_id = thread->t_id;
#endif

			if (!of->mf_slock_count)
				FILE_MAP_READ_LOCK(&fh->fh_lock, thd_id);
			of->mf_slock_count++;
			if (!fh->fh_start) {
				FILE_MAP_UNLOCK(&fh->fh_lock, thd_id);
				ASSERT_NS(of->mf_slock_count == 1);
				FILE_MAP_WRITE_LOCK(&fh->fh_lock, thd_id);
				if (!fs_remap_file(of, 0, 0, stat)) {
					of->mf_slock_count--;
					if (!of->mf_slock_count)
						FILE_MAP_UNLOCK(&fh->fh_lock, thd_id);
					return FAILED;
				}
			}
			if (offset >= fh->fh_length) {
				of->mf_slock_count--;
				if (!of->mf_slock_count)
					FILE_MAP_UNLOCK(&fh->fh_lock, thd_id);
				return FAILED;
			}
			
			if (offset + (off_t) size > fh->fh_length)
				stat->ts_read += (u_int) (offset + (off_t) size - fh->fh_length);
			else
				stat->ts_read += size;
			*data = fh->fh_start + offset;
			break;
		}
	}

	return OK;
}

xtPublic void xt_unlock_file_ptr(XTOpenFilePtr of, xtWord1 *data, XTThreadPtr thread)
{
	switch (of->of_type) {
		case XT_FT_NONE:
			break;
		case XT_FT_REWRITE_FLUSH:
		case XT_FT_STANDARD:
			if (data)
				xt_free_ns(data);
			break;
		case XT_FT_MEM_MAP:
			of->mf_slock_count--;
			if (!of->mf_slock_count)
				FILE_MAP_UNLOCK(&of->x.mf_memmap->mm_lock, thread->t_id);
			break;
		case XT_FT_HEAP:
			of->mf_slock_count--;
			if (!of->mf_slock_count)
				FILE_MAP_UNLOCK(&of->x.of_heap->fh_lock, thread->t_id);
			break;
	}
}

/* ----------------------------------------------------------------------
 * Directory operations
 */

/*
 * The filter may contain one '*' as wildcard.
 */
XTOpenDirPtr xt_dir_open(XTThreadPtr self, c_char *path, c_char *filter)
{
	XTOpenDirPtr	od;

#ifdef XT_SOLARIS
	/* see the comment in filesys_xt.h */
	size_t sz = pathconf(path, _PC_NAME_MAX) + sizeof(XTOpenDirRec) + 1;
#else
	size_t sz = sizeof(XTOpenDirRec);
#endif
	pushsr_(od, xt_dir_close, (XTOpenDirPtr) xt_calloc(self, sz));

#ifdef XT_WIN
	size_t			len;

	od->od_handle = XT_NULL_FD;

	// path = path\(filter | *)
	len = strlen(path) + 1 + (filter ? strlen(filter) : 1) + 1;
	od->od_path = (char *) xt_malloc(self, len);

	strcpy(od->od_path, path);
	xt_add_dir_char(len, od->od_path);
	if (filter)
		strcat(od->od_path, filter);
	else
		strcat(od->od_path, "*");
#else
	od->od_path = xt_dup_string(self, path);

	if (filter)
		od->od_filter = xt_dup_string(self, filter);

	od->od_dir = opendir(path);
	if (!od->od_dir)
		xt_throw_ferrno(XT_CONTEXT, errno, path);
#endif
	popr_(); // Discard xt_dir_close(od)
	return od;
}

void xt_dir_close(XTThreadPtr self, XTOpenDirPtr od)
{
	if (od) {
#ifdef XT_WIN
		if (od->od_handle != XT_NULL_FD) {
			FindClose(od->od_handle);
			od->od_handle = XT_NULL_FD;
		}
#else
		if (od->od_dir) {
			closedir(od->od_dir);
			od->od_dir = NULL;
		}
		if (od->od_filter) {
			xt_free(self, od->od_filter);
			od->od_filter = NULL;
		}
#endif
		if (od->od_path) {
			xt_free(self, od->od_path);
			od->od_path = NULL;
		}
		xt_free(self, od);
	}
}

#ifdef XT_WIN
xtBool xt_dir_next(XTThreadPtr self, XTOpenDirPtr od)
{
	int err = 0;

	if (od->od_handle == INVALID_HANDLE_VALUE) {
		od->od_handle = FindFirstFile(od->od_path, &od->od_data);
		if (od->od_handle == INVALID_HANDLE_VALUE)
			err = fs_get_win_error();
	}
	else {
		if (!FindNextFile(od->od_handle, &od->od_data))
			err = fs_get_win_error();
	}

	if (err) {
		if (err != ERROR_NO_MORE_FILES) {
			if (err == ERROR_FILE_NOT_FOUND) {
				char path[PATH_MAX];

				xt_strcpy(PATH_MAX, path, od->od_path);
				xt_remove_last_name_of_path(path);
				if (!fs_exists(path, self))
					xt_throw_ferrno(XT_CONTEXT, err, path);
			}
			else
				xt_throw_ferrno(XT_CONTEXT, err, od->od_path);
		}
		return FAILED;
	}

	return OK;
}
#else
static xtBool fs_match_filter(c_char *name, c_char *filter)
{
	while (*name && *filter) {
		if (*filter == '*') {
			if (filter[1] == *name)
				filter++;
			else
				name++;
		}
		else {
			if (*name != *filter)
				return FALSE;
			name++;
			filter++;
		}
	}
	if (!*name) {
		if (!*filter || (*filter == '*' && !filter[1]))
			return TRUE;
	}
	return FALSE;
}

xtBool xt_dir_next(XTThreadPtr self, XTOpenDirPtr od)
{
	int				err;
	struct dirent	*result;

	for (;;) {
		err = readdir_r(od->od_dir, &od->od_entry, &result);
		if (err) {
			xt_throw_ferrno(XT_CONTEXT, err, od->od_path);
			return FAILED;
		}
		if (!result)
			break;
		/* Filter out '.' and '..': */
		if (od->od_entry.d_name[0] == '.') {
			if (od->od_entry.d_name[1] == '.') {
				if (od->od_entry.d_name[2] == '\0')
					continue;
			}
			else {
				if (od->od_entry.d_name[1] == '\0')
					continue;
			}
		}
		if (!od->od_filter)
			break;
		if (fs_match_filter(od->od_entry.d_name, od->od_filter))
			break;
	}
	return result ? TRUE : FALSE;
}
#endif

char *xt_dir_name(XTThreadPtr XT_UNUSED(self), XTOpenDirPtr od)
{
#ifdef XT_WIN
	return od->od_data.cFileName;
#else
	return od->od_entry.d_name;
#endif
}

xtBool xt_dir_is_file(XTThreadPtr self, XTOpenDirPtr od)
{
	(void) self;
#ifdef XT_WIN
	if (od->od_data.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
		return FALSE;
#elif defined(XT_SOLARIS)
        char path[PATH_MAX];
	struct stat sb;

	xt_strcpy(PATH_MAX, path, od->od_path);
	xt_add_dir_char(PATH_MAX, path);
	xt_strcat(PATH_MAX, path, od->od_entry.d_name);

	if (stat(path, &sb) == -1) {
		xt_throw_ferrno(XT_CONTEXT, errno, path);
		return FAILED;
	}

	if ( sb.st_mode & S_IFDIR )
		return FALSE;
#else
	if (od->od_entry.d_type & DT_DIR)
		return FALSE;
#endif
	return TRUE;
}

off_t xt_dir_file_size(XTThreadPtr self, XTOpenDirPtr od)
{
#ifdef XT_WIN
	return (off_t) od->od_data.nFileSizeLow | (((off_t) od->od_data.nFileSizeHigh) << 32);
#else
	char	path[PATH_MAX];
	off_t	size;

	xt_strcpy(PATH_MAX, path, od->od_path);
	xt_add_dir_char(PATH_MAX, path);
	xt_strcat(PATH_MAX, path, od->od_entry.d_name);
	if (!xt_fs_stat(self, path, &size, NULL))
		return -1;
	return size;
#endif
}

/* ----------------------------------------------------------------------
 * File mapping operations
 */

static xtBool fs_map_file(XTFileMemMapPtr mm, XTFilePtr file, xtBool grow)
{
#ifdef INJECT_WRITE_REMAP_ERROR
	if (xt_is_extension(file->fil_path, INJECT_REMAP_FILE_TYPE)) {
		if (mm->mm_length > INJECT_REMAP_FILE_SIZE) {
			xt_register_ferrno(XT_REG_CONTEXT, 30, file->fil_path);
			return FAILED;
		}
	}
#endif
	ASSERT_NS(!mm->mm_start);
#ifdef XT_WIN
	/* This will grow the file to the given size: */
	mm->mm_mapdes = CreateFileMapping(file->fil_filedes, NULL, PAGE_READWRITE, (DWORD) (mm->mm_length >> 32), (DWORD) mm->mm_length, NULL);
	if (mm->mm_mapdes == NULL) {
		xt_register_ferrno(XT_REG_CONTEXT, fs_get_win_error(), file->fil_path);
		return FAILED;
	}

	mm->mm_start = (xtWord1 *) MapViewOfFile(mm->mm_mapdes, FILE_MAP_WRITE, 0, 0, 0);
	if (!mm->mm_start) {
		CloseHandle(mm->mm_mapdes);
		mm->mm_mapdes = NULL;
		xt_register_ferrno(XT_REG_CONTEXT, fs_get_win_error(), file->fil_path);
		return FAILED;
	}
#else
	if (grow) {
		char data[2];

		if (pwrite(file->fil_filedes, data, 1, mm->mm_length - 1) == -1) {
			xt_register_ferrno(XT_REG_CONTEXT, errno, file->fil_path);
			return FAILED;
		}
	}

	/* Remap: */
	mm->mm_start = (xtWord1 *) mmap(0, (size_t) mm->mm_length, PROT_READ | PROT_WRITE, MAP_SHARED, file->fil_filedes, 0);
	if (mm->mm_start == MAP_FAILED) {
		mm->mm_start = NULL;
		xt_register_ferrno(XT_REG_CONTEXT, errno, file->fil_path);
		return FAILED;
	}
#endif
	return OK;
}

static xtBool fs_remap_file(XTOpenFilePtr map, off_t offset, size_t size, XTIOStatsPtr stat)
{
	off_t			new_size = 0;
	XTFileMemMapPtr	mm = map->x.mf_memmap;
	xtWord8			s;

	if (offset + (off_t) size > mm->mm_length) {
		/* Expand the file: */
		new_size = (mm->mm_length + (off_t) mm->mm_grow_size) / (off_t) mm->mm_grow_size;
		new_size *= mm->mm_grow_size;
		while (new_size < offset + (off_t) size)
			new_size += mm->mm_grow_size;

		if (sizeof(size_t) == 4 && new_size >= (off_t) 0xFFFFFFFF) {
			xt_register_ixterr(XT_REG_CONTEXT, XT_ERR_FILE_TOO_LONG, xt_file_path(map));
			return FAILED;
		}
	}
	else if (!mm->mm_start)
		new_size = mm->mm_length;

	if (new_size) {
		if (mm->mm_start) {
			/* Flush & unmap: */
			stat->ts_flush_start = xt_trace_clock();
#ifdef XT_WIN
			if (!FlushViewOfFile(mm->mm_start, 0)) {
				xt_register_ferrno(XT_REG_CONTEXT, fs_get_win_error(), xt_file_path(map));
				goto failed;
			}

			if (!UnmapViewOfFile(mm->mm_start)) {
				xt_register_ferrno(XT_REG_CONTEXT, fs_get_win_error(), xt_file_path(map));
				goto failed;
			}
#else
			if (msync( (char *)mm->mm_start, (size_t) mm->mm_length, MS_SYNC) == -1) {
				xt_register_ferrno(XT_REG_CONTEXT, errno, xt_file_path(map));
				goto failed;
			}

			/* Unmap: */
			if (munmap((caddr_t) mm->mm_start, (size_t) mm->mm_length) == -1) {
				xt_register_ferrno(XT_REG_CONTEXT, errno, xt_file_path(map));
				goto failed;
			}
#endif
			s = stat->ts_flush_start;
			stat->ts_flush_start = 0;
			stat->ts_flush_time += xt_trace_clock() - s;
			stat->ts_flush++;
		}
		mm->mm_start = NULL;
#ifdef XT_WIN
		/* It is possible that a previous remap attempt has failed: the map was closed
		 * but the new map was not allocated (e.g. because of insufficient disk space). 
		 * In this case mm->mm_mapdes will be NULL.
		 */
		if (mm->mm_mapdes && !CloseHandle(mm->mm_mapdes))
			return xt_register_ferrno(XT_REG_CONTEXT, fs_get_win_error(), xt_file_path(map));
		mm->mm_mapdes = NULL;
#endif
		off_t old_size = mm->mm_length;
		mm->mm_length = new_size;

		if (!fs_map_file(mm, map->fr_file, TRUE)) {
			/* Try to restore old mapping */
			mm->mm_length = old_size;
			fs_map_file(mm, map->fr_file, FALSE);
			goto failed;
		}
	}
	return OK;
	
	failed:
	s = stat->ts_flush_start;
	stat->ts_flush_start = 0;
	stat->ts_flush_time += xt_trace_clock() - s;
	return FAILED;
}

/* ----------------------------------------------------------------------
 * Copy files/directories
 */

static void fs_copy_file(XTThreadPtr self, char *from_path, char *to_path, void *copy_buf)
{
	XTOpenFilePtr	from;
	XTOpenFilePtr	to;
	off_t			offset = 0;
	size_t			read_size= 0;

	from = xt_open_file(self, from_path, XT_FT_STANDARD, XT_FS_READONLY, 16*1024);
	pushr_(xt_close_file, from);
	to = xt_open_file(self, to_path, XT_FT_STANDARD, XT_FS_CREATE | XT_FS_MAKE_PATH, 16*1024);
	pushr_(xt_close_file, to);

	for (;;) {
		if (!xt_pread_file(from, offset, 16*1024, 0, copy_buf, &read_size, &self->st_statistics.st_x, self))
			xt_throw(self);
		if (!read_size)
			break;
		if (!xt_pwrite_file(to, offset, read_size, copy_buf, &self->st_statistics.st_x, self))
			xt_throw(self);
		offset += (off_t) read_size;
	}

	freer_();
	freer_();
}

xtPublic void xt_fs_copy_file(XTThreadPtr self, char *from_path, char *to_path)
{
	void *buffer;

	buffer = xt_malloc(self, 16*1024);
	pushr_(xt_free, buffer);
	fs_copy_file(self, from_path, to_path, buffer);
	freer_();
}

static void fs_copy_dir(XTThreadPtr self, char *from_path, char *to_path, void *copy_buf)
{
	XTOpenDirPtr	od;
	char			*file;
	
	xt_add_dir_char(PATH_MAX, from_path);
	xt_add_dir_char(PATH_MAX, to_path);

	pushsr_(od, xt_dir_close, xt_dir_open(self, from_path, NULL));
	while (xt_dir_next(self, od)) {
		file = xt_dir_name(self, od);
		if (*file == '.')
			continue;
#ifdef XT_WIN
		if (strcmp(file, "pbxt-lock") == 0)
			continue;
#endif
		xt_strcat(PATH_MAX, from_path, file);
		xt_strcat(PATH_MAX, to_path, file);
		if (xt_dir_is_file(self, od))
			fs_copy_file(self, from_path, to_path, copy_buf);
		else
			fs_copy_dir(self, from_path, to_path, copy_buf);
		xt_remove_last_name_of_path(from_path);
		xt_remove_last_name_of_path(to_path);
	}
	freer_();

	xt_remove_dir_char(from_path);
	xt_remove_dir_char(to_path);
}

xtPublic void xt_fs_copy_dir(XTThreadPtr self, const char *from, const char *to)
{
	void	*buffer;
	char	from_path[PATH_MAX];
	char	to_path[PATH_MAX];

	xt_strcpy(PATH_MAX, from_path, from);
	xt_strcpy(PATH_MAX, to_path, to);

	buffer = xt_malloc(self, 16*1024);
	pushr_(xt_free, buffer);
	fs_copy_dir(self, from_path, to_path, buffer);
	freer_();
}

