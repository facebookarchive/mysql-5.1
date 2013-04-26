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
 * 2009-07-30	Paul McCullagh
 *
 * H&G2JCtL
 */

#include <pthread.h>


#include <stdlib.h>
#include <stddef.h>
#include <stdio.h>
#include <sys/time.h>
#include <unistd.h>
#include <dirent.h>
#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/types.h>
#include <ctype.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <assert.h>

#ifdef __APPLE__
#define MAC
#endif

/*
 * Define this if I/O should pause.
 */
//#define SHOULD_PAUSE
//#define PERIODIC_FLUSH

#define SIM_RECORD_SIZE			221
#ifdef MAC
#define SIM_FILE_SIZE			(256*1024*1024)
#define SIM_WRITE_AMOUNT		(1*1024*1024)
//#define SIM_WRITE_AMOUNT		(221 * 100)
#else
#define SIM_FILE_SIZE			((off_t) (2L*1024L*1024L*1024L))
#define SIM_WRITE_AMOUNT		(8*1024*1024)
#endif
#define SIM_FLUSH_THRESHOLD		(2*1024*1024)
#define SIM_PAUSE_THRESHOLD		(10*1024*1024)

#ifndef SHOULD_PAUSE
#undef  SIM_PAUSE_THRESHOLD
#define SIM_PAUSE_THRESHOLD		0
#endif

#ifndef PERIODIC_FLUSH
#undef  SIM_FLUSH_THRESHOLD
#define SIM_FLUSH_THRESHOLD		0
#endif

#define my_time					unsigned long long
#define u_long					unsigned long
#define TRUE					1
#define FALSE					0

typedef struct SortedRec {
	off_t		sr_offset;
	int			sr_order;
	char		*sr_data;
} SortedRec;

#define SORTED_MAX_RECORDS		10000
#define SORTED_DATA_SIZE		(SORTED_MAX_RECORDS * SIM_RECORD_SIZE)
#define SORTED_BUFFER_SIZE		(256*1024)

typedef struct RewriteRec {
	off_t		rr_offset;
	off_t		rr_size;
} RewriteRec;

#define REWRITE_MAX_RECORDS		1000
/* This is the maximum distance between to blocks that
 * will cause the blocks to be combined and written
 * as one block!
 */
#ifdef MAC
#define REWRITE_BLOCK_DISTANCE	(64*1024)
#else
#define REWRITE_BLOCK_DISTANCE	(1024*1024)
#endif

#define REWRITE_RECORD_LIMIT	256

typedef struct File {	
	int			file_fh;
	
	int			fi_monitor_index;
	char		fi_file_path[200];
	char		fi_test_name[200];
	char		fi_monitor_name[10];
	int			fi_monitor_active;

	my_time		total_time;

	my_time		flush_start;
	my_time		flush_time;
	u_long		flush_count;

	my_time		last_flush_time;
	u_long		last_flush_count;

	my_time		write_start;
	my_time		write_time;
	u_long		write_count;
	
	off_t		last_block_offset;
	size_t		last_block_size;
	u_long		block_write_count;

	my_time		last_write_time;
	u_long		last_write_count;

	my_time		read_start;
	my_time		read_time;
	u_long		read_count;

	my_time		last_read_time;
	u_long		last_read_count;

	/* Sorted file I/O */
	int			sf_rec_count;
	SortedRec	sf_records[SORTED_MAX_RECORDS];
	size_t		sf_alloc_pos;
	int			sf_order;
	char		sf_data[SORTED_DATA_SIZE];
	char		sf_buffer[SORTED_BUFFER_SIZE];

	/* Re-write sync: */
	off_t		rs_min_block_offset;
	off_t		rs_max_block_offset;
	size_t		rs_flush_block_total;
	size_t		rs_rec_count;
	RewriteRec	rs_records[REWRITE_MAX_RECORDS];

	void		(*fi_write)(struct File *f, void *block, size_t size, off_t start);
	void		(*fi_sync)(struct File *f);
	void		(*fi_write_all)(struct File *f);
} File;

/* -------------------  TIMING ------------------- */

static my_time my_clock(void)
{
	static my_time	my_start_clock = 0;
	struct timeval	tv;
	my_time			now;

	gettimeofday(&tv, NULL);
	now = (my_time) tv.tv_sec * (my_time) 1000000 + tv.tv_usec;
	if (my_start_clock)
		return now - my_start_clock;
	my_start_clock = now;
	return 0;
}

/* -------------------  ERRORS ------------------- */

static void print_error(char *file, int err)
{
	printf("ERROR %s: %s\n", file, strerror(err));
}

static void error_exit(char *file, int err)
{
	print_error(file, err);
	exit(1);
}

static void fatal_error(char *message)
{
	printf("%s", message);
	exit(1);
}

/* -------------------  ERRORS ------------------- */

static void *my_bsearch(void *key, register const void *base, size_t count, size_t size, size_t *idx, int (*compare)(void *key, void *rec))
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

/* -------------------  MONITORING THREAD ------------------- */

/* Monitor file types: */
#define MAX_FILES			10

static int		monitor_file_count;
static int		monitor_files_in_use;
static int		monitor_running;
static	File	*monitor_files[MAX_FILES];

static void monitor_file(File *f, const char *test_name, const char *monitor_name)
{
	if (monitor_file_count == MAX_FILES)
		fatal_error("Too many files to monitor");
	monitor_files[monitor_file_count] = f;
	f->fi_monitor_index = monitor_file_count;
	sprintf(f->fi_file_path, "test-data-%d", (int) monitor_file_count);
	strcpy(f->fi_test_name, test_name);
	strcpy(f->fi_monitor_name, monitor_name);
	monitor_file_count++;
	f->fi_monitor_active = TRUE;
	monitor_files_in_use++;
}

static void unmonitor_file(File *f)
{
	/* Wait for the last activity to be reported! */
	while (monitor_running && f->fi_monitor_active) {
		usleep(100);
	}
	monitor_files_in_use--;
	monitor_files[f->fi_monitor_index] = NULL;
}

static void print_header()
{
	File	*f;
	int		i;

	printf("time ");
	for (i=0; i<MAX_FILES; i++) {
		if ((f = monitor_files[i])) {
			printf("%7s %5s %9s %5s %9s %5s ", f->fi_monitor_name, f->fi_monitor_name, f->fi_monitor_name, f->fi_monitor_name, f->fi_monitor_name, f->fi_monitor_name);
		}
	}
	printf("\n");
	printf("     ");
	for (i=0; i<MAX_FILES; i++) {
		if ((f = monitor_files[i])) {
			printf("%7s %5s %9s %5s %9s %5s ", "flush", "ftime", "write", "wtime", "read", "rtime");
		}
	}
	printf("\n");
}

static void *iotest_monitor(void *data)
{
	File	*f;
	int		i;
	int		row = 0;
	my_time	curr, last;
	my_time	now, fstart, wstart, rstart;
	int		version = 0;
	int		activity;

	my_time	curr_flush_time;
	u_long	curr_flush_count;
	my_time	curr_write_time;
	u_long	curr_write_count;
	my_time	curr_read_time;
	u_long	curr_read_count;

	my_time	flush_time;
	u_long	flush_count;
	my_time	write_time;
	u_long	write_count;
	my_time	read_time;
	u_long	read_count;

	monitor_running = TRUE;
	last = my_clock();
	for (;;) {
		curr = my_clock();
		
		if (!monitor_files_in_use)
			goto wait_phase;

		if ((row % 20) == 0 || version != monitor_file_count) {
			version = monitor_file_count;
			print_header();
		}

		printf("%4.0f ", ((double) curr - (double) last) / (double) 1000);
		activity = FALSE;
		for (i=0; i<MAX_FILES; i++) {
			if ((f = monitor_files[i])) {
				curr_flush_time = f->flush_time;
				fstart = f->flush_start;
				curr_write_time = f->write_time;
				wstart = f->write_start;
				curr_read_time = f->read_time;
				rstart = f->read_start;
				now = my_clock();

				if (fstart)
					curr_flush_time += now - fstart;
				flush_time = curr_flush_time - f->last_flush_time;
				f->last_flush_time = curr_flush_time;

				curr_flush_count = f->flush_count;
				flush_count = curr_flush_count - f->last_flush_count;
				f->last_flush_count = curr_flush_count;

				if (wstart)
					curr_write_time += now - wstart;
				write_time = curr_write_time - f->last_write_time;
				f->last_write_time = curr_write_time;

				curr_write_count = f->write_count;
				write_count = curr_write_count - f->last_write_count;
				f->last_write_count = curr_write_count;
	
				if (rstart)
					curr_read_time += now - rstart;
				read_time = curr_read_time - f->last_read_time;
				f->last_read_time = curr_read_time;

				curr_read_count = f->read_count;
				read_count = curr_read_count - f->last_read_count;
				f->last_read_count = curr_read_count;
	
				printf("%7lu %5.0f %9.2f %5.0f %9.2f %5.0f ", flush_count, (double) flush_time / (double) 1000,
					(double) write_count / (double) 1024, (double) write_time / (double) 1000,
					(double) read_count / (double) 1024, (double) read_time / (double) 1000);
				if (flush_count || flush_time || write_count || write_time || read_count || read_time) {
					f->fi_monitor_active = TRUE;
					activity = TRUE;
				}
				else
					f->fi_monitor_active = FALSE;
			}
		}
		printf("\n");
		row++;

		wait_phase:

		/* Leave the loop, only when there is no more activity. */
		if (!monitor_running && !activity)
			break;

		do {
			usleep(1000);
		} while (my_clock() - curr < 1000000);
		last = curr;
	}
	
	return NULL;
}

/* -------------------  BASIC FILE I/O ------------------- */

#define PREFILL				0
#define SET_EOF				1
#define TRUNCATE			2

#define XT_MASK				((S_IRUSR | S_IWUSR) | (S_IRGRP | S_IWGRP) | (S_IROTH))

static void create_file(File *f, size_t size, int type)
{
	int		fd;
	size_t	tfer;
	char	*block;
	size_t	i;
	off_t	eof;
	off_t	offset;

	if (!(block = (char *) malloc(512)))
		error_exit(f->fi_file_path, errno);
	for (i=0; i<512; i++)
		block[i] = (char) i;

	fd = open(f->fi_file_path, O_CREAT | O_RDWR, XT_MASK);
	if (fd == -1)
		error_exit(f->fi_file_path, errno);

	eof = lseek(fd, 0, SEEK_END);
	if (type == PREFILL && size == eof)
		goto done;

	if (ftruncate(fd, 0) == -1)
		error_exit(f->fi_file_path, errno);

	if (type == SET_EOF) {
		if (size > 512)
			offset = size - 512;
		else
			offset = 0;
		if (pwrite(fd, block, 512, offset) != 512)
			error_exit(f->fi_file_path, errno);
	}
	else {
		offset = 0;
		while (size > 0) {
			tfer = size;
			if (tfer > 512)
				tfer = 512;
			if (pwrite(fd, block, tfer, offset) != tfer)
				error_exit(f->fi_file_path, errno);
			size -= tfer;
			offset += tfer;
		}
	}

	if (fsync(fd) == -1)
		error_exit(f->fi_file_path, errno);

	done:
	close(fd);
	free(block);
}

/*
static void delete_file(char *file)
{
	unlink(file);
}
*/

static void write_file(File *f, void *block, size_t size, off_t offset)
{
	my_time t, s;

	s = my_clock();
	f->write_start = s;
	if (pwrite(f->file_fh, block, size, offset) != size)
		error_exit(f->fi_file_path, errno);
	t = my_clock();
	f->write_start = 0;
	f->write_time += (t - s);
	f->write_count += size;

	/* Does this block touch the previous block? */
	if (f->last_block_offset == -1 ||
		offset < f->last_block_offset ||
		offset > f->last_block_offset + f->last_block_size)
		/* If not, it is a new block (with a gap): */
		f->block_write_count++;
	f->last_block_offset = offset;
	f->last_block_size = size;
}

static void read_file(File *f, void *block, size_t size, off_t start)
{
	my_time t, s;

	s = my_clock();
	f->read_start = s;
	if (pread(f->file_fh, block, size, start) != size)
		error_exit(f->fi_file_path, errno);
	t = my_clock();
	f->read_start = 0;
	f->read_time += (t - s);
	f->read_count += size;
}

static void sync_file(File *f)
{
	my_time t, s;

	s = my_clock();
	f->flush_start = s;
	if (fsync(f->file_fh) == -1)
		error_exit(f->fi_file_path, errno);
	t = my_clock();
	f->flush_start = 0;
	f->flush_time += t - s;
	f->flush_count++;
}

static void new_file(File **ret_f, const char *test_name, const char *monitor_name)
{
	File *f;

	f = malloc(sizeof(File));
	memset(f, 0, sizeof(File));
	f->last_block_offset = (off_t) -1;
	f->rs_min_block_offset = (off_t) -1;

	monitor_file(f, test_name, monitor_name);
	f->fi_write = write_file;
	f->fi_sync = sync_file;

	*ret_f = f;
}

static void open_file(File *f)
{
	f->file_fh = open(f->fi_file_path, O_RDWR, 0);
	if (f->file_fh == -1)
		error_exit(f->fi_file_path, errno);

	f->total_time = my_clock();
}

static void close_file(File *f)
{
	f->fi_sync(f);
	f->total_time = my_clock() - f->total_time;
	if (f->file_fh != -1)
		close(f->file_fh);

	unmonitor_file(f);

	printf("\n=* TEST: %s (%s) *=\n", f->fi_test_name, f->fi_monitor_name);
	printf("Written K:  %.2f\n", (double) f->write_count / (double) 1024);
	printf("Run time:   %.2f ms\n", (double) f->total_time / (double) 1000);
	if (f->rs_flush_block_total > 0)
		printf("Flush blks: %lu\n", f->rs_flush_block_total);
	if (f->write_count > 0) {
		printf("Tot blocks: %lu\n", f->block_write_count);
		printf("Seek time:  %.3f ms\n", (double) f->flush_time / (double) f->block_write_count / (double) 1000);
	}
	printf("\n");
	if (f->write_time)
		printf("Write K/s: %.2f\n", (double) f->write_count * (double) 1000000 / (double) 1024 / (double) f->write_time);
	if (f->read_time)
		printf("Read  K/s: %.2f\n", (double) f->read_count * (double) 1000000 / (double) 1024 / (double) f->read_time);
	if (f->flush_time)
		printf("Flush K/s: %.2f\n", (double) f->write_count * (double) 1000000 / (double) 1024 / (double) f->flush_time);
	if (f->write_time + f->read_time + f->flush_time)
		printf("Total K/s: %.2f\n", (double) f->write_count * (double) 1000000 / (double) 1024 / ((double) f->write_time + (double) f->read_time + (double) f->flush_time));
	printf("=*=\n");
}

/* -------------------  SORTED I/O ------------------- */

/* Sort records before they are writing.
 * Options are also added to write records,
 * in large blocks. This requires reading the block
 * first.
 */

static int compare_rec(const void *a, const void *b)
{
	SortedRec	*ra = (SortedRec *) a;
	SortedRec	*rb = (SortedRec *) b;

	if (ra->sr_offset == rb->sr_offset) {
		if (ra->sr_order == rb->sr_order)
			return 0;
		if (ra->sr_order < rb->sr_order)
			return -1;
		return 1;
	}
	if (ra->sr_offset < rb->sr_offset)
		return -1;
	return 1;
}

static void sorted_write_all(File *f)
{
	SortedRec	*rec;
	int			i;

	qsort(f->sf_records, f->sf_rec_count, sizeof(SortedRec), compare_rec);
	rec = f->sf_records;
	for (i=0; i<f->sf_rec_count; i++) {
		write_file(f, rec->sr_data, SIM_RECORD_SIZE, rec->sr_offset);
		rec++;
	}
	
	f->sf_rec_count = 0;
	f->sf_alloc_pos = 0;
	f->sf_order = 0;
}

static void sorted_write_rw_all(File *f)
{
	SortedRec	*rec;
	int			i;
	off_t		offset = 0;
	size_t		size = 0;

	qsort(f->sf_records, f->sf_rec_count, sizeof(SortedRec), compare_rec);
	rec = f->sf_records;
	for (i=0; i<f->sf_rec_count; i++) {
		reread:
		if (!size) {
			offset = (rec->sr_offset / 1024) * 1024;
			size = SORTED_BUFFER_SIZE;
			if (offset+(off_t)size > SIM_FILE_SIZE)
				size = (size_t) (SIM_FILE_SIZE - offset);
			read_file(f, f->sf_buffer, size, offset);
		}

		if (rec->sr_offset >= offset && rec->sr_offset+SIM_RECORD_SIZE <= offset+size)
			memcpy(&f->sf_buffer[rec->sr_offset - offset], rec->sr_data, SIM_RECORD_SIZE);
		else {
			write_file(f, f->sf_buffer, size, offset);
			size = 0;
			goto reread;
		}
		rec++;
	}
	
	f->sf_rec_count = 0;
	f->sf_alloc_pos = 0;
	f->sf_order = 0;
}

static void sorted_write_rw_no_gaps_all(File *f)
{
	SortedRec	*rec;
	int			i;
	off_t		offset;
	size_t		size = 0;

	qsort(f->sf_records, f->sf_rec_count, sizeof(SortedRec), compare_rec);
	rec = f->sf_records;
	if (!f->sf_rec_count)
		goto done;

	offset = (rec->sr_offset / 1024) * 1024;
	for (i=0; i<f->sf_rec_count; i++) {
		reread:
		if (!size) {
			size = SORTED_BUFFER_SIZE;
			if (offset+size > SIM_FILE_SIZE)
				size = SIM_FILE_SIZE - offset;
			read_file(f, f->sf_buffer, size, offset);
		}

		if (rec->sr_offset >= offset && rec->sr_offset+SIM_RECORD_SIZE <= offset+size)
			memcpy(&f->sf_buffer[rec->sr_offset - offset], rec->sr_data, SIM_RECORD_SIZE);
		else {
			write_file(f, f->sf_buffer, size, offset);
			offset += size;
			if (rec->sr_offset < offset)
				offset = (rec->sr_offset / 1024) * 1024;
			size = 0;
			goto reread;
		}
		rec++;
	}

	done:
	f->sf_rec_count = 0;
	f->sf_alloc_pos = 0;
	f->sf_order = 0;
}

static void sorted_sync_file(File *f)
{
	f->fi_write_all(f);
	sync_file(f);
}

static void sorted_write_file(File *f, void *block, size_t size, off_t offset)
{
	SortedRec *rec;

	if (size != SIM_RECORD_SIZE)
		printf("ooops\n");

	if (f->sf_rec_count == SORTED_MAX_RECORDS ||
		f->sf_alloc_pos + size > SORTED_DATA_SIZE) {
		f->fi_write_all(f);
	}

	rec = &f->sf_records[f->sf_rec_count];
	rec->sr_offset = offset;
	rec->sr_order = f->sf_order;
	rec->sr_data = &f->sf_data[f->sf_alloc_pos];
	memcpy(rec->sr_data, block, size);
	f->sf_alloc_pos += size;
	f->sf_rec_count++;
	f->sf_order++;
}

/* -------------------  RE-WRITE FLUSH ------------------- */

/* The idea is that it is better to re-write the file
 * sequentially, then allow the FS to write scattered
 * dirty blocks!
 *
 * This comes from the fact that seeking is a lot more
 * expensive than sequential write.
 */

static void rewrite_all_sync_file(File *f)
{
	off_t	offset = 0;
	size_t	size = 0;

	while (offset < SIM_FILE_SIZE) {
		size = SORTED_BUFFER_SIZE;
		if (offset + size > SIM_FILE_SIZE)
			size = SIM_FILE_SIZE - offset;
		read_file(f, f->sf_buffer, size, offset);
		write_file(f, f->sf_buffer, size, offset);
		offset += size;
	}

	sync_file(f);
}

static void rewrite_min_max_sync_file(File *f)
{
	off_t	offset = 0;
	size_t	size = 0;
	off_t	eof = SIM_FILE_SIZE;

	if (f->rs_min_block_offset != (off_t) -1)
		offset = f->rs_min_block_offset / 1024 * 1024;
	eof = (f->rs_max_block_offset + 1023) / 1024 * 1024;
	if (eof > SIM_FILE_SIZE)
		eof = SIM_FILE_SIZE;
	while (offset < eof) {
		size = SORTED_BUFFER_SIZE;
		if (offset + size > eof)
			size = eof - offset;
		read_file(f, f->sf_buffer, size, offset);
		write_file(f, f->sf_buffer, size, offset);
		offset += size;
	}

	sync_file(f);
}

static void rewrite_min_max_write_file(File *f, void *block, size_t size, off_t offset)
{
	off_t		top_offset;

	write_file(f, block, size, offset);

	/* Round up and down by 1K */
	top_offset = offset + size;
	offset = offset / 1024 * 1024;
	top_offset = (top_offset + 1023) / 1024 * 1024;
	size = (size_t) (top_offset - offset);

	/* Calculate max and min and max offset: */
	if (f->rs_min_block_offset == (off_t) -1 || offset < f->rs_min_block_offset)
		f->rs_min_block_offset = offset;
	if (offset + size > f->rs_max_block_offset)
		f->rs_max_block_offset = offset + size;
}

static void rewrite_opt_rewrite_file(File *f)
{
	RewriteRec	*rec;
	int			i;
	off_t		offset;
	off_t		size;
	size_t		tfer;

	/* Re-write all areas written: */
	rec = f->rs_records;
	for (i=0; i<f->rs_rec_count; i++) {
		size = rec->rr_size;
		offset = rec->rr_offset;
		while (size) {
			tfer = SORTED_BUFFER_SIZE;
			if ((off_t) tfer > size)
				tfer = size;
			read_file(f, f->sf_buffer, tfer, offset);
			write_file(f, f->sf_buffer, tfer, offset);
			offset += tfer;
			size -= tfer;
		}
		rec++;
	}

	f->rs_flush_block_total += f->rs_rec_count;
	f->rs_rec_count = 0;
}

static int rewrite_opt_comp(void *k, void *r)
{
	register off_t		*key = (off_t *) k;
	register RewriteRec	*rec = (RewriteRec *) r;

	if (*key == rec->rr_offset)
		return 0;
	if (*key < rec->rr_offset)
		return -1;
	return 1;
}

static void rewrite_opt_sync_file(File *f)
{
	rewrite_opt_rewrite_file(f);
	sync_file(f);
}

static void rewrite_opt_write_file(File *f, void *block, size_t size, off_t offset)
{
	RewriteRec	*rec;
	size_t		idx;
	off_t		top_offset;

	write_file(f, block, size, offset);

	/* Round up and down by 1K */
	top_offset = offset + size;
	offset = offset / 1024 * 1024;
	top_offset = (top_offset + 1023) / 1024 * 1024;
	size = (size_t) (top_offset - offset);

	if ((rec = my_bsearch(&offset, f->rs_records, f->rs_rec_count, sizeof(RewriteRec), &idx, rewrite_opt_comp))) {
		if ((off_t) size > rec->rr_size)
			rec->rr_size = size;
		goto merge_right;
	}

	if (idx == 0) {
		/* The offset is before the first entry. */
		if (idx < f->rs_rec_count) {
			/* There is a first entry: */
			rec = f->rs_records;
			if (rec->rr_offset - (offset + size) < REWRITE_BLOCK_DISTANCE)
				goto add_to_right;
		}

		/* Add the first entry: */
		goto add_the_entry;
	}

	/* Not the first entry: */
	idx--;
	rec = f->rs_records + idx;

	if (offset - (rec->rr_offset + rec->rr_size) < REWRITE_BLOCK_DISTANCE) {
		/* Add to block on left: */
		size = (offset + size) - rec->rr_offset;
		if (size > rec->rr_size)
			rec->rr_size = size;
		goto merge_right;
	}
	
	idx++;
	rec = f->rs_records + idx;

	if (idx < f->rs_rec_count && rec->rr_offset - (offset + size) < REWRITE_BLOCK_DISTANCE)
		goto add_to_right;

	add_the_entry:
	if (f->rs_rec_count == REWRITE_MAX_RECORDS) {
		rewrite_opt_rewrite_file(f);
		idx = 0;
	}
	rec = f->rs_records + idx;
	memmove(rec+1, rec, (f->rs_rec_count - idx) * sizeof(RewriteRec));
	rec->rr_offset = offset;
	rec->rr_size = (off_t) size;
	f->rs_rec_count++;
	return;

	add_to_right:
	rec->rr_size += rec->rr_offset - offset;
	if (size > rec->rr_size)
		rec->rr_size = size;
	rec->rr_offset = offset;

	merge_right:
	if (idx+1 < f->rs_rec_count) {
		/* There is a record right: */
		if (rec->rr_offset + rec->rr_size + REWRITE_BLOCK_DISTANCE > (rec+1)->rr_offset) {
			/* Merge and remove! */
			size = (rec+1)->rr_size + ((rec+1)->rr_offset - rec->rr_offset);
			if (size > rec->rr_size)
				rec->rr_size = size;
			f->rs_rec_count--;
			assert(f->rs_rec_count > idx);
			memmove(rec+1, rec+2, (f->rs_rec_count - idx - 1) * sizeof(RewriteRec));
		}
	}
	return;
	
}

static void rewrite_limit_sync_file(File *f)
{
	rewrite_opt_rewrite_file(f);
	sync_file(f);
}

/*
 * This options is like opt but it limits the number of
 * blocks that can be written.
 */
static void rewrite_limit_write_file(File *f, void *block, size_t size, off_t offset)
{
	RewriteRec	*rec;

	/* There must always be room for one more: */
	assert(f->rs_rec_count < REWRITE_RECORD_LIMIT);
	rewrite_opt_write_file(f, block, size, offset);
	if (f->rs_rec_count == REWRITE_RECORD_LIMIT) {
		/* Consolidate 2 blocks that are closest to each other in other to
		 * make space for another block:
		 */
		int		i, idx;
		off_t	gap;
		off_t	min_gap = (off_t) -1;

		rec = f->rs_records;
		for (i=0; i<f->rs_rec_count-1; i++) {
			gap = (rec+1)->rr_offset - (rec->rr_offset + rec->rr_size);
			if (min_gap == (off_t) -1 || gap < min_gap) {
				idx = i;
				min_gap = gap;
			}
			rec++;
		}

		/* Merge this with the next: */
		rec = f->rs_records + idx;
		size = (rec+1)->rr_size + ((rec+1)->rr_offset - rec->rr_offset);
		if (size > rec->rr_size)
			rec->rr_size = size;
		f->rs_rec_count--;
		assert(f->rs_rec_count > idx);
		memmove(rec+1, rec+2, (f->rs_rec_count - idx - 1) * sizeof(RewriteRec));
	}	
}

/* -------------------  SIMULATION I/O ------------------- */

/*
static void random_read_bytes(File *f, off_t file_size, size_t size, size_t count, int print)
{
	char	*block;
	size_t	i;
	off_t	offset;
	long	x = (long) (file_size / (off_t) size), y;
	
	block = (char *) malloc(size);

	for (i=0; i<count; i++) {
		y = random() % x;
		offset = (off_t) y * (off_t) size;
		if (offset+size > file_size) {
			printf("NOOO\n");
			exit(1);
		}
		read_file(f, block, size, offset);
		if (print) {
			if ((i%100) == 0)
				printf("%ld\n", (long) i);
		}
	}
	free(block);
}
*/

static void read_write_bytes(File *f, off_t file_size)
{
	off_t	offset = 0;
	size_t	tfer;

	while (file_size > 0) {
		tfer = SORTED_BUFFER_SIZE;
		if ((off_t) tfer > file_size)
			tfer = (size_t) file_size;
		read_file(f, f->sf_buffer, tfer, offset);
		f->fi_write(f, f->sf_buffer, tfer, offset);
		offset += tfer;
		file_size -= tfer;
	}
}

static void random_write_bytes(File *f, off_t file_size, size_t size, off_t amount_to_write, int sync_after, int pause_after)
{
	char	*block;
	off_t	offset;
	long	x = (long) (file_size / (off_t) size), y;
	long	pbytes_written = 0;
	long	sbytes_written = 0;
	off_t	total_written = 0;
	int		i;
	
	block = (char *) malloc(size);
	for (i=0; i<size; i++)
		block[i] = (char) i;

	if (pause_after) {
		pbytes_written = 0;
		sleep(10);
	}
	while (total_written < amount_to_write) {
		y = random() % x;
		do {
			offset = (off_t) y * (off_t) size;
		}
		while (offset+size > file_size);
		f->fi_write(f, block, size, offset);
		sbytes_written += size;
		pbytes_written += size;
		total_written += size;
		if (sync_after && sbytes_written > sync_after) {
			sbytes_written = 0;
			f->fi_sync(f);
		}
		if (pause_after && pbytes_written > pause_after) {
			pbytes_written = 0;
			sleep(10);
		}
	}
	free(block);
}

static void seq_write_bytes(File *f, off_t file_size, size_t size, off_t amount_to_write, int sync_after, int pause_after)
{
	char    *block;
	off_t   offset;
	long    pbytes_written = 0;
	long    sbytes_written = 0;
	off_t   total_written = 0;
	int     i;

	block = (char *) malloc(size);
	for (i=0; i<size; i++)
		block[i] = (char) i;

	if (pause_after) {
		pbytes_written = 0;
		sleep(10);
	}
	offset = 0;
	while (total_written < amount_to_write) {
		if (offset+size > file_size)
			offset = 0;
		f->fi_write(f, block, size, offset);
		sbytes_written += size;
		pbytes_written += size;
		total_written += size;
		offset += size;
		if (sync_after && sbytes_written > sync_after) {
			sbytes_written = 0;
			f->fi_sync(f);
		}
		if (pause_after && pbytes_written > pause_after) {
			pbytes_written = 0;
			sleep(10);
		}
	}
	free(block);
}

static void simulate_xlog_write(File *f, off_t eof, off_t start_point, size_t size, off_t amount_to_write)
{
	char	*block;
	off_t	start = start_point;
	off_t	offset;
	size_t	tfer;
	off_t	total_written = 0;
	int		i;
	
	block = (char *) malloc(1024*16);
	for (i=0; i<1024*16; i++)
		block[i] = (char) i;

	while (total_written < amount_to_write) {
		/* Write 512 byte boundary block around random data we wish to write. */
		offset = (start / 512) * 512;
		tfer = (((start + size) / 512) * 512 + 512) - offset;
		f->fi_write(f, block, tfer, offset);
		total_written += tfer;
		f->fi_sync(f);
		start += size;
		if (start + size > eof)
			start = start_point;
	}

	free(block);
}

static void seq_write_blocks_aligned(File *f, off_t file_size, size_t block_size, off_t amount_to_write)
{
	char	*block;
	off_t	i, no_of_writes;
	off_t	offset, inc;

	/* Blocks, 1 K in size: */
	block_size = block_size / 1024 * 1024;

	block = (char *) malloc(block_size);
	for (i=0; i<block_size; i++)
		block[i] = (char) i;

	no_of_writes = amount_to_write / block_size;

	/* Calculate and round increment: */
	inc = (file_size / no_of_writes) / 1024 * 1024;

	/* This is required if we want a gap: */
	assert(inc > block_size);

	offset = 0;
	for (i=0; i<no_of_writes; i++) {
		f->fi_write(f, block, block_size, offset);
		offset += inc;
	}
}

/* ------------------- TESTS ------------------- */

static void *test_xlog_write_prealloc(void *data)
{
	File	*f;

	new_file(&f, __FUNCTION__, "xlog");
	create_file(f, SIM_FILE_SIZE, PREFILL);
	open_file(f);
	simulate_xlog_write(f, SIM_FILE_SIZE, 0, SIM_RECORD_SIZE, SIM_WRITE_AMOUNT);
	close_file(f);
	return NULL;
}

static void *test_xlog_write_no_prealloc(void *data)
{
	File	*f;

	new_file(&f, __FUNCTION__, "xlog");
	create_file(f, 512, TRUNCATE);
	open_file(f);
	simulate_xlog_write(f, SIM_FILE_SIZE, 0, SIM_RECORD_SIZE, SIM_WRITE_AMOUNT);
	close_file(f);
	return NULL;
}

static void *test_xlog_write_set_eof(void *data)
{
	File	*f;

	new_file(&f, __FUNCTION__, "xlog");
	create_file(f, SIM_FILE_SIZE, SET_EOF);
	open_file(f);
	simulate_xlog_write(f, SIM_FILE_SIZE, 0, SIM_RECORD_SIZE, SIM_WRITE_AMOUNT);
	close_file(f);
	return NULL;
}

static void *test_rnd_write_prealloc(void *data)
{
	File	*f;

	new_file(&f, __FUNCTION__, "rndw");
	create_file(f, SIM_FILE_SIZE, PREFILL);
	open_file(f);
	random_write_bytes(f, SIM_FILE_SIZE, SIM_RECORD_SIZE, SIM_WRITE_AMOUNT, SIM_FLUSH_THRESHOLD, SIM_PAUSE_THRESHOLD);
	close_file(f);
	return NULL;
}

static void *test_rnd_write_no_prealloc(void *data)
{
	File    *f;

	new_file(&f, __FUNCTION__, "rndw");
	create_file(f, 0, TRUNCATE);
	open_file(f);
	random_write_bytes(f, SIM_FILE_SIZE, SIM_RECORD_SIZE, SIM_WRITE_AMOUNT, SIM_FLUSH_THRESHOLD, SIM_PAUSE_THRESHOLD);
	close_file(f);
	return NULL;
}

static void *test_rnd_write_rewrite_all_sync_prealloc(void *data)
{
	File	*f;

	new_file(&f, __FUNCTION__, "rndw");
	create_file(f, SIM_FILE_SIZE, PREFILL);
	open_file(f);
	f->fi_sync = rewrite_all_sync_file;
	random_write_bytes(f, SIM_FILE_SIZE, SIM_RECORD_SIZE, SIM_WRITE_AMOUNT, SIM_FLUSH_THRESHOLD, SIM_PAUSE_THRESHOLD);
	close_file(f);
	return NULL;
}

static void *test_rnd_write_rewrite_min_max_sync_prealloc(void *data)
{
	File	*f;

	new_file(&f, __FUNCTION__, "rndw");
	create_file(f, SIM_FILE_SIZE, PREFILL);
	open_file(f);
	f->fi_write = rewrite_min_max_write_file;
	f->fi_sync = rewrite_min_max_sync_file;
	random_write_bytes(f, SIM_FILE_SIZE, SIM_RECORD_SIZE, SIM_WRITE_AMOUNT, SIM_FLUSH_THRESHOLD, SIM_PAUSE_THRESHOLD);
	close_file(f);
	return NULL;
}

static void *test_rnd_write_rewrite_opt_sync_prealloc(void *data)
{
	File	*f;

	new_file(&f, __FUNCTION__, "rndw");
	create_file(f, SIM_FILE_SIZE, PREFILL);
	open_file(f);
	f->fi_write = rewrite_opt_write_file;
	f->fi_sync = rewrite_opt_sync_file;
	random_write_bytes(f, SIM_FILE_SIZE, SIM_RECORD_SIZE, SIM_WRITE_AMOUNT, SIM_FLUSH_THRESHOLD, SIM_PAUSE_THRESHOLD);
	close_file(f);
	return NULL;
}

static void *test_rnd_write_rewrite_limit_sync_prealloc(void *data)
{
	File	*f;

	new_file(&f, __FUNCTION__, "rndw");
	create_file(f, SIM_FILE_SIZE, PREFILL);
	open_file(f);
	f->fi_write = rewrite_limit_write_file;
	f->fi_sync = rewrite_limit_sync_file;
	random_write_bytes(f, SIM_FILE_SIZE, SIM_RECORD_SIZE, SIM_WRITE_AMOUNT, SIM_FLUSH_THRESHOLD, SIM_PAUSE_THRESHOLD);
	close_file(f);
	return NULL;
}

static void *test_rnd_write_sorted_prealloc(void *data)
{
	File	*f;

	new_file(&f, __FUNCTION__, "rndsw");
	create_file(f, SIM_FILE_SIZE, PREFILL);
	open_file(f);
	f->fi_write = sorted_write_file;
	f->fi_sync = sorted_sync_file;
	f->fi_write_all = sorted_write_all;
	f->fi_write_all = sorted_write_rw_all;
	f->fi_write_all = sorted_write_rw_no_gaps_all;
	random_write_bytes(f, SIM_FILE_SIZE, SIM_RECORD_SIZE, SIM_WRITE_AMOUNT, SIM_FLUSH_THRESHOLD, SIM_PAUSE_THRESHOLD);
	close_file(f);
	return NULL;
}

static void *test_seq_write_prealloc(void *data)
{
	File	*f;

	new_file(&f, __FUNCTION__, "seqw");
	create_file(f, SIM_FILE_SIZE, PREFILL);
	open_file(f);
	seq_write_bytes(f, SIM_FILE_SIZE, SIM_RECORD_SIZE, SIM_WRITE_AMOUNT, SIM_FLUSH_THRESHOLD, SIM_PAUSE_THRESHOLD);
	close_file(f);
	return NULL;
}

static void *test_seq_write_no_prealloc(void *data)
{
	File    *f;

	new_file(&f, __FUNCTION__, "seqw");
	create_file(f, 0, TRUNCATE);
	open_file(f);
	seq_write_bytes(f, SIM_FILE_SIZE, SIM_RECORD_SIZE, SIM_WRITE_AMOUNT, SIM_FLUSH_THRESHOLD, SIM_PAUSE_THRESHOLD);
	close_file(f);
	return NULL;
}

static void *test_seq_rw_prealloc(void *data)
{
	File	*f;

	new_file(&f, __FUNCTION__, "seqrw");
	create_file(f, SIM_FILE_SIZE, PREFILL);
	open_file(f);
	read_write_bytes(f, SIM_FILE_SIZE);
	close_file(f);
	return NULL;
}

int block_write_block_count = 128;

static void *test_block_write_aligned_prealloc(void *data)
{
	File	*f;
	off_t	write_amount = SIM_FILE_SIZE / 2;

	new_file(&f, __FUNCTION__, "block");
	create_file(f, SIM_FILE_SIZE, PREFILL);
	open_file(f);
	seq_write_blocks_aligned(f, SIM_FILE_SIZE, write_amount / block_write_block_count, write_amount);
	close_file(f);
	return NULL;
}

/* -------------------  THREADING ------------------- */

static pthread_t run_task_as_thread(void *(*task)(void *))
{
	pthread_t	thread;
	int			err;

	err = pthread_create(&thread, NULL, task, NULL);
	if (err)
		error_exit("pthread_create", err);
	return thread;
}

static void wait_for_task(pthread_t thread)
{
	void *value;

	pthread_join(thread, &value);
}

/* -------------------  MAIN ------------------- */

static void reference_tests()
{
	test_xlog_write_prealloc(NULL);
	test_xlog_write_no_prealloc(NULL);
	test_xlog_write_set_eof(NULL);

	test_rnd_write_prealloc(NULL);
	test_rnd_write_no_prealloc(NULL);
	test_rnd_write_rewrite_all_sync_prealloc(NULL);
	test_rnd_write_rewrite_min_max_sync_prealloc(NULL);
	test_rnd_write_rewrite_opt_sync_prealloc(NULL);
	test_rnd_write_rewrite_limit_sync_prealloc(NULL);

	test_rnd_write_sorted_prealloc(NULL);

	test_seq_write_prealloc(NULL);
	test_seq_write_no_prealloc(NULL);
	test_seq_rw_prealloc(NULL);

	test_block_write_aligned_prealloc(NULL);
}

static void do_task(void *(*task)(void *))
{
	pthread_t t1;
	t1 = run_task_as_thread(task);
	wait_for_task(t1);
}

static void do_test_1()
{
	pthread_t t1;
	//pthread_t t2;

	t1 = run_task_as_thread(test_rnd_write_sorted_prealloc);
	//t2 = run_task_as_thread(test_xlog_write_prealloc);

	wait_for_task(t1);
	//wait_for_task(t2);
}

static void do_test_2()
{
	//do_task(test_rnd_write_prealloc);
	//do_task(test_rnd_write_rewrite_min_max_sync_prealloc);
	do_task(test_rnd_write_rewrite_limit_sync_prealloc);

	/*
	block_write_block_count = 1;
	do_task(test_block_write_aligned_prealloc);
	block_write_block_count = 2;
	do_task(test_block_write_aligned_prealloc);
	block_write_block_count = 4;
	do_task(test_block_write_aligned_prealloc);
	block_write_block_count = 8;
	do_task(test_block_write_aligned_prealloc);
	block_write_block_count = 16;
	do_task(test_block_write_aligned_prealloc);
	block_write_block_count = 32;
	do_task(test_block_write_aligned_prealloc);
	block_write_block_count = 64;
	do_task(test_block_write_aligned_prealloc);
	block_write_block_count = 128;
	do_task(test_block_write_aligned_prealloc);
	*/
}

int main(int argc, char **argv)
{
	//pthread_t m;
	
	//m = run_task_as_thread(iotest_monitor);

	do_test_2();

	monitor_running = FALSE;
	//wait_for_task(m);
    return 0;
}
