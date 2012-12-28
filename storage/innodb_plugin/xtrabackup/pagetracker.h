/******************************************************
PageTracker: 

Module used for tracking pages that were updated, so that 
incremental backup does not need to read entire db.

This module has 2 types - Bitmap and PageTracker.

(i) The Bitmap type implements dynamic bitmaps that start with size 0 and
resize according to the need.   

(ii) PageTracker type is used for tracking pages changed in one mysql instance.
It has one Bitmap per tablespace. Innodb already numbers the tablespaces 
starting from 0 and the numbering is unique instance-wide. 

xtrabackup.c has code to sniff transaction logs (see xtrabackup_read_logfile)
and invoke pagetracker module as the log records are parsed.

If a file PAGETRACKER_DUMP_CMD_FILE (defined as pagetracker_dump.cmd) is found
in the target-dir (refer xtrabackup args), pagetracker contents are dumped to 
file PAGETRACKER_DUMP_DATA_FILE (defined as pagetracker_dump.data) in the 
target-dir and PAGETRACKER_DUMP_CMD_FILE is removed. Refer the comment header
of PageTracker_dump function for details on the dump format. 

If a file PAGETRACKER_EXIT_CMD_FILE (defined as pagetracker_exit.cmd) is found
in the target-dir (refer xtrabackup args), it exits.  

Created 12/10/2010 
Author Nagavamsi Ponnekanti

*******************************************************/

#include <univ.i>

#define BITS_IN_BYTE 8
#define BITS_IN_IB_UINT64_T (BITS_IN_BYTE *  sizeof(ib_uint64_t)) 


// This type implements dynamic bitmaps that start with size 0 and
// resize according to the need.   
typedef struct bitmap {
	ib_uint64_t *bitmap;
	ib_uint64_t length;
} Bitmap;

// Initializes the bitmap with 0 bits
void Bitmap_init(Bitmap *bm);

// Sets the bit value for the position given. 
// Expands the bitmap if it does not have enough bits. 
// Returns the change in size of bitmap if it was 
// expanded and 0 otherwise.
ib_uint64_t Bitmap_setbit(Bitmap *bm, ib_uint64_t bitpos);


// Gets the bit value for the position given. 
// Returns -1 if the bit does not exist.
int  Bitmap_getbit(Bitmap *bm, ib_uint64_t bitpos);

// Finds a page >= *startpage and < endpage for which the bit is set and puts
// it in *startpage. Returns TRUE if such page exists and FALSE otherwise.  
ibool Bitmap_getnext(Bitmap *bm, ib_uint64_t *startpage, 
		     ib_uint64_t endpage);
	

// This type is used for tracking pages changed in one mysql server.
// It has one bitmap per tablespace. 
// It starts off with 0 bitmaps and increases the #bitmaps dynamically 
// as needed. Each bitmap is also dynamic.
typedef struct pagetracker
{
	Bitmap		*bitmaps;	// one bitmap per tablespace
	ib_uint64_t	nbitmaps;	// #tablespaces
	  
	ib_uint64_t	startLSN;	// LSN at which tracking began
	ib_uint64_t	endLSN;		// LSN at which tracking ended

	const char*	targetdir;	// xtrabackup arg
	ib_uint64_t	size;		// size of pagetracker in bytes

} PageTracker;

// We don't expect pagetracker to grow beyond 64M
#define MAX_PAGETRACKER_SIZE_BYTES (64 * 1024 * 1024)

// Initializes pagetracker for 0 tablespaces (i.e 0 bitmaps)
void PageTracker_init(PageTracker* pt, const char* targetdir);


// Sets the LSN at which page tracking began. 
// Does NOT make sense to call more than once.
void PageTracker_set_start_lsn(PageTracker* pt, ib_uint64_t lsn);


// Sets the LSN at which page tracking completed. 
void PageTracker_set_end_lsn(PageTracker* pt, ib_uint64_t lsn);

// Gets the status of a page ie whether it was updated or not. 
// Returns
//	0 is page was NOT updated
//	1 if page was updated 
// 	-1 if the page# does not fall within the range of bitmap.
int PageTracker_get_page_status(PageTracker* pt, ulint space_id, ib_uint64_t page);

// Sets the bit corresponding to [space, page] passed in.
// Resizes the #bitmaps or the size of bitmap as needed.
void PageTracker_track_page(PageTracker* pt, ulint space_id, ib_uint64_t page);

#define PAGETRACKER_DUMP_CMD_FILE "pagetracker_dump.cmd"
#define PAGETRACKER_DUMP_DATA_FILE "pagetracker_dump.data"
#define PAGETRACKER_EXIT_CMD_FILE "pagetracker_exit.cmd"
#define PAGETRACKER_PID_FILE "pagetracker_pid"


#define PAGETRACKER_NO_DUMP 0
#define PAGETRACKER_DUMP_SUCCESS  1
#define PAGETRACKER_DUMP_FAIL 2

// These are ib_uint64_t offsets AND not byte offsets.
// We write startlsn, nbitmaps and endlsn twice as there is no checksum
// for these 3 numbers.
#define PAGETRACKER_STARTLSN_OFFSET 0
#define PAGETRACKER_ENDLSN_OFFSET 1
#define PAGETRACKER_NBITMAPS_OFFSET 2
#define PAGETRACKER_STARTLSN_OFFSET2 3
#define PAGETRACKER_ENDLSN_OFFSET2 4
#define PAGETRACKER_NBITMAPS_OFFSET2 5
#define PAGETRACKER_1STBITMAP_OFFSET 6

// If targetdir has pagetracker_dump.cmd file, 
// dump bitmaps to pagetracker_dump.data file and 
// remove pagetracker_dump.cmd file. Refer the comment
// header of PageTracker_dump for details on dump format.
// Returns
// 	PAGETRACKER_NO_DUMP		- no dump was attempted
//	PAGETRACKER_DUMP_SUCCESS	- dump succeeded
//	PAGETRACKER_DUMP_FAIL		- dump failed
int PageTracker_check_and_dump(PageTracker* pt);

// If targetdir has pagetracker_exit.cmd file, 
// remove it and shutdown.
void PageTracker_check_and_exit(PageTracker* pt); 

// Creates PAGETRACKER_PID_FILE with process pid in it.
// This also signifies that log copying has begun.
void PageTracker_create_pidfile(PageTracker* pt);


// checks that dump file contents match contents of passed in PageTracker
// Returns TRUE on match and FALSE on mismatch
ibool PageTracker_validate_dump(PageTracker *pt, char* dump_file);


// Gets the bitmap of tracked pages for one tablespace from the pagetracker 
// dump file. Returns TRUE on success and FALSE on failure.
ibool PageTracker_get_bitmap(char* dump_file, ulint space_id, Bitmap* pBitmap,
			     ib_uint64_t* startlsn, ib_uint64_t* endlsn);
