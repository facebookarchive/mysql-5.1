#include <stdio.h>
#include <string.h>
#include <pagetracker.h>
#include <os0file.h>

// 64 bit 1
const ib_uint64_t one = 1;
 
char	path_dumpcmd[FN_REFLEN];
char	path_dumpdata[FN_REFLEN];
char	path_exitcmd[FN_REFLEN];
char	path_pidfile[FN_REFLEN];


// Initializes the bitmap with 0 bits
void Bitmap_init(Bitmap* bm)
{
	bm->length = 0;
	bm->bitmap = NULL;
}

// Gets the bit value for the position given. 
// Returns -1 if the bit does not exist.  
int Bitmap_getbit(Bitmap* bm, ib_uint64_t bitpos)
{
	ib_uint64_t index = bitpos / BITS_IN_IB_UINT64_T;
	ib_uint64_t bitpos_in_uint64 = bitpos - index * BITS_IN_IB_UINT64_T; 

	if (index >= bm->length) {
		return -1;
	}

	return ((bm->bitmap[index] & (one << bitpos_in_uint64)) ? 1 : 0);
}


// Sets the bit value for the position given. 
// Expands the bitmap if it does not have enough bits.
// Returns the change in size of bitmap if it was 
// expanded and 0 otherwise.
ib_uint64_t Bitmap_setbit(Bitmap* bm, ib_uint64_t bitpos)
{		
	ib_uint64_t index = bitpos / BITS_IN_IB_UINT64_T;
	ib_uint64_t bitpos_in_uint64 = bitpos - index * BITS_IN_IB_UINT64_T; 
	ib_uint64_t size_increase = 0;
	
	if (index >= bm->length) {
		ib_uint64_t *new_bitmap = NULL;
		ib_uint64_t new_length = 2*index + 1;

		ut_a(sizeof(ib_uint64_t) * new_length < MAX_PAGETRACKER_SIZE_BYTES);
		new_bitmap = (ib_uint64_t *) realloc (bm->bitmap,
						      sizeof(ib_uint64_t) * new_length);

		// zero the rest
		size_increase = sizeof(ib_uint64_t) * (new_length - bm->length);
		memset(&new_bitmap[bm->length], 0, size_increase);

		bm->bitmap = new_bitmap;
		bm->length = new_length;
		new_bitmap = NULL;
	}

	// Now we should have enough bits to set the bit we want
	bm->bitmap[index] |= (one << bitpos_in_uint64);
	return size_increase;
}


// Finds a page >= *startpage and < endpage for which the bit is set and puts
// it in *startpage. Returns TRUE if such page exists and FALSE otherwise. 
ibool Bitmap_getnext(Bitmap *bm, ib_uint64_t *startpage, ib_uint64_t endpage)
{
	ib_uint64_t pagenum = *startpage;
	ib_uint64_t index = pagenum / BITS_IN_IB_UINT64_T;
	ib_uint64_t bitpos_in_uint64 = pagenum - index * BITS_IN_IB_UINT64_T;

	while ((index < bm->length) && (pagenum < endpage)) {
		if (!(bm->bitmap[index])) {
			pagenum += (64 - bitpos_in_uint64);
		} else {
			while ((bitpos_in_uint64 <= 63) &&
			       (pagenum < endpage)) {
				if (bm->bitmap[index] & 
				    (one << bitpos_in_uint64)) {
					*startpage = pagenum;
					return TRUE;
				}
				pagenum++;
				bitpos_in_uint64++;
			}
		}

		index++;
		bitpos_in_uint64 = 0;
	}

	return FALSE;
}

// Initializes path by concatenating targetdir and filename.
// Assumes that path is of length FN_REFLEN. 
void PageTracker_init_path(PageTracker* pt, char* path, char* filename)
{
	if ((strlen(pt->targetdir) + strlen(filename) + 1) < FN_REFLEN) {
		sprintf(path, "%s/%s", pt->targetdir, filename);
	} else {
		fprintf(stderr, "Pagetracker: Path of %s too long\n", filename);
		exit(1);
	}

}

// Initializes pagetracker for 0 tablespaces (i.e 0 bitmaps)
void PageTracker_init(PageTracker* pt, const char* targetdir)
{
	pt->nbitmaps = 0;
	pt->bitmaps = NULL;
	pt->startLSN = 0;
	pt->endLSN = 0;
	pt->targetdir = targetdir;
	pt->size = sizeof(PageTracker);

	PageTracker_init_path(pt, path_dumpdata, PAGETRACKER_DUMP_DATA_FILE);
	PageTracker_init_path(pt, path_dumpcmd, PAGETRACKER_DUMP_CMD_FILE);
	PageTracker_init_path(pt, path_exitcmd, PAGETRACKER_EXIT_CMD_FILE);
	PageTracker_init_path(pt, path_pidfile, PAGETRACKER_PID_FILE);
}

// Sets the LSN at which page tracking began. 
// Does NOT make sense to call more than once.
void PageTracker_set_start_lsn(PageTracker* pt, ib_uint64_t lsn)
{
	ut_a(!pt->startLSN);
	pt->startLSN = lsn;
	printf("PageTracker start: (lsn=%llu) (startLSN=%llu)\n", 
	       lsn, pt->startLSN);
}

// Sets the LSN at which page tracking completed. 
void PageTracker_set_end_lsn(PageTracker* pt, ib_uint64_t lsn)
{
	pt->endLSN = lsn;
}

// Gets the status of a page ie whether it was updated or not. 
// Returns
//	0 is page was NOT updated
//	1 if page was updated 
// 	-1 if the page# does not fall within the range of bitmap.
int 
PageTracker_get_page_status(PageTracker* pt, ulint space_id, ib_uint64_t page)
{
	if (space_id >= pt->nbitmaps) { 
		return -1;
	}

	return Bitmap_getbit(&(pt->bitmaps[space_id]), page);
}


// Sets the bit corresponding to [space, page] passed in.
// Resizes the #bitmaps or the size of bitmap as needed.
void PageTracker_track_page(PageTracker* pt, ulint space_id, ib_uint64_t page)
{
	ib_uint64_t size_increase = 0;
	// printf("PageTracker tracking space=%lu, page=%llu\n", space_id, page);
		
	if (space_id >= pt->nbitmaps) {
		ib_uint64_t new_nbitmaps = 2*space_id + 1;
		Bitmap *new_bitmaps;
		size_increase = sizeof(Bitmap) * (new_nbitmaps - pt->nbitmaps);
		pt->size += size_increase;
		ut_a(pt->size <= MAX_PAGETRACKER_SIZE_BYTES);

		new_bitmaps = (Bitmap *) realloc (pt->bitmaps,
						  sizeof(Bitmap) * new_nbitmaps);

		// set the rest to 0
		memset(&new_bitmaps[pt->nbitmaps], 0, size_increase);

		pt->bitmaps = new_bitmaps;
		pt->nbitmaps = new_nbitmaps;
		new_bitmaps = NULL;			
	}

	// Now we should have enough bitmaps
	pt->size += Bitmap_setbit(&(pt->bitmaps[space_id]), page);
	ut_a(pt->size <= MAX_PAGETRACKER_SIZE_BYTES);


	// We may want to remove these sanity checks after enough testing
	ut_a(Bitmap_getbit(&(pt->bitmaps[space_id]), page) > 0);
	ut_a(PageTracker_get_page_status(pt, space_id, page) > 0);
}

/*
  This function dumps the PageTracker info i.e pages changed to a file. 
  Here is the format in which PageTracker data structure is dumped:
  StartLSN (8 bytes)
  EndLSN (8 bytes)
  #Bitmaps (8 bytes)
  StartLSN (8 bytes) duplicated to ensure integrity
  EndLSN (8 bytes) duplicated to ensure integrity
  #Bitmaps (8 bytes) duplicated to ensure integrity
  Offset of bitmap for tablespace 0 (8 bytes)
  Offset of bitmap for tablespace 1 (8 bytes) 
  .....
  Offset of bitmap for LAST tablespace (8 bytes)
  Offset of END of bitmap for LAST tablespace (8 bytes)
  Contents of bitmap for tablespace 0 (size varies from 0 to any multiple of 8)
  Checksum for above bitmap (8 bytes)
  Contents of bitmap for tablespace 1 (size varies from 0 to any multiple of 8)
  Checksum for above bitmap (8 bytes)
  ....
  Contents of bitmap for LAST tablespace
  Checksum for above bitmap (8 bytes)
 */
int PageTracker_dump(PageTracker* pt, os_file_t dumpfile)
{
	ib_uint64_t noffsets = PAGETRACKER_1STBITMAP_OFFSET + pt->nbitmaps + 1;

	// Allocate enough memory for checksums as well
	ib_uint64_t allocsize = pt->size/sizeof(ib_uint64_t) + pt->nbitmaps;
	ib_uint64_t allocsize_bytes = allocsize * sizeof(ib_uint64_t);
	ib_uint64_t *pserialize = (ib_uint64_t *)calloc ((size_t)allocsize,
							 sizeof(ib_uint64_t));
	byte*	    pwriteoffset = (byte *)pserialize;
	byte*	    pstart = (byte *)pserialize;	

	// write the offset table into pserialize;

	ib_uint64_t *poffsets = pserialize;
	ib_uint64_t i;
	ib_uint64_t checksum = 0;

	ibool success = TRUE;

	ut_a(allocsize_bytes >= noffsets * sizeof(ib_uint64_t));

	poffsets[PAGETRACKER_STARTLSN_OFFSET] = pt->startLSN;
	poffsets[PAGETRACKER_ENDLSN_OFFSET] = pt->endLSN;
	poffsets[PAGETRACKER_NBITMAPS_OFFSET] = pt->nbitmaps;

	// write these again
	poffsets[PAGETRACKER_STARTLSN_OFFSET2] = pt->startLSN;
	poffsets[PAGETRACKER_ENDLSN_OFFSET2] = pt->endLSN;
	poffsets[PAGETRACKER_NBITMAPS_OFFSET2] = pt->nbitmaps;
	

	// offset where 1st bitmap starts - same as size of poffsets in bytes
	poffsets[PAGETRACKER_1STBITMAP_OFFSET] = noffsets * sizeof(ib_uint64_t);

	// Note that this loop goes till pt->nbitmaps + 1. This is because 
	// when reading the dump, we want to get length of last bitmap by doing
	// offset[n] - offset[n-1].
	for (i = 1; i < pt->nbitmaps + 1; i++) {
		// bitmap offset = prev bitmap offset + size of prev bitmap
		// account for checksum as well by adding 1
		ib_uint64_t bitmaplen = sizeof(ib_uint64_t) *
			(1 + pt->bitmaps[i-1].length);
		poffsets[PAGETRACKER_1STBITMAP_OFFSET + i] = bitmaplen +
			poffsets[PAGETRACKER_1STBITMAP_OFFSET + i - 1];
	}
		
	// write the individual bitmps 
	pwriteoffset = pstart + (noffsets * sizeof(ib_uint64_t));
	
	for (i = 0; i < pt->nbitmaps; i++) {
		Bitmap *bm = &pt->bitmaps[i];

		// NOTE THAT bm->bitmap could be NULL!

		ut_a(allocsize_bytes >= ((pwriteoffset - pstart) + 
				  (bm->length * sizeof(ib_uint64_t))));

		memcpy(pwriteoffset, 
		       (void *)bm->bitmap, 
		       (ulint)(bm->length * sizeof(ib_uint64_t)));
		pwriteoffset += (bm->length * sizeof(ib_uint64_t));

		// write checksum
		checksum = my_fast_crc32((uchar*)(bm->bitmap), 
					 bm->length * sizeof(ib_uint64_t));

		ut_a(allocsize_bytes > (pwriteoffset - pstart) + sizeof(ib_uint64_t));
		memcpy(pwriteoffset, &checksum, sizeof(ib_uint64_t));
		pwriteoffset += sizeof(ib_uint64_t);

	}

	if (!os_file_write(path_dumpdata, dumpfile,
			   (void *)pstart, 
			   (ulint)0,
			   (ulint)0,
			   (ulint)(pwriteoffset - pstart))) {
		success = FALSE;
		os_file_get_last_error(TRUE);
	}

	free(pserialize);
	pserialize = NULL;

	return (success ? PAGETRACKER_DUMP_SUCCESS : PAGETRACKER_DUMP_FAIL);	
}


// If targetdir has pagetracker_dump.cmd file, 
// dump bitmaps to pagetracker_dump.data file and 
// remove pagetracker_dump.cmd file. Refer the comment
// header of PageTracker_dump for details on dump format.
// Returns
// 	PAGETRACKER_NO_DUMP		- no dump was attempted
//	PAGETRACKER_DUMP_SUCCESS	- dump succeeded
//	PAGETRACKER_DUMP_FAIL		- dump failed
int PageTracker_check_and_dump(PageTracker* pt)
{
	ibool 		success = FALSE;
	ibool 		exists = FALSE;
	os_file_t	data_file = -1;
	int		dump_status = PAGETRACKER_DUMP_FAIL;
	os_file_type_t	filetype;

	success = os_file_status(path_dumpcmd, &exists, &filetype);
	if (!success || !exists || (filetype != OS_FILE_TYPE_FILE)) {
		return PAGETRACKER_NO_DUMP;
	}

	// we need to dump
	// create dump file. truncate if it exists. 
	data_file = os_file_create(path_dumpdata, OS_FILE_OVERWRITE, 
				   OS_FILE_NORMAL, OS_LOG_FILE, &success);
	if (!success || (data_file < 0)) {
		os_file_get_last_error(TRUE);
		return PAGETRACKER_DUMP_FAIL;
	}

	dump_status = PageTracker_dump(pt, data_file);

	if (dump_status == PAGETRACKER_DUMP_SUCCESS) {
		if (!os_file_flush(data_file)) {
			os_file_get_last_error(TRUE);
			dump_status = PAGETRACKER_DUMP_FAIL;
		}		
	}
	
	if (!os_file_close(data_file)) {
		os_file_get_last_error(TRUE);
		dump_status = PAGETRACKER_DUMP_FAIL;
	}

	if (dump_status == PAGETRACKER_DUMP_FAIL) {
		return PAGETRACKER_DUMP_FAIL;
	}

	// just for sanity checking. we may want to remove this. 
	if (!PageTracker_validate_dump(pt, path_dumpdata)) {
		return PAGETRACKER_DUMP_FAIL;
	}

	if (!os_file_delete(path_dumpcmd)) {
		os_file_get_last_error(TRUE);
		return PAGETRACKER_DUMP_FAIL;
	}

	return PAGETRACKER_DUMP_SUCCESS;
	
}

// If targetdir has pagetracker_shutdown.cmd file, 
// remove the file and shutdown.
void PageTracker_check_and_exit(PageTracker* pt)
{
	ibool 		success = FALSE;
	ibool 		exists = FALSE;
	os_file_type_t	filetype;

	success = os_file_status(path_exitcmd, &exists, &filetype);
	if (!success || !exists || (filetype != OS_FILE_TYPE_FILE)) {
		return;
	}

	if (!os_file_delete(path_exitcmd)) {
		os_file_get_last_error(TRUE);
		fprintf(stderr, "Pagetracker error while deleting file %s\n",
			path_exitcmd);
		success = FALSE;
	}

	if (!os_file_delete(path_pidfile)) {
		os_file_get_last_error(TRUE);
		fprintf(stderr, "Pagetracker error while deleting file %s\n",
			path_pidfile);
		success = FALSE;
	}

	exit((success ? 0 : -1));
}

// Creates PAGETRACKER_PID_FILE with process pid in it.
// This also signifies that log copying has begun.
void PageTracker_create_pidfile(PageTracker* pt) 
{
	os_file_t	pidfile = -1;
	char		pidstring[100];
	ibool		success = FALSE;

	sprintf(pidstring, "%lu", (ulint)getpid());

	pidfile = os_file_create(path_pidfile, OS_FILE_CREATE, 
				   OS_FILE_NORMAL, OS_LOG_FILE, &success);
	if (!success || (pidfile < 0)) {
		os_file_get_last_error(TRUE);
		fprintf(stderr, 
			"Pagetracker exiting: could not create file %s!!!\n",
			path_pidfile);
		exit(-1);
	}
	
	if (!os_file_write(path_pidfile, pidfile,
			   (void *)pidstring, (ulint)0, (ulint)0,
			   strlen(pidstring))) {
		os_file_get_last_error(TRUE);
		fprintf(stderr, 
			"Pagetracker exiting: could not write to file %s!!!\n",
			path_pidfile);
		exit(-1);
	}

	if (!os_file_flush(pidfile)) {
		os_file_get_last_error(TRUE);
		fprintf(stderr, 
			"Pagetracker exiting: could not flush file %s!!!\n",
			path_pidfile);
		exit(-1);
	}		

	if (!os_file_close(pidfile)) {
		os_file_get_last_error(TRUE);
		fprintf(stderr, 
			"Pagetracker exiting: could not close file %s!!!\n",
			path_pidfile);
		exit(-1);
	}
	
}

// checks that dump file contents match contents of passed in PageTracker
ibool PageTracker_validate_dump(PageTracker *pt, char* dump_file)
{
	os_file_t	dump_fd = -1;
	ib_uint64_t	first7[7];
	ibool		success = TRUE;
	ib_uint64_t 	noffsets = 0;
	ib_uint64_t 	*poffsets = NULL;
	ib_uint64_t	i;

	dump_fd = os_file_create(dump_file, OS_FILE_OPEN, 
				 OS_FILE_NORMAL, OS_LOG_FILE, &success);
	if (dump_fd < 0) {
		os_file_get_last_error(TRUE);
		return FALSE;
	}

	success = os_file_read(dump_fd, (void *)first7, (ulint)0, (ulint)0,
			       (ulint)(7 * sizeof(ib_uint64_t)));
	if (!success) {
		os_file_get_last_error(TRUE);
		return FALSE;
	}

	if (first7[PAGETRACKER_STARTLSN_OFFSET] != pt->startLSN) {
		fprintf(stderr, 
			"Pagetracker: StartLSN mismatch %llu %llu\n",
			first7[PAGETRACKER_STARTLSN_OFFSET], pt->startLSN);
		success = FALSE;
	}
	
	if (first7[PAGETRACKER_ENDLSN_OFFSET] != pt->endLSN) {
		fprintf(stderr, 
			"Pagetracker: EndLSN mismatch %llu %llu\n",
			first7[PAGETRACKER_ENDLSN_OFFSET], pt->endLSN);
		success = FALSE;
	}

	if (first7[PAGETRACKER_NBITMAPS_OFFSET] != pt->nbitmaps) {
		fprintf(stderr, 
			"Pagetracker: Nbitmaps mismatch %llu %llu\n",
			first7[PAGETRACKER_NBITMAPS_OFFSET], pt->nbitmaps);
		success = FALSE;
	}

	noffsets = PAGETRACKER_1STBITMAP_OFFSET + pt->nbitmaps + 1;

	if (first7[PAGETRACKER_1STBITMAP_OFFSET] != 
	    noffsets * sizeof(ib_uint64_t)) {
		fprintf(stderr, 
			"Pagetracker: Noffsets mismatch %llu %llu\n",
			first7[PAGETRACKER_1STBITMAP_OFFSET], 
			noffsets * sizeof(ib_uint64_t));
		success = FALSE;
	}

	if (!success) {
		goto cleanup_and_return;
	}

	poffsets = (ib_uint64_t *)malloc (noffsets * sizeof(ib_uint64_t));

	success = os_file_read(dump_fd, (void *)poffsets, (ulint)0, (ulint)0,
			       (ulint)(noffsets * sizeof(ib_uint64_t)));
	if (!success) {
		os_file_get_last_error(TRUE);
		goto cleanup_and_return;
	}

	for (i = 1; i < pt->nbitmaps + 1; i++) {
		// bitmap offset = prev bitmap offset + size of prev bitmap
		ib_uint64_t  offset_file, offset_memory;
		offset_file = poffsets[PAGETRACKER_1STBITMAP_OFFSET + i];
		offset_memory = poffsets[PAGETRACKER_1STBITMAP_OFFSET + i - 1]
			+ (1 + pt->bitmaps[i-1].length) * sizeof(ib_uint64_t);
		if (offset_file != offset_memory) {
			fprintf(stderr, 
				"Pagetracker: Offset mismatch i=%llu, %llu, %llu\n",
				i, offset_file, offset_memory);
			success = FALSE;
			goto cleanup_and_return;
		}
	}
		
	// validate the individual bitmaps 
	for (i = 0; i < pt->nbitmaps; i++) {
		Bitmap *pbm_memory = &pt->bitmaps[i];
		Bitmap bm_file;
		ib_uint64_t nbytes;

		Bitmap_init(&bm_file);
		nbytes = (poffsets[PAGETRACKER_1STBITMAP_OFFSET + i + 1] -
			  poffsets[PAGETRACKER_1STBITMAP_OFFSET + i]);

		// deduct checksum length
		ut_a(nbytes > 0);
		nbytes -= sizeof(ib_uint64_t);

		bm_file.length = nbytes / sizeof(ib_uint64_t);

		if (bm_file.length != pbm_memory->length) {
			fprintf(stderr, 
				"Pagetracker: Length mismatch i=%llu, %llu, %llu\n",
				i, bm_file.length, pbm_memory->length);
			success = FALSE;
			goto cleanup_and_return;
		}

		if (!bm_file.length) {
			continue;
		}

		bm_file.bitmap = (ib_uint64_t *)malloc(nbytes);

		success = os_file_read(dump_fd, (void *)bm_file.bitmap,
				       (ulint)poffsets[PAGETRACKER_1STBITMAP_OFFSET + i], 
				       (ulint)0,
				       (ulint)nbytes);
		if (!success) {
			os_file_get_last_error(TRUE);
			fprintf(stderr, 
				"Pagetracker: Could not read bitmap %llu\n", i);
			success = FALSE;
		} else if (memcmp(bm_file.bitmap, pbm_memory->bitmap, nbytes)) {
			fprintf(stderr, 
				"Pagetracker: Could not match bitmap %llu\n", i);
			success = FALSE;
		}

		free(bm_file.bitmap);
		bm_file.bitmap = NULL;

		if (!success) {
			goto cleanup_and_return;
		}
	}

cleanup_and_return:
	if (poffsets) {
		free(poffsets);
		poffsets = NULL;
	}
	
	if (dump_fd > 0) {
		if (!os_file_close(dump_fd)) {
			os_file_get_last_error(TRUE);
		}
	}
	
	return success;				
}


// Gets the bitmap of tracked pages for one tablespace from the pagetracker 
// dump file. Returns TRUE on success and FALSE on failure.
ibool PageTracker_get_bitmap(char* dump_file, ulint space_id, Bitmap* pBitmap,
			     ib_uint64_t* startlsn, ib_uint64_t* endlsn)
{
	os_file_t	dump_fd = -1;
	ib_uint64_t	first7[7];
	ibool		success = FALSE;
	ib_uint64_t	nspaces = 0;
	ib_uint64_t	nbytes = 0;

	ib_uint64_t	bitmap_offsets[2];	// offsets of bitmap we want and 
						// the next one
	
	ib_uint64_t     bitmap_offset_offset = 0;

	if (pBitmap->bitmap) {
		ut_a(pBitmap->length > 0);
		free(pBitmap->bitmap);
		pBitmap->bitmap = NULL;
		pBitmap->length = 0;
	} else {
		ut_a(pBitmap->length == 0);
	}

	dump_fd = os_file_create(dump_file, OS_FILE_OPEN, 
				 OS_FILE_NORMAL, OS_LOG_FILE, &success);
	if (dump_fd < 0) {
		os_file_get_last_error(TRUE);
		return FALSE;
	}

	success = os_file_read(dump_fd, (void *)first7, (ulint)0, (ulint)0,
			       (ulint)(7 * sizeof(ib_uint64_t)));

	if (!success) {
		os_file_get_last_error(TRUE);
		success = FALSE;
		goto cleanup_and_return;
	}

	nspaces = first7[PAGETRACKER_NBITMAPS_OFFSET];	
	*startlsn = first7[PAGETRACKER_STARTLSN_OFFSET];
	*endlsn = first7[PAGETRACKER_ENDLSN_OFFSET];

	// validate STARTLSN, ENDLSN and NBITMAPS
	if (nspaces != first7[PAGETRACKER_NBITMAPS_OFFSET2]) {
		fprintf(stderr, 
			"Pagetracker error: nbitmaps mismatch %llu, %llu\n",
			nspaces, first7[PAGETRACKER_NBITMAPS_OFFSET2]);
		success = FALSE;
		goto cleanup_and_return;
	}

	if (*startlsn != first7[PAGETRACKER_STARTLSN_OFFSET2]) {
		fprintf(stderr, 
			"Pagetracker error: startlsn mismatch %llu, %llu\n",
			*startlsn, first7[PAGETRACKER_STARTLSN_OFFSET2]);
		success = FALSE;
		goto cleanup_and_return;
	}

	if (*endlsn != first7[PAGETRACKER_ENDLSN_OFFSET2]) {
		fprintf(stderr, 
			"Pagetracker error: endlsn mismatch %llu, %llu\n",
			*endlsn, first7[PAGETRACKER_ENDLSN_OFFSET2]);
		success = FALSE;
		goto cleanup_and_return;		
	}

	if (space_id >= nspaces) {
		fprintf(stderr, 
			"Pagetracker info: Spaceid %lu larger than max %llu\n",
			space_id, nspaces);
		success = TRUE;
		goto cleanup_and_return;
	}

	// compute the offset of bitmap offset
	bitmap_offset_offset = (PAGETRACKER_1STBITMAP_OFFSET + space_id) *
		sizeof(ib_uint64_t);

	// read the offset of bitmap we want as well as the next one
	success = os_file_read(dump_fd, (void *)bitmap_offsets, 
			       (ulint)bitmap_offset_offset,
			       (ulint)0, (ulint)(2 * sizeof(ib_uint64_t)));
	if (!success) {
		os_file_get_last_error(TRUE);
		success = FALSE;
		goto cleanup_and_return;
	}

	if (bitmap_offsets[1] < bitmap_offsets[0]) {
		fprintf(stderr, 
			"Pagetracker: Bad adjacent bitmap offsets %llu, %llu\n",
			bitmap_offsets[0], bitmap_offsets[1]);
		success = FALSE;
		goto cleanup_and_return;
	} else {
		ib_uint64_t checksum_computed = 0;
		ib_uint64_t checksum_file = 0;

		ut_a(bitmap_offsets[1] > bitmap_offsets[0]);
		nbytes = bitmap_offsets[1] - bitmap_offsets[0];
		pBitmap->bitmap = (ib_uint64_t *)malloc(nbytes);
		pBitmap->length = nbytes/sizeof(ib_uint64_t);
		success = os_file_read(dump_fd, (void *)pBitmap->bitmap, 
				       (ulint)bitmap_offsets[0], 
				       (ulint)0, (ulint)nbytes);

		if (!success) {
			os_file_get_last_error(TRUE);
			success = FALSE;
			goto cleanup_and_return;
		}

		// adjust bitmap length by deducting checksum len
		pBitmap->length -= 1;
		checksum_file = pBitmap->bitmap[pBitmap->length];

		// validate checksum
		checksum_computed = my_fast_crc32((uchar*)(pBitmap->bitmap),
						  pBitmap->length * 
						  sizeof(ib_uint64_t));

		if (checksum_computed != checksum_file) {
			fprintf(stderr,
				"ERROR Pagetracker checksums mismatch %llu, %llu\n",
				checksum_computed, checksum_file);
			success = FALSE;
			goto cleanup_and_return;
		} else {
			fprintf(stderr,
				"INFO Pagetracker checksums match %llu\n",
				checksum_computed);
		}

		// For bitmap of length 0, we expect pBitmap->bitmap to be NULL
		if (!pBitmap->length) {
			free(pBitmap->bitmap);
			pBitmap->bitmap = NULL;
		}

		// fprintf(stderr, 
		//	"Pagetracker read bitmap of %llu bytes for space %lu\n",
		//	nbytes, space_id);
		success = TRUE;
	} 

cleanup_and_return:	
	if (dump_fd > 0) {		
		if (!os_file_close(dump_fd))
		{
			os_file_get_last_error(TRUE);
			success = FALSE;
		}
	}

	if (!success && pBitmap->bitmap) {
		free(pBitmap->bitmap);
		pBitmap->bitmap = NULL;
		pBitmap->length = 0;
	}

	return success;
}
