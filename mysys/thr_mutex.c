/* Copyright (C) 2000-2003 MySQL AB

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

/* This makes a wrapper for mutex handling to make it easier to debug mutex */

#include <my_global.h>
#include "my_atomic.h"
#if defined(TARGET_OS_LINUX) && !defined (__USE_UNIX98)
#define __USE_UNIX98			/* To get rw locks under Linux */
#endif
#if defined(THREAD) && defined(SAFE_MUTEX)
#undef SAFE_MUTEX			/* Avoid safe_mutex redefinitions */
#include "mysys_priv.h"
#include "my_static.h"
#include <m_string.h>

#ifndef DO_NOT_REMOVE_THREAD_WRAPPERS
/* Remove wrappers */
#undef pthread_mutex_t
#undef pthread_mutex_init
#undef pthread_mutex_lock
#undef pthread_mutex_timedlock
#undef pthread_mutex_unlock
#undef pthread_mutex_destroy
#undef pthread_cond_wait
#undef pthread_cond_timedwait
#ifdef HAVE_NONPOSIX_PTHREAD_MUTEX_INIT
#define pthread_mutex_init(a,b) my_pthread_mutex_init((a),(b))
#endif
#endif /* DO_NOT_REMOVE_THREAD_WRAPPERS */

static pthread_mutex_t THR_LOCK_mutex;
static ulong safe_mutex_count= 0;		/* Number of mutexes created */
#ifdef SAFE_MUTEX_DETECT_DESTROY
static struct st_safe_mutex_info_t *safe_mutex_root= NULL;
#endif

void safe_mutex_global_init(void)
{
  pthread_mutex_init(&THR_LOCK_mutex,MY_MUTEX_INIT_FAST);
}


int safe_mutex_init(safe_mutex_t *mp,
		    const pthread_mutexattr_t *attr __attribute__((unused)),
		    const char *file,
		    uint line)
{
  bzero((char*) mp,sizeof(*mp));
  pthread_mutex_init(&mp->global,MY_MUTEX_INIT_ERRCHK);
  pthread_mutex_init(&mp->mutex,attr);
  /* Mark that mutex is initialized */
  mp->file= file;
  mp->line= line;

#ifdef SAFE_MUTEX_DETECT_DESTROY
  /*
    Monitor the freeing of mutexes.  This code depends on single thread init
    and destroy
  */
  if ((mp->info= (safe_mutex_info_t *) malloc(sizeof(safe_mutex_info_t))))
  {
    struct st_safe_mutex_info_t *info =mp->info;

    info->init_file= file;
    info->init_line= line;
    info->prev= NULL;
    info->next= NULL;

    pthread_mutex_lock(&THR_LOCK_mutex);
    if ((info->next= safe_mutex_root))
      safe_mutex_root->prev= info;
    safe_mutex_root= info;
    safe_mutex_count++;
    pthread_mutex_unlock(&THR_LOCK_mutex);
  }
#else
  thread_safe_increment(safe_mutex_count, &THR_LOCK_mutex);
#endif /* SAFE_MUTEX_DETECT_DESTROY */
  return 0;
}


int safe_mutex_lock(safe_mutex_t *mp, my_bool try_lock, const char *file, uint line)
{
  int error;
  if (!mp->file)
  {
    fprintf(stderr,
	    "safe_mutex: Trying to lock unitialized mutex at %s, line %d\n",
	    file, line);
    fflush(stderr);
    abort();
  }

  pthread_mutex_lock(&mp->global);
  if (mp->count > 0)
  {
    if (try_lock)
    {
      pthread_mutex_unlock(&mp->global);
      return EBUSY;
    }
    else if (pthread_equal(pthread_self(),mp->thread))
    {
      fprintf(stderr,
              "safe_mutex: Trying to lock mutex at %s, line %d, when the"
              " mutex was already locked at %s, line %d in thread %s\n",
              file,line,mp->file, mp->line, my_thread_name());
      fflush(stderr);
      abort();
    }
  }
  pthread_mutex_unlock(&mp->global);

  /*
    If we are imitating trylock(), we need to take special
    precautions.

    - We cannot use pthread_mutex_lock() only since another thread can
      overtake this thread and take the lock before this thread
      causing pthread_mutex_trylock() to hang. In this case, we should
      just return EBUSY. Hence, we use pthread_mutex_trylock() to be
      able to return immediately.

    - We cannot just use trylock() and continue execution below, since
      this would generate an error and abort execution if the thread
      was overtaken and trylock() returned EBUSY . In this case, we
      instead just return EBUSY, since this is the expected behaviour
      of trylock().
   */
  if (try_lock)
  {
    error= pthread_mutex_trylock(&mp->mutex);
    if (error == EBUSY)
      return error;
  }
  else
    error= pthread_mutex_lock(&mp->mutex);

  if (error || (error=pthread_mutex_lock(&mp->global)))
  {
    fprintf(stderr,"Got error %d when trying to lock mutex at %s, line %d\n",
	    error, file, line);
    fflush(stderr);
    abort();
  }
  mp->thread= pthread_self();
  if (mp->count++)
  {
    fprintf(stderr,"safe_mutex: Error in thread libray: Got mutex at %s, \
line %d more than 1 time\n", file,line);
    fflush(stderr);
    abort();
  }
  mp->file= file;
  mp->line=line;
  pthread_mutex_unlock(&mp->global);
  return error;
}


int safe_mutex_timedlock(safe_mutex_t *mp, const struct timespec *abs_timeout,
                         const char *file, uint line)
{
  int error;
  if (!mp->file)
  {
    fprintf(stderr,
	    "safe_mutex: Trying to lock unitialized mutex at %s, line %d\n",
	    file, line);
    fflush(stderr);
    abort();
  }

  pthread_mutex_lock(&mp->global);
  if (mp->count > 0)
  {
    if (pthread_equal(pthread_self(),mp->thread))
    {
      fprintf(stderr,
              "safe_mutex: Trying to lock mutex at %s, line %d, when the"
              " mutex was already locked at %s, line %d in thread %s\n",
              file,line,mp->file, mp->line, my_thread_name());
      fflush(stderr);
      abort();
    }
  }
  pthread_mutex_unlock(&mp->global);

  error= pthread_mutex_timedlock(&mp->mutex, abs_timeout);

  if (error || (error=pthread_mutex_lock(&mp->global)))
  {
    fprintf(stderr,"Got error %d when trying to lock mutex at %s, line %d\n",
	    error, file, line);
    fflush(stderr);
    abort();
  }
  mp->thread= pthread_self();
  if (mp->count++)
  {
    fprintf(stderr,"safe_mutex: Error in thread libray: Got mutex at %s, \
line %d more than 1 time\n", file,line);
    fflush(stderr);
    abort();
  }
  mp->file= file;
  mp->line=line;
  pthread_mutex_unlock(&mp->global);
  return error;
}


int safe_mutex_unlock(safe_mutex_t *mp,const char *file, uint line)
{
  int error;
  pthread_mutex_lock(&mp->global);
  if (mp->count == 0)
  {
    fprintf(stderr,"safe_mutex: Trying to unlock mutex that wasn't locked at %s, line %d\n            Last used at %s, line: %d\n",
	    file,line,mp->file ? mp->file : "",mp->line);
    fflush(stderr);
    abort();
  }
  if (!pthread_equal(pthread_self(),mp->thread))
  {
    fprintf(stderr,"safe_mutex: Trying to unlock mutex at %s, line %d  that was locked by another thread at: %s, line: %d\n",
	    file,line,mp->file,mp->line);
    fflush(stderr);
    abort();
  }
  mp->thread= 0;
  mp->count--;
#ifdef __WIN__
  pthread_mutex_unlock(&mp->mutex);
  error=0;
#else
  error=pthread_mutex_unlock(&mp->mutex);
  if (error)
  {
    fprintf(stderr,"safe_mutex: Got error: %d (%d) when trying to unlock mutex at %s, line %d\n", error, errno, file, line);
    fflush(stderr);
    abort();
  }
#endif /* __WIN__ */
  pthread_mutex_unlock(&mp->global);
  return error;
}


int safe_cond_wait(pthread_cond_t *cond, safe_mutex_t *mp, const char *file,
		   uint line)
{
  int error;
  pthread_mutex_lock(&mp->global);
  if (mp->count == 0)
  {
    fprintf(stderr,"safe_mutex: Trying to cond_wait on a unlocked mutex at %s, line %d\n",file,line);
    fflush(stderr);
    abort();
  }
  if (!pthread_equal(pthread_self(),mp->thread))
  {
    fprintf(stderr,"safe_mutex: Trying to cond_wait on a mutex at %s, line %d  that was locked by another thread at: %s, line: %d\n",
	    file,line,mp->file,mp->line);
    fflush(stderr);
    abort();
  }

  if (mp->count-- != 1)
  {
    fprintf(stderr,"safe_mutex:  Count was %d on locked mutex at %s, line %d\n",
	    mp->count+1, file, line);
    fflush(stderr);
    abort();
  }
  pthread_mutex_unlock(&mp->global);
  error=pthread_cond_wait(cond,&mp->mutex);
  pthread_mutex_lock(&mp->global);
  if (error)
  {
    fprintf(stderr,"safe_mutex: Got error: %d (%d) when doing a safe_mutex_wait at %s, line %d\n", error, errno, file, line);
    fflush(stderr);
    abort();
  }
  mp->thread=pthread_self();
  if (mp->count++)
  {
    fprintf(stderr,
	    "safe_mutex:  Count was %d in thread 0x%lx when locking mutex at %s, line %d\n",
	    mp->count-1, my_thread_dbug_id(), file, line);
    fflush(stderr);
    abort();
  }
  mp->file= file;
  mp->line=line;
  pthread_mutex_unlock(&mp->global);
  return error;
}


int safe_cond_timedwait(pthread_cond_t *cond, safe_mutex_t *mp,
			struct timespec *abstime,
			const char *file, uint line)
{
  int error;
  pthread_mutex_lock(&mp->global);
  if (mp->count != 1 || !pthread_equal(pthread_self(),mp->thread))
  {
    fprintf(stderr,"safe_mutex: Trying to cond_wait at %s, line %d on a not hold mutex\n",file,line);
    fflush(stderr);
    abort();
  }
  mp->count--;					/* Mutex will be released */
  pthread_mutex_unlock(&mp->global);
  error=pthread_cond_timedwait(cond,&mp->mutex,abstime);
#ifdef EXTRA_DEBUG
  if (error && (error != EINTR && error != ETIMEDOUT && error != ETIME))
  {
    fprintf(stderr,"safe_mutex: Got error: %d (%d) when doing a safe_mutex_timedwait at %s, line %d\n", error, errno, file, line);
  }
#endif
  pthread_mutex_lock(&mp->global);
  mp->thread=pthread_self();
  if (mp->count++)
  {
    fprintf(stderr,
	    "safe_mutex:  Count was %d in thread 0x%lx when locking mutex at %s, line %d (error: %d (%d))\n",
	    mp->count-1, my_thread_dbug_id(), file, line, error, error);
    fflush(stderr);
    abort();
  }
  mp->file= file;
  mp->line=line;
  pthread_mutex_unlock(&mp->global);
  return error;
}


int safe_mutex_destroy(safe_mutex_t *mp, const char *file, uint line)
{
  int error=0;
  if (!mp->file)
  {
    fprintf(stderr,
	    "safe_mutex: Trying to destroy unitialized mutex at %s, line %d\n",
	    file, line);
    fflush(stderr);
    abort();
  }
  if (mp->count != 0)
  {
    fprintf(stderr,"safe_mutex: Trying to destroy a mutex that was locked at %s, line %d at %s, line %d\n",
	    mp->file,mp->line, file, line);
    fflush(stderr);
    abort();
  }
#ifdef __WIN__ 
  pthread_mutex_destroy(&mp->global);
  pthread_mutex_destroy(&mp->mutex);
#else
  if (pthread_mutex_destroy(&mp->global))
    error=1;
  if (pthread_mutex_destroy(&mp->mutex))
    error=1;
#endif
  mp->file= 0;					/* Mark destroyed */

#ifdef SAFE_MUTEX_DETECT_DESTROY
  if (mp->info)
  {
    struct st_safe_mutex_info_t *info= mp->info;
    pthread_mutex_lock(&THR_LOCK_mutex);

    if (info->prev)
      info->prev->next = info->next;
    else
      safe_mutex_root = info->next;
    if (info->next)
      info->next->prev = info->prev;
    safe_mutex_count--;

    pthread_mutex_unlock(&THR_LOCK_mutex);
    free(info);
    mp->info= NULL;				/* Get crash if double free */
  }
#else
  thread_safe_sub(safe_mutex_count, 1, &THR_LOCK_mutex);
#endif /* SAFE_MUTEX_DETECT_DESTROY */
  return error;
}


/*
  Free global resources and check that all mutex has been destroyed

  SYNOPSIS
    safe_mutex_end()
    file		Print errors on this file

  NOTES
    We can't use DBUG_PRINT() here as we have in my_end() disabled
    DBUG handling before calling this function.

   In MySQL one may get one warning for a mutex created in my_thr_init.c
   This is ok, as this thread may not yet have been exited.
*/

void safe_mutex_end(FILE *file __attribute__((unused)))
{
  if (!safe_mutex_count)			/* safetly */
    pthread_mutex_destroy(&THR_LOCK_mutex);
#ifdef SAFE_MUTEX_DETECT_DESTROY
  if (!file)
    return;

  if (safe_mutex_count)
  {
    fprintf(file, "Warning: Not destroyed mutex: %lu\n", safe_mutex_count);
    (void) fflush(file);
  }
  {
    struct st_safe_mutex_info_t *ptr;
    for (ptr= safe_mutex_root ; ptr ; ptr= ptr->next)
    {
      fprintf(file, "\tMutex initiated at line %4u in '%s'\n",
	      ptr->init_line, ptr->init_file);
      (void) fflush(file);
    }
  }
#endif /* SAFE_MUTEX_DETECT_DESTROY */
}

#endif /* THREAD && SAFE_MUTEX */

#if defined(THREAD) && defined(MY_PTHREAD_FASTMUTEX) && !defined(SAFE_MUTEX)

#include "mysys_priv.h"
#include "my_static.h"
#include <m_string.h>

#include <m_ctype.h>
#include <hash.h>
#include <myisampack.h>
#include <mysys_err.h>
#include <my_sys.h>

#undef pthread_mutex_t
#undef pthread_mutex_init
#undef pthread_mutex_lock
#undef pthread_mutex_timedlock
#undef pthread_mutex_trylock
#undef pthread_mutex_unlock
#undef pthread_mutex_destroy
#undef pthread_cond_wait
#undef pthread_cond_timedwait

#define pause_cpu() asm volatile("PAUSE")

static ulong mutex_delay(ulong delayloops)
{
  ulong	i;
  volatile ulong j;

  j = 0;

  for (i = 0; i < delayloops; i++) {
    pause_cpu();
    j += i;
  }

  return(j);
}

#define MY_PTHREAD_FASTMUTEX_FAST_SPINS 25
#define MY_PTHREAD_FASTMUTEX_SLOW_SPINS 4
#define MAX_CONCURRENT_SPINNERS cpu_count
#define MAX_STATS 10000

static my_fastmutex_stats mutex_stats[MAX_STATS];

#if defined(MY_COUNT_MUTEX_CALLERS)
static my_fastmutex_stats mutex_caller_stats[MAX_STATS];
#endif

static int cpu_count= 0;

/* The total spin-wait time is ~6 microseonds on a circa-2008 x86_64
 * CPU with 4 (MY_PTHREAD_FASTMUTEX_SPINS) loops & fastmutex_max_spin_wait_loops
 * set to 100. That may change and the result is displayed in the db error log
 * and in SHOW STATUS as mysql_spin_wait_microseconds.
 */
static long fastmutex_max_spin_wait_loops= 100;

/* Delays for the maximum spin wait time. The caller can time this to determine
 * the max wait time. Ignore the return value.
 */
ulong my_fastmutex_delay()
{
  int x;
  ulong res= 0;
  uint maxdelay= fastmutex_max_spin_wait_loops;
  for (x=0; x < MY_PTHREAD_FASTMUTEX_SLOW_SPINS; ++x)
  {
    res += mutex_delay(maxdelay);
    /* Use the average delay */
    maxdelay += (fastmutex_max_spin_wait_loops / 2);
  }
  return res;
}

/* Returns pointer to mutex contention statistics array. Reads from
 * this are dirty (no locks). Returns in 'num_stats' the size of the
 * array. Entries with fms_name != NULL are valid.
 */
my_fastmutex_stats* my_fastmutex_get_stats(int *num_stats)
{
  *num_stats= MAX_STATS;
  return mutex_stats;
}

#if defined(MY_COUNT_MUTEX_CALLERS)
my_fastmutex_stats* my_fastmutex_get_caller_stats(int *num_stats)
{
  *num_stats= MAX_STATS;
  return mutex_caller_stats;
}
#endif

/* Initialized the mutex contention statistics array */
static void my_fastmutex_init(my_fastmutex_stats* stats)
{
  int i;

  for (i=0; i < MAX_STATS; ++i)
  {
    stats[i].fms_name= NULL;
    stats[i].fms_line= 0;
    stats[i].fms_users= 0;
    stats[i].fms_locks= 0;
    stats[i].fms_slow_spins= 0;
    stats[i].fms_fast_spins= 0;
    stats[i].fms_slow_spin_wins= 0;
    stats[i].fms_fast_spin_wins= 0;
    stats[i].fms_sleeps= 0;
  }
}

/* Returns counters aggregated over all entries in the statistics array.
 * 'num_mutexes' returns the number of valid entries. 
 */
void my_fastmutex_report_stats(unsigned long long* sleeps,
                               unsigned long long* fast_spins,
                               unsigned long long* slow_spins,
                               unsigned long long* fast_spin_wins,
                               unsigned long long* slow_spin_wins,
                               unsigned long long* locks,
                               int* num_mutexes)
{
  unsigned long long num_sleeps= 0;
  unsigned long long num_fast_spins= 0;
  unsigned long long num_slow_spins= 0;
  unsigned long long num_fast_spin_wins= 0;
  unsigned long long num_slow_spin_wins= 0;
  unsigned long long num_locks= 0;
  int i;
  my_fastmutex_stats *fms;

  for (i= 0, fms= mutex_stats; i < MAX_STATS; ++i, ++fms)
  {
    num_sleeps += fms->fms_sleeps;
    num_fast_spins += fms->fms_fast_spins;
    num_slow_spins += fms->fms_slow_spins;
    num_fast_spin_wins += fms->fms_fast_spin_wins;
    num_slow_spin_wins += fms->fms_slow_spin_wins;
    num_locks += fms->fms_locks;
    (*num_mutexes)++;
  }
  *sleeps= num_sleeps;
  *fast_spins= num_fast_spins;
  *slow_spins= num_slow_spins;
  *fast_spin_wins= num_fast_spin_wins;
  *slow_spin_wins= num_slow_spin_wins;
  *locks= num_locks;
}

/* All of the ut* functions and constants are copied from Innodb.
 * Thanks Heikki.
 */

/* Constants for random number generation.
 */
#define UT_HASH_RANDOM_MASK     1463735687UL
#define UT_HASH_RANDOM_MASK2    1653893711UL

/* Returns the hash from a pair of ulong values
 *   n1, n2: input for which the hash is computed
 */
static ulong
ut_fold_ulong_pair(ulong n1, ulong n2)
{
  return(((((n1 ^ n2 ^ UT_HASH_RANDOM_MASK2) << 8) + n1)
          ^ UT_HASH_RANDOM_MASK) + n2);
}

/* Returns the hash from a string
 *   str: null-terminated string for which the hash is computed
 */
static ulong
ut_fold_string(const char* str)
{
  ulong fold= 0;
  while (*str != '\0') {
    fold = ut_fold_ulong_pair(fold, (ulong)(*str));
    str++;
  }
  return(fold);
}

/**
  Park-Miller random number generator. A simple linear congruential
  generator that operates in multiplicative group of integers modulo n.

  x_{k+1} = (x_k g) mod n

  Popular pair of parameters: n = 2^32 − 5 = 4294967291 and g = 279470273.
  The period of the generator is about 2^31.
  Largest value that can be returned: 2147483646 (RAND_MAX)

  Reference:

  S. K. Park and K. W. Miller
  "Random number generators: good ones are hard to find"
  Commun. ACM, October 1988, Volume 31, No 10, pages 1192-1201.
*/

static double park_rng(uint *state)
{
  *state= (((my_ulonglong) *state) * 279470273U) % 4294967291U;
  return (*state / 2147483647.0);
}

#if defined(MY_COUNT_MUTEX_CALLERS)
static void
increment_sleep_for_caller(const char* caller, int line)
{
  ulong index= ut_fold_ulong_pair(ut_fold_string(caller), line) % MAX_STATS;
  my_fastmutex_stats* stats= &mutex_caller_stats[index];
  // Note that fms_locks, fms_{fast,slow}_spins and fms_users are not updated
  // because this is only called when a thread might sleep (to be fast). To run
  // faster this is not thread safe. The last updater of the array slot
  // gets to name it (fms_name).
  stats->fms_sleeps++;
  stats->fms_name= caller;
  stats->fms_line= line;
}
#endif

int my_pthread_fastmutex_init(my_pthread_fastmutex_t *mp,
                              const pthread_mutexattr_t *attr,
                              const char* caller,
                              const int line)
{
  DBUG_ASSERT(caller);
  if ((cpu_count > 1) && (attr == MY_MUTEX_INIT_FAST)) {
    mp->fast_spins= MY_PTHREAD_FASTMUTEX_FAST_SPINS;
    mp->slow_spins= MY_PTHREAD_FASTMUTEX_SLOW_SPINS;
  }
  else
    mp->fast_spins= mp->slow_spins= 0;

  mp->rng_state= 1;
  mp->stats_index=
      ut_fold_ulong_pair(ut_fold_string(caller), line) % MAX_STATS;
  mutex_stats[mp->stats_index].fms_name= caller;
  mutex_stats[mp->stats_index].fms_line= line;
  mutex_stats[mp->stats_index].fms_users++;
  return pthread_mutex_init(&mp->mutex, attr); 
}

int my_pthread_fastmutex_init_by_name(my_pthread_fastmutex_t *mp,
                                      const pthread_mutexattr_t *attr,
                                      const char* name)
{
  return my_pthread_fastmutex_init(mp, attr, name, 0);
}

static volatile int32 num_current_spinners = 0;

int my_pthread_fastmutex_lock(my_pthread_fastmutex_t *mp
#if defined(MY_COUNT_MUTEX_CALLERS)
                              , const char* caller, int line
#endif
                              )
{
  int   res;
  uint  i;
  uint  maxdelay= fastmutex_max_spin_wait_loops;
  my_fastmutex_stats *fms= &mutex_stats[mp->stats_index];

  /* We perform two types of spinning.  First, we "fast spin" which is
   * a very tight loop of "check lock, PAUSE cpu" repeatedly (by
   * default, 25 times).  If this tight loop does not result in the
   * lock being acquired, we change to "slow" spinning, where we pause
   * much longer between checking the lock.  If this fails to result
   * in the lock being acquired, we invoke the pthread wait function.
   *
   * Fast spins are inspired by LinuxThread's/NPTL's adaptive mutex
   * lock.  Slow spins are inspired by InnoDB's mutex routines.
   *
   * As a general note, pthread_mutex_trylock simply performs an
   * atomic memory operation; this means it is fast and doesn't
   * involve a syscall -- simply a memory synchronization.  This makes
   * the call to trylock cheap but not one we want to use constantly,
   * hence performing it frequently at first (for the case of a very
   * short held lock) but backing off and simply checking it much less
   * frequently while spinning the cpu.
   */
  fms->fms_locks++;
  for (i= 0; i < mp->fast_spins; i++)
  {
    res= pthread_mutex_trylock(&mp->mutex);
    assert(res == 0 || res == EBUSY);

    if (res != EBUSY) {
      fms->fms_fast_spins += i;
      fms->fms_fast_spin_wins += (i > 0);  // don't count zero contention!
      return res;
    }

    pause_cpu();
  }
  fms->fms_fast_spins += mp->fast_spins;

  if (num_current_spinners < MAX_CONCURRENT_SPINNERS) {
    my_atomic_add32(&num_current_spinners, 1);
    for (i= 0; i < mp->slow_spins; i++) {
      res= pthread_mutex_trylock(&mp->mutex);
      assert(res == 0 || res == EBUSY);

      if (res != EBUSY) {
        fms->fms_slow_spins += i;
        fms->fms_slow_spin_wins++;
        my_atomic_add32(&num_current_spinners, -1);
        return res;
      }

      mutex_delay(maxdelay);
      maxdelay += park_rng(&mp->rng_state) * fastmutex_max_spin_wait_loops + 1;
    }
    my_atomic_add32(&num_current_spinners, -1);
  }
  fms->fms_slow_spins += mp->slow_spins;

  fms->fms_sleeps++;

#if defined(MY_COUNT_MUTEX_CALLERS)
  increment_sleep_for_caller(caller, line);
#endif

  return pthread_mutex_lock(&mp->mutex);
}

int my_pthread_fastmutex_timedlock(my_pthread_fastmutex_t *mp,
                                   const struct timespec *abs_timeout
#if defined(MY_COUNT_MUTEX_CALLERS)
                                 , const char* caller, int line
#endif
                                  )
{
  int   res;
  uint  i;
  uint  maxdelay= fastmutex_max_spin_wait_loops;
  my_fastmutex_stats *fms= &mutex_stats[mp->stats_index];

  fms->fms_locks++;
  for (i= 0; i < mp->fast_spins; i++)
  {
    res= pthread_mutex_trylock(&mp->mutex);
    assert(res == 0 || res == EBUSY);

    if (res != EBUSY) {
      fms->fms_fast_spins += i;
      fms->fms_fast_spin_wins += (i > 0);  // don't count zero contention!
      return res;
    }

    pause_cpu();
  }
  fms->fms_fast_spins += mp->fast_spins;

  if (num_current_spinners < MAX_CONCURRENT_SPINNERS) {
    my_atomic_add32(&num_current_spinners, 1);
    for (i= 0; i < mp->slow_spins; i++) {
      res= pthread_mutex_trylock(&mp->mutex);
      assert(res == 0 || res == EBUSY);

      if (res != EBUSY) {
        fms->fms_slow_spins += i;
        fms->fms_slow_spin_wins++;
        my_atomic_add32(&num_current_spinners, -1);
        return res;
      }

      mutex_delay(maxdelay);
      maxdelay += park_rng(&mp->rng_state) * fastmutex_max_spin_wait_loops + 1;
    }
    my_atomic_add32(&num_current_spinners, -1);
  }
  fms->fms_slow_spins += mp->slow_spins;

  fms->fms_sleeps++;

#if defined(MY_COUNT_MUTEX_CALLERS)
  increment_sleep_for_caller(caller, line);
#endif

  return pthread_mutex_timedlock(&mp->mutex, abs_timeout);
}

void fastmutex_global_init(void)
{
#ifdef _SC_NPROCESSORS_CONF
  cpu_count= sysconf(_SC_NPROCESSORS_CONF);
#endif
  my_fastmutex_init(mutex_stats);
#if defined(MY_COUNT_MUTEX_CALLERS)
  my_fastmutex_init(mutex_caller_stats);
#endif
}

/* There is a my.cnf variable to set the number of loops.
 */
void my_fastmutex_set_max_spin_wait_loops(long spin_wait_loops)
{
  fastmutex_max_spin_wait_loops= spin_wait_loops;
}
  
#endif /* defined(THREAD) && defined(MY_PTHREAD_FASTMUTEX) && !defined(SAFE_MUTEX) */ 

#if defined(MY_FASTRWLOCK)

int my_fastrwlock_init(my_fastrwlock_t *rw,
                       const pthread_rwlockattr_t *attr,
                       const char* caller, const int line)
{
  /* TODO -- check attr */
  DBUG_ASSERT(caller);
  if (cpu_count > 1)
  {
    rw->fast_spins= MY_PTHREAD_FASTMUTEX_FAST_SPINS;
    rw->slow_spins= MY_PTHREAD_FASTMUTEX_SLOW_SPINS;
  }
  else
    rw->fast_spins= rw->slow_spins= 0;

  rw->stats_index=
      ut_fold_ulong_pair(ut_fold_string(caller), line) % MAX_STATS;
  mutex_stats[rw->stats_index].fms_name= caller;
  mutex_stats[rw->stats_index].fms_line= line;
  mutex_stats[rw->stats_index].fms_users++;
  return pthread_rwlock_init(&rw->frw_lock, attr); 
}

int my_fastrwlock_init_by_name(my_fastrwlock_t *rw,
                               const pthread_rwlockattr_t *attr,
                               const char* caller)
{
  return my_fastrwlock_init(rw, attr, caller, 0);
}

int my_fastrwlock_rdlock(my_fastrwlock_t *rw
#if defined(MY_COUNT_MUTEX_CALLERS)
                              , const char* caller, int line
#endif
                              )
{
  int   res;
  uint  i;
  uint  maxdelay= fastmutex_max_spin_wait_loops;
  my_fastmutex_stats *fms= &mutex_stats[rw->stats_index];

  fms->fms_locks++;
  for (i= 0; i < rw->fast_spins; i++)
  {
    res= pthread_rwlock_tryrdlock(&rw->frw_lock);
    assert(res == 0 || res == EBUSY);

    if (res != EBUSY) {
      fms->fms_fast_spins += i;
      fms->fms_fast_spin_wins += (i > 0);  // don't count zero contention!
      return res;
    }

    pause_cpu();
  }
  fms->fms_fast_spins += rw->fast_spins;

  if (num_current_spinners < MAX_CONCURRENT_SPINNERS) {
    my_atomic_add32(&num_current_spinners, 1);
    for (i= 0; i < rw->slow_spins; i++) {
      res= pthread_rwlock_tryrdlock(&rw->frw_lock);
      assert(res == 0 || res == EBUSY);

      if (res != EBUSY) {
        fms->fms_slow_spins += i;
        fms->fms_slow_spin_wins++;
        my_atomic_add32(&num_current_spinners, -1);
        return res;
      }

      mutex_delay(maxdelay);
      maxdelay += park_rng(&rw->rng_state) * fastmutex_max_spin_wait_loops + 1;
    }
    my_atomic_add32(&num_current_spinners, -1);

  }

  fms->fms_slow_spins += rw->slow_spins;
  fms->fms_sleeps++;

#if defined(MY_COUNT_MUTEX_CALLERS)
  increment_sleep_for_caller(caller, line);
#endif

  return pthread_rwlock_rdlock(&rw->frw_lock);
}

int my_fastrwlock_wrlock(my_fastrwlock_t *rw
#if defined(MY_COUNT_MUTEX_CALLERS)
                              , const char* caller, int line
#endif
                              )
{
  int   res;
  uint  i;
  uint  maxdelay= fastmutex_max_spin_wait_loops;
  my_fastmutex_stats *fms= &mutex_stats[rw->stats_index];

  fms->fms_locks++;
  for (i= 0; i < rw->fast_spins; i++)
  {
    res= pthread_rwlock_trywrlock(&rw->frw_lock);
    assert(res == 0 || res == EBUSY);

    if (res != EBUSY) {
      fms->fms_fast_spins += i;
      fms->fms_fast_spin_wins += (i > 0);  // don't count zero contention!
      return res;
    }

    pause_cpu();
  }
  fms->fms_fast_spins += rw->fast_spins;

  if (num_current_spinners < MAX_CONCURRENT_SPINNERS) {
    my_atomic_add32(&num_current_spinners, 1);
    for (i= 0; i < rw->slow_spins; i++) {
      res= pthread_rwlock_trywrlock(&rw->frw_lock);
      assert(res == 0 || res == EBUSY);

      if (res != EBUSY) {
        fms->fms_slow_spins += i;
        fms->fms_slow_spin_wins++;
        my_atomic_add32(&num_current_spinners, -1);
        return res;
      }

      mutex_delay(maxdelay);
      maxdelay += park_rng(&rw->rng_state) * fastmutex_max_spin_wait_loops + 1;
    }
    my_atomic_add32(&num_current_spinners, -1);
  }

  fms->fms_slow_spins += rw->slow_spins;
  fms->fms_sleeps++;

#if defined(MY_COUNT_MUTEX_CALLERS)
  increment_sleep_for_caller(caller, line);
#endif

  return pthread_rwlock_wrlock(&rw->frw_lock);
}

int my_fastrwlock_tryrdlock(my_fastrwlock_t *rw)
{
  /* TODO: should counters be updated here */
  return pthread_rwlock_tryrdlock(&rw->frw_lock);
}

int my_fastrwlock_trywrlock(my_fastrwlock_t *rw)
{
  /* TODO: should counters be updated here */
  return pthread_rwlock_trywrlock(&rw->frw_lock);
}

int my_fastrwlock_destroy(my_fastrwlock_t *rw)
{
  return pthread_rwlock_destroy(&rw->frw_lock);
}

int my_fastrwlock_unlock(my_fastrwlock_t *rw)
{
  return pthread_rwlock_unlock(&rw->frw_lock);
}

#endif /* defined(MY_FASTRWLOCK) */
