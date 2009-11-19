/*******************************************************************
Various utilities for Innobase.

(c) 1994, 1995 Innobase Oy

Created 5/11/1994 Heikki Tuuri
********************************************************************/

#include "mysys_priv.h"
#include "string.h"

int
my_usectime(
/*========*/
			/* out: 0 on success, -1 otherwise */
	ulong*	sec,	/* out: seconds since the Epoch */
	ulong*	ms)	/* out: microseconds since the Epoch+*sec */
{
	struct timeval	tv;
	int		ret;
	int		errno_gettimeofday;
	int		i;

	for (i = 0; i < 10; i++) {

		ret = gettimeofday(&tv, NULL);

		if (ret == -1) {
			errno_gettimeofday = errno;
			my_print_timestamp(stderr);
			fprintf(stderr, "  MySQL: gettimeofday(): %s\n",
				strerror(errno_gettimeofday));
			usleep(100000);  /* 0.1 sec */
			errno = errno_gettimeofday;
		} else {
			break;
		}
	}

	if (ret != -1) {
		*sec = (ulong) tv.tv_sec;
		*ms  = (ulong) tv.tv_usec;
	}

	return(ret);
}

/**************************************************************
Returns difference in microseconds between (start_sec,start_usec) and now.
Returns 0 when the result would be negative or the calls to my_usectime failed. */

double
my_usecdiff_now(
/*========================*/
				/* out: difference in microseconds */
	int	start_res,	/* in: return from start my_usectime call */
	ulong	start_sec,	/* in: sec from start my_usectime call */
	ulong	start_usec)	/* in: ms from start my_usectime call */
{
	int	end_res;
	ulong	end_sec, end_usec;
	double	result;

	end_res = my_usectime(&end_sec, &end_usec);

	if (end_res || start_res)
		return 0;	/* my_usectime failed */

	double end_mics = end_sec * 1000000LL + end_usec;
	double start_mics = start_sec * 1000000LL + start_usec;
	result = end_mics - start_mics;

	return (result >= 0) ? result : 0;
}

/**************************************************************
Prints a timestamp to a file. */

void
my_print_timestamp(
    /*===============*/
    FILE*  file) /* in: file where to print */
{
	struct tm  cal_tm;
	struct tm* cal_tm_ptr;
	time_t     tm;

	time(&tm);

#ifdef HAVE_LOCALTIME_R
	localtime_r(&tm, &cal_tm);
	cal_tm_ptr = &cal_tm;
#else
	cal_tm_ptr = localtime(&tm);
#endif
	fprintf(file,"%02d%02d%02d %2d:%02d:%02d",
	    cal_tm_ptr->tm_year % 100,
	    cal_tm_ptr->tm_mon + 1,
	    cal_tm_ptr->tm_mday,
	    cal_tm_ptr->tm_hour,
	    cal_tm_ptr->tm_min,
	    cal_tm_ptr->tm_sec);
}


/**************************************************************
Reads the time stamp counter on an Intel processor */

static __inline__ ulonglong rdtsc(void)
{
  ulonglong result;

#if defined(__GNUC__) && defined(__i386__)
  __asm__ __volatile__ ("rdtsc" : "=A" (result));
#elif defined(__GNUC__) && defined(__x86_64__)
  __asm__ __volatile__ ("rdtsc\n\t" \
                        "shlq $32,%%rdx\n\t" \
                        "orq %%rdx,%%rax"
                        : "=a" (result) :: "%edx");
#elif defined(__GNUC__) && defined(__ia64__)
  __asm __volatile__ ("mov %0=ar.itc" : "=r" (result));
#else
  result = 0;
  assert(! "Aborted: rdtsc unimplemented for this configuration.");
#endif

  return result;

}

/**************************************************************
The inverse of the CPU frequency used to convert the time stamp counter
to seconds. */
static double my_tsc_scale = 0;

ulonglong
my_init_fast_timer(int seconds)
{
  ulonglong delta;
  int retry = 3;

  do {
    ulonglong before = rdtsc();
    sleep(seconds);
    delta = rdtsc() - before;
  } while (retry-- && delta < 0);

  my_tsc_scale = (delta > 0) ? (double)seconds / delta : 0;

  return delta;
}

void
my_get_fast_timer(my_fast_timer_t* timer)
{
  *timer = rdtsc();
}

double
my_fast_timer_diff_now(my_fast_timer_t const *in, my_fast_timer_t *out)
{
  my_fast_timer_t now = rdtsc();

  ulonglong delta = now - *in;

  if (out) {
    *out = now;
  }

  return (delta > 0) ? my_tsc_scale * delta : 0;
}
