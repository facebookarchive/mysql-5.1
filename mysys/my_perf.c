/*******************************************************************
Various performance statistics utilities.
********************************************************************/

#include "my_perf.h"

/**************************************************************
Reads the time stamp counter on an Intel processor */

static __inline__
ulonglong
rdtsc(void)
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

/***********************************************************************//**
Return the time in microseconds since CPU started counting or 0 on an error. */
double
my_fast_timer_usecs()
/*===================*/
	/* out: time in microseconds */
{
  return 1000000.0 * my_tsc_scale * rdtsc();
}
