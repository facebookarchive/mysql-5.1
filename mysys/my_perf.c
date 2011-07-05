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

/* Various performance statistics utilities. */

#include "my_perf.h"
#include "my_atomic.h"
#include "string.h"

double my_tsc_scale = 0;

my_bool my_fast_timer_enabled = 0;

/***********************************************************************//**
Initialize an my_io_perf_t struct. */
void my_io_perf_init(my_io_perf_t* perf)
{
  perf->bytes = 0;
  perf->requests = 0;
  perf->svc_usecs = 0;
  perf->svc_usecs_max = 0;
  perf->wait_usecs = 0;
  perf->wait_usecs_max = 0;
  perf->old_ios = 0;
}

/**********************************************************************
Accumulate per-table IO stats helper function */
void my_io_perf_sum(my_io_perf_t* sum, const my_io_perf_t* perf)
{
  sum->bytes += perf->bytes;
  sum->requests += perf->requests;
  sum->svc_usecs += perf->svc_usecs;
  sum->svc_usecs_max = max(sum->svc_usecs_max, perf->svc_usecs_max);
  sum->wait_usecs += perf->wait_usecs;
  sum->wait_usecs_max = max(sum->wait_usecs_max, perf->wait_usecs_max);
  sum->old_ios += perf->old_ios;
}

/**********************************************************************
Return a - b in diff */
void my_io_perf_diff(my_io_perf_t* diff,
                    const my_io_perf_t* a, const my_io_perf_t* b)
{
  if (a->bytes > b->bytes)
    diff->bytes = a->bytes - b->bytes;
  else
    diff->bytes = 0;

  if (a->requests > b->requests)
    diff->requests = a->requests - b->requests;
  else
    diff->requests = 0;

  if (a->svc_usecs > b->svc_usecs)
    diff->svc_usecs = a->svc_usecs - b->svc_usecs;
  else
    diff->svc_usecs = 0;

  if (a->wait_usecs > b->wait_usecs)
    diff->wait_usecs = a->wait_usecs - b->wait_usecs;
  else
    diff->wait_usecs = 0;

  if (a->old_ios > b->old_ios)
    diff->old_ios = a->old_ios - b->old_ios;
  else
    diff->old_ios = 0;

  diff->svc_usecs_max = max(a->svc_usecs_max, b->svc_usecs_max);
  diff->wait_usecs_max = max(a->wait_usecs_max, b->wait_usecs_max);
}

/**********************************************************************
Accumulate per-table IO stats helper function using atomic ops */
void my_io_perf_sum_atomic(my_io_perf_t* sum, longlong bytes,
    longlong requests, longlong svc_usecs, longlong wait_usecs,
    longlong old_ios)
{
  my_atomic_bigint old_svc_usecs_max, old_wait_usecs_max;

  my_atomic_add_bigint(&sum->bytes, bytes);
  my_atomic_add_bigint(&sum->requests, requests);

  my_atomic_add_bigint(&sum->svc_usecs, svc_usecs);
  my_atomic_add_bigint(&sum->wait_usecs, wait_usecs);

  // In the unlikely case that two threads attempt to update the max
  // value at the same time, only the first will succeed.  It's possible
  // that the second thread would have set a larger max value, but we
  // would rather error on the side of simplicity and avoid looping the
  // compare-and-swap.

  old_svc_usecs_max = sum->svc_usecs_max;
  if (svc_usecs > old_svc_usecs_max)
    my_atomic_cas_bigint(&sum->svc_usecs_max, &old_svc_usecs_max, svc_usecs);

  old_wait_usecs_max = sum->wait_usecs_max;
  if (wait_usecs > old_wait_usecs_max)
    my_atomic_cas_bigint(&sum->wait_usecs_max, &old_wait_usecs_max, wait_usecs);

  my_atomic_add_bigint(&sum->old_ios, old_ios);
}


void my_init_fast_timer(int seconds)
{
  int usec = 0;
  longlong min_delta = 0;

#ifdef TARGET_OS_LINUX
  /* We need to identify whether the time stamp counters are synchronized
     between the CPUs.  We do this by setting the processor affinity to each HW
     thread in turn, sleeping briefly, and then checking the time delta.  This
     includes wrapping around to compare CPU 0 with CPU N-1.  We find the
     smallest time delta between them.  If it is negative, then the CPUs are
     out of sync and fast timers revert to using gettimeofday().  If it is
     positive, we use this to compute the scale factor to convert between time
     stamp counter values and seconds.  We use the smallest value for this
     calibration because usleep may sleep longer depending on system load, so
     the smallest non-negative value is most accurate. */

  int ncpu = sysconf(_SC_NPROCESSORS_ONLN);

  if (ncpu > 0)
  {
    cpu_set_t mask;
    usec = 1000000 * seconds / ncpu;

    CPU_ZERO(&mask);
    CPU_SET(0, &mask);
    if (sched_setaffinity(0, sizeof(mask), &mask) == 0)
    {
      int cpu;
      ulonglong last;

      sched_yield(); // make sure the scheduler is invoked to move to CPU 0

      last = rdtsc();
      for (cpu = 1; cpu <= ncpu; cpu++)
      {
        ulonglong now;
        longlong delta;

        CPU_CLR(cpu-1, &mask);
        CPU_SET(cpu < ncpu ? cpu : 0, &mask); // wrap cpu == ncpu back to 0
        if (sched_setaffinity(0, sizeof(mask), &mask) == -1)
        {
          min_delta = 0;
          break;
        }

        usleep(usec);
        now = rdtsc();
        delta = now - last;
        last = now;

        if (cpu == 1 || delta < min_delta)
          min_delta = delta;
      }

      for (cpu = 0; cpu < ncpu; cpu++)
        CPU_SET(cpu, &mask);
      sched_setaffinity(0, sizeof(mask), &mask);
    }
  }

  my_fast_timer_enabled = (min_delta > 0);
#endif /* TARGET_OS_LINUX */

  if (my_fast_timer_enabled)
    my_tsc_scale = usec / (min_delta * 1000000.0);
  else
    my_tsc_scale = 1.0 / 1000000.0;
}


static my_bool my_cpuid(uint32 vend[3], uint32* model,
                        uint32* family, uint32* stepping,
                        uint32* features_ecx, uint32* features_edx)
{
#if defined(__GNUC__) && defined(__x86_64__)
  uint32 sig;
  asm ("cpuid" : "=b" (vend[0]), "=c" (vend[2]), "=d" (vend[1]) : "a" (0));
  asm ("cpuid" : "=a" (sig), "=c" (*features_ecx), "=d" (*features_edx)
               : "a" (1)
               : "ebx");

  *model = ((sig >> 4) & 0xF);
  *family = ((sig >> 8) & 0xF);
  *stepping = (sig & 0xF);

  if (memcmp(vend, "GenuineIntel", 12) == 0 ||
      (memcmp(vend, "AuthenticAMD", 12) == 0 && *family == 0xF))
  {
    *model += (((sig >> 16) & 0xF) << 4);
    *family += ((sig >> 20) & 0xFF);
  }

  fprintf(stderr,
      "CPUID %.12s model %d family %d stepping %d features %08X %08X\n",
      (char*)vend, *model, *family, *stepping, *features_ecx, *features_edx);

  return 1;
#endif
  return 0;
}



void my_fast_crc32_init(my_bool cpuid_has_crc32);

void my_init_cpu_optimizations()
{
  my_bool cpuid_has_crc32 = 0;

#if defined(__GNUC__) && defined(__x86_64__)
  uint32 vend[3], model, family, stepping, features_ecx, features_edx;

  my_cpuid(vend, &model, &family, &stepping, &features_ecx, &features_edx);

  cpuid_has_crc32 = (features_ecx >> 20) & 1;
#endif

  fprintf(stderr, "SSE4.2 CRC32 is %s\n",
		  cpuid_has_crc32 ? "enabled" : "disabled");

  my_fast_crc32_init(cpuid_has_crc32);
}

