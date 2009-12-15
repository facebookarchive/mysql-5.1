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

/* Reads the time stamp counter on an Intel processor */
static __inline__
ulonglong
rdtsc(void)
{
  ulonglong tsc;

#if defined(__GNUC__) && defined(__i386__)
  __asm__ __volatile__ ("rdtsc" : "=A" (tsc));
#elif defined(__GNUC__) && defined(__x86_64__)
  uint high, low;
  __asm__ __volatile__ ("rdtsc" : "=a"(low), "=d"(high));
  tsc = (((ulonglong)high)<<32) | low;
#else
  tsc = 0;
  assert(! "Aborted: rdtsc unimplemented for this configuration.");
#endif

  return tsc;
}

/* The inverse of the CPU frequency used to convert the time stamp counter
   to seconds. */
static double my_tsc_scale = 0;

void my_init_fast_timer(int seconds)
{
  ulonglong delta;
  int retry = 3;

  do {
    ulonglong before = rdtsc();
    sleep(seconds);
    delta = rdtsc() - before;
  } while (retry-- && delta < 0);

  my_tsc_scale = (delta > 0) ? (double)seconds / delta : 0;
}

void my_get_fast_timer(my_fast_timer_t* timer)
{
  *timer = rdtsc();
}

double my_fast_timer_diff_now(my_fast_timer_t const *in,
                              my_fast_timer_t *out)
{
  my_fast_timer_t now = rdtsc();

  ulonglong delta = now - *in;

  if (out) {
    *out = now;
  }

  return (delta > 0) ? my_tsc_scale * delta : 0;
}

double my_fast_timer_usecs()
{
  return 1000000.0 * my_tsc_scale * rdtsc();
}

ulong my_fast_timer_msecs()
{
  return (ulong)(1000.0 * my_tsc_scale * rdtsc());
}

double my_fast_timer_get_scale()
{
  return my_tsc_scale;
}
