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

double my_tsc_scale = 0;

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
Accumulate per-table IO stats helper function using atomic ops */
void my_io_perf_sum_atomic(my_io_perf_t* sum, longlong bytes,
    longlong requests, longlong svc_usecs, longlong wait_usecs,
    longlong old_ios)
{
  my_atomic_add64(&sum->bytes, bytes);
  my_atomic_add64(&sum->requests, requests);

  my_atomic_add64(&sum->svc_usecs, svc_usecs);
  my_atomic_add64(&sum->wait_usecs, wait_usecs);

  // In the unlikely case that two threads attempt to update the max
  // value at the same time, only the first will succeed.  It's possible
  // that the second thread would have set a larger max value, but we
  // would rather error on the side of simplicity and avoid looping the
  // compare-and-swap.

  longlong old_svc_usecs_max = sum->svc_usecs_max;
  if (svc_usecs > old_svc_usecs_max)
    my_atomic_cas64(&sum->svc_usecs_max, &old_svc_usecs_max, svc_usecs);

  longlong old_wait_usecs_max = sum->wait_usecs_max;
  if (wait_usecs > old_wait_usecs_max)
    my_atomic_cas64(&sum->wait_usecs_max, &old_wait_usecs_max, wait_usecs);

  my_atomic_add64(&sum->old_ios, old_ios);
}

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
