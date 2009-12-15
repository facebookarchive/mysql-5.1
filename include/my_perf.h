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

#ifndef _my_perf_h
#define _my_perf_h

#include "my_global.h"

C_MODE_START

/* Type used for low-overhead timers */
typedef ulonglong my_fast_timer_t;

/* Initialize the fast timer at startup Counts timer ticks for given seconds.
 */
extern void my_init_fast_timer(int seconds);

/* Returns a fast timer suitable for performance measurements. */
extern void my_get_fast_timer(my_fast_timer_t* timer);

/* Returns the difference between now and the time from 'in' in seconds.  Also
   optionally returns current fast timer in 'out'.  It is safe to pass the same
   struct for 'in' and 'out'. */
extern double my_fast_timer_diff_now(my_fast_timer_t const *in,
                                     my_fast_timer_t *out);

/* Return the time in microseconds since CPU started counting or 0 on an error.
*/
extern double my_fast_timer_usecs();

/* Return the time in miliseconds since arbitrary epoch or 0 on an error.  This
   will wrap and should only be used for heuristics and performance analysis.
*/
extern ulong my_fast_timer_msecs();

/* Return the fast timer scale factor.  If this value is 0 fast timers are
   not initialized. */
extern double my_fast_timer_get_scale();

C_MODE_END

#endif /* _my_perf_h */
