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

#ifndef _my_perf_h
#define _my_perf_h

#include "my_global.h"

C_MODE_START

/* Initialize the fast timer at startup
   Counts timer ticks for given seconds.
   Returns ticks counted in given seconds or 0 on failure. */
extern ulonglong my_init_fast_timer(int seconds);

typedef ulonglong my_fast_timer_t;

/* Returns a fast timer suitable for performance measurements. */
extern void my_get_fast_timer(my_fast_timer_t* timer);

/* Returns the difference between now and the time from 'in' in seconds.
   Also optionally returns current fast timer in 'out'.
   It is safe to pass the same struct for 'in' and 'out'.
*/
extern double my_fast_timer_diff_now(my_fast_timer_t const *in, my_fast_timer_t *out);

extern double my_fast_timer_usecs();

C_MODE_END
#endif /* _my_perf_h */
