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
  perf->slow_ios = 0;
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
  sum->slow_ios += perf->slow_ios;
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

  if (a->slow_ios > b->slow_ios)
    diff->slow_ios = a->slow_ios - b->slow_ios;
  else
    diff->slow_ios = 0;

  diff->svc_usecs_max = max(a->svc_usecs_max, b->svc_usecs_max);
  diff->wait_usecs_max = max(a->wait_usecs_max, b->wait_usecs_max);
}

/**********************************************************************
Accumulate per-table IO stats helper function using atomic ops */
void my_io_perf_sum_atomic(my_io_perf_t* sum, longlong bytes,
    longlong requests, longlong svc_usecs, longlong wait_usecs,
    longlong slow_ios)
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

  my_atomic_add_bigint(&sum->slow_ios, slow_ios);
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

uint32 sbox[256] = 
  {
    0xF53E1837, 0x5F14C86B, 0x9EE3964C, 0xFA796D53,
    0x32223FC3, 0x4D82BC98, 0xA0C7FA62, 0x63E2C982,
    0x24994A5B, 0x1ECE7BEE, 0x292B38EF, 0xD5CD4E56,
    0x514F4303, 0x7BE12B83, 0x7192F195, 0x82DC7300,
    0x084380B4, 0x480B55D3, 0x5F430471, 0x13F75991,
    0x3F9CF22C, 0x2FE0907A, 0xFD8E1E69, 0x7B1D5DE8,
    0xD575A85C, 0xAD01C50A, 0x7EE00737, 0x3CE981E8,
    0x0E447EFA, 0x23089DD6, 0xB59F149F, 0x13600EC7,
    0xE802C8E6, 0x670921E4, 0x7207EFF0, 0xE74761B0,
    0x69035234, 0xBFA40F19, 0xF63651A0, 0x29E64C26,
    0x1F98CCA7, 0xD957007E, 0xE71DDC75, 0x3E729595,
    0x7580B7CC, 0xD7FAF60B, 0x92484323, 0xA44113EB,
    0xE4CBDE08, 0x346827C9, 0x3CF32AFA, 0x0B29BCF1,
    0x6E29F7DF, 0xB01E71CB, 0x3BFBC0D1, 0x62EDC5B8,
    0xB7DE789A, 0xA4748EC9, 0xE17A4C4F, 0x67E5BD03,
    0xF3B33D1A, 0x97D8D3E9, 0x09121BC0, 0x347B2D2C,
    0x79A1913C, 0x504172DE, 0x7F1F8483, 0x13AC3CF6,
    0x7A2094DB, 0xC778FA12, 0xADF7469F, 0x21786B7B,
    0x71A445D0, 0xA8896C1B, 0x656F62FB, 0x83A059B3,
    0x972DFE6E, 0x4122000C, 0x97D9DA19, 0x17D5947B,
    0xB1AFFD0C, 0x6EF83B97, 0xAF7F780B, 0x4613138A,
    0x7C3E73A6, 0xCF15E03D, 0x41576322, 0x672DF292,
    0xB658588D, 0x33EBEFA9, 0x938CBF06, 0x06B67381,
    0x07F192C6, 0x2BDA5855, 0x348EE0E8, 0x19DBB6E3,
    0x3222184B, 0xB69D5DBA, 0x7E760B88, 0xAF4D8154,
    0x007A51AD, 0x35112500, 0xC9CD2D7D, 0x4F4FB761,
    0x694772E3, 0x694C8351, 0x4A7E3AF5, 0x67D65CE1,
    0x9287DE92, 0x2518DB3C, 0x8CB4EC06, 0xD154D38F,
    0xE19A26BB, 0x295EE439, 0xC50A1104, 0x2153C6A7,
    0x82366656, 0x0713BC2F, 0x6462215A, 0x21D9BFCE,
    0xBA8EACE6, 0xAE2DF4C1, 0x2A8D5E80, 0x3F7E52D1,
    0x29359399, 0xFEA1D19C, 0x18879313, 0x455AFA81,
    0xFADFE838, 0x62609838, 0xD1028839, 0x0736E92F,
    0x3BCA22A3, 0x1485B08A, 0x2DA7900B, 0x852C156D,
    0xE8F24803, 0x00078472, 0x13F0D332, 0x2ACFD0CF,
    0x5F747F5C, 0x87BB1E2F, 0xA7EFCB63, 0x23F432F0,
    0xE6CE7C5C, 0x1F954EF6, 0xB609C91B, 0x3B4571BF,
    0xEED17DC0, 0xE556CDA0, 0xA7846A8D, 0xFF105F94,
    0x52B7CCDE, 0x0E33E801, 0x664455EA, 0xF2C70414,
    0x73E7B486, 0x8F830661, 0x8B59E826, 0xBB8AEDCA,
    0xF3D70AB9, 0xD739F2B9, 0x4A04C34A, 0x88D0F089,
    0xE02191A2, 0xD89D9C78, 0x192C2749, 0xFC43A78F,
    0x0AAC88CB, 0x9438D42D, 0x9E280F7A, 0x36063802,
    0x38E8D018, 0x1C42A9CB, 0x92AAFF6C, 0xA24820C5,
    0x007F077F, 0xCE5BC543, 0x69668D58, 0x10D6FF74,
    0xBE00F621, 0x21300BBE, 0x2E9E8F46, 0x5ACEA629,
    0xFA1F86C7, 0x52F206B8, 0x3EDF1A75, 0x6DA8D843,
    0xCF719928, 0x73E3891F, 0xB4B95DD6, 0xB2A42D27,
    0xEDA20BBF, 0x1A58DBDF, 0xA449AD03, 0x6DDEF22B,
    0x900531E6, 0x3D3BFF35, 0x5B24ABA2, 0x472B3E4C,
    0x387F2D75, 0x4D8DBA36, 0x71CB5641, 0xE3473F3F,
    0xF6CD4B7F, 0xBF7D1428, 0x344B64D0, 0xC5CDFCB6,
    0xFE2E0182, 0x2C37A673, 0xDE4EB7A3, 0x63FDC933,
    0x01DC4063, 0x611F3571, 0xD167BFAF, 0x4496596F,
    0x3DEE0689, 0xD8704910, 0x7052A114, 0x068C9EC5,
    0x75D0E766, 0x4D54CC20, 0xB44ECDE2, 0x4ABC653E,
    0x2C550A21, 0x1A52C0DB, 0xCFED03D0, 0x119BAFE2,
    0x876A6133, 0xBC232088, 0x435BA1B2, 0xAE99BBFA,
    0xBB4F08E4, 0xA62B5F49, 0x1DA4B695, 0x336B84DE,
    0xDC813D31, 0x00C134FB, 0x397A98E6, 0x151F0E64,
    0xD9EB3E69, 0xD3C7DF60, 0xD2F2C336, 0x2DDD067B,
    0xBD122835, 0xB0B3BD3A, 0xB0D54E46, 0x8641F1E4,
    0xA0B38F96, 0x51D39199, 0x37A6AD75, 0xDF84EE41,
    0x3C034CBA, 0xACDA62FC, 0x11923B8B, 0x45EF170A,
  };

uint32 my_sbox_hash(const uchar* data, ulong length) {
  ulong i;
  uint32 hash = 0;
  for(i = 0; i < length; i++) {
    hash ^= sbox[ data[i] ];
    hash *= 3;
  }
  return hash;
}

