/* Copyright (C) 2003 MySQL AB

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

#include "mysys_priv.h"

#ifndef HAVE_COMPRESS
#undef DYNAMIC_CRC_TABLE
#include "../zlib/crc32.c"
#endif


/* The below CRC32 implementation is based on the implementation included with
 * zlib with modifications to process 8 bytes at a time and using SSE 4.2
 * extentions when available.  The polynomial constant has been changed to
 * match the one used by SSE 4.2 and does not return the same value as the
 * version used by zlib.  This implementation only supports 64-bit
 * little-endian processors.  The original zlib copyright notice follows. */

/* crc32.c -- compute the CRC-32 of a buf stream
 * Copyright (C) 1995-2005 Mark Adler
 * For conditions of distribution and use, see copyright notice in zlib.h
 *
 * Thanks to Rodney Brown <rbrown64@csc.com.au> for his contribution of faster
 * CRC methods: exclusive-oring 32 bits of buf at a time, and pre-computing
 * tables for updating the shift register in one step with three exclusive-ors
 * instead of four steps with four exclusive-ors.  This results in about a
 * factor of two increase in speed on a Power PC G4 (PPC7455) using gcc -O3.
 */

static uint32 s_fast_crc_table[8][256];
static my_bool s_fast_crc_table_initialized = 0;
static my_bool s_fast_crc_sse2_enabled = 0;

void my_fast_crc32_init(my_bool cpuid_has_crc32)
{
  s_fast_crc_sse2_enabled = cpuid_has_crc32;

  if (cpuid_has_crc32)
    return;

  // bit-reversed poly 0x1EDC6F41 (from SSE42 crc32 instruction)
  static const uint32 poly = 0x82f63b78;

  uint32 n, k, c;

  for (n = 0; n < 256; n++) {
      c = n;
      for (k = 0; k < 8; k++)
        c = (c & 1) ? (poly ^ (c >> 1)) : (c >> 1);
      s_fast_crc_table[0][n] = c;
  }

  for (n = 0; n < 256; n++) {
      c = s_fast_crc_table[0][n];
      for (k = 1; k < 8; k++) {
        c = s_fast_crc_table[0][c & 0xFF] ^ (c >> 8);
        s_fast_crc_table[k][n] = c;
      }
  }

  s_fast_crc_table_initialized = 1;
}

#if defined(__GNUC__) && defined(__x86_64__)

// opcodes taken from objdump of "crc32b (%%rdx), %%rcx"
// for RHEL4 support (GCC 3 doesn't support this instruction)
#define my_fast_crc32_sse42_byte \
    asm (".byte 0xf2, 0x48, 0x0f, 0x38, 0xf0, 0x0a" \
         : "=c"(crc) : "c"(crc), "d"(buf)); \
    len--, buf++;

// opcodes taken from objdump of "crc32q (%%rdx), %%rcx"
// for RHEL4 support (GCC 3 doesn't support this instruction)
#define my_fast_crc32_sse42_quadword \
    asm (".byte 0xf2, 0x48, 0x0f, 0x38, 0xf1, 0x0a" \
         : "=c"(crc) : "c"(crc), "d"(buf)); \
    len -= 8, buf += 8;

STATIC_INLINE uint32 my_fast_crc32_sse42(const uchar* buf, ulong len)
{
  uint64 crc = (uint32)(-1); // this must only set low 32 bits

  while (len && ((uint64)buf & 7))
  {
    my_fast_crc32_sse42_byte;
  }

  while (len >= 32)
  {
    my_fast_crc32_sse42_quadword;
    my_fast_crc32_sse42_quadword;
    my_fast_crc32_sse42_quadword;
    my_fast_crc32_sse42_quadword;
  }

  while (len >= 8)
  {
    my_fast_crc32_sse42_quadword;
  }

  while (len)
  {
    my_fast_crc32_sse42_byte;
  }

  return ((~crc) & 0xFFFFFFFF);
}

#endif // defined(__GNUC__) && defined(__x86_64__)

#define my_fast_crc32_byte \
    crc = (crc >> 8) ^ s_fast_crc_table[0][(crc ^ *buf++) & 0xFF]; \
    len--;

#define my_fast_crc32_quadword \
    crc ^= *(uint64*)buf; \
    crc = s_fast_crc_table[7][(crc      ) & 0xFF] ^ \
          s_fast_crc_table[6][(crc >>  8) & 0xFF] ^ \
          s_fast_crc_table[5][(crc >> 16) & 0xFF] ^ \
          s_fast_crc_table[4][(crc >> 24) & 0xFF] ^ \
          s_fast_crc_table[3][(crc >> 32) & 0xFF] ^ \
          s_fast_crc_table[2][(crc >> 40) & 0xFF] ^ \
          s_fast_crc_table[1][(crc >> 48) & 0xFF] ^ \
          s_fast_crc_table[0][(crc >> 56)]; \
    len -= 8, buf += 8;

STATIC_INLINE uint32 my_fast_crc32_slice8(const uchar* buf, ulong len)
{
  assert(s_fast_crc_table_initialized);

  uint64 crc = (uint32)(-1); // this must only set low 32 bits

  while (len && ((uint64)buf & 7))
  {
    my_fast_crc32_byte;
  }

  while (len >= 32)
  {
    my_fast_crc32_quadword;
    my_fast_crc32_quadword;
    my_fast_crc32_quadword;
    my_fast_crc32_quadword;
  }

  while (len >= 8)
  {
    my_fast_crc32_quadword;
  }

  while (len)
  {
    my_fast_crc32_byte;
  }

  return ((~crc) & 0xFFFFFFFF);
}

uint32 my_fast_crc32(const uchar* buf, ulong len)
{
  if (s_fast_crc_sse2_enabled)
    return my_fast_crc32_sse42(buf, len);
  else
    return my_fast_crc32_slice8(buf, len);
}
