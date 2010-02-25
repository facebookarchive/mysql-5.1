/****************************************************************************
 *  flashcache_ioctl.h
 *  Device mapper target for block-level disk caching
 *
 *  Copyright (C) International Business Machines Corp., 2006
 *  Copyright (C) Facebook, 2009
 *  Author: Ming Zhao (mingzhao@ufl.edu)
 *  Author: Mohan Srinivasan (mohan@facebook.com) rewrote all of this.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; under version 2 of the License.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 ****************************************************************************/

#ifndef FLASHCACHE_IOCTL_H
#define FLASHCACHE_IOCTL_H

#include <linux/types.h>

#define FLASHCACHE_IOCTL 0xfe

enum {
	FLASHCACHEADDNCPID_CMD=200,
	FLASHCACHEDELNCPID_CMD,
	FLASHCACHEDELNCALL_CMD,
};

#define FLASHCACHEADDNCPID	_IOW(FLASHCACHE_IOCTL, FLASHCACHEADDNCPID_CMD, pid_t)
#define FLASHCACHEDELNCPID	_IOW(FLASHCACHE_IOCTL, FLASHCACHEDELNCPID_CMD, pid_t)
#define FLASHCACHEDELNCALL	_IOW(FLASHCACHE_IOCTL, FLASHCACHEDELNCALL_CMD, pid_t)

#endif
