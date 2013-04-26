# Copyright (c) 2008 PrimeBase Technologies GmbH
# 
# PrimeBase XT
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
#
# 2006-03-22	Paul McCullagh
#
# H&G2JCtL
#
# This file is used to make the Windows version

SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DSAFEMALLOC -DSAFE_MUTEX -DDEBUG")
SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -DSAFEMALLOC -DSAFE_MUTEX -DDEBUG")

INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/include
					${CMAKE_SOURCE_DIR}/storage/pbxt/src)

SET(PBXTSTAT_SOURCES xtstat_xt.cc)

ADD_EXECUTABLE(xtstat_xt ${PBXTSTAT_SOURCES})
TARGET_LINK_LIBRARIES(xtstat_xt mysqlclient_notls wsock32 pbxt)
