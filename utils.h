/* Copyright (c) 2014-2019, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved. This code is under the AGPL license, check the
 * LICENSE file for more info. */

#ifndef __UTILS_H
#define __UTILS_H

#include <time.h>

typedef long long mstime_t;
mstime_t mstime(void);
mstime_t randomTimeError(mstime_t milliseconds);

#endif
