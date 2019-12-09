/* Copyright (c) 2014-2019, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved. This code is under the AGPL license, check the
 * LICENSE file for more info. */

#include "utils.h"

#include <stdlib.h>
#include <sys/time.h>

/* Return the UNIX time in microseconds */
long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long long)tv.tv_sec)*1000000;
    ust += tv.tv_usec;
    return ust;
}

/* Return the UNIX time in milliseconds */
mstime_t mstime(void) {
    return ustime()/1000;
}

/* Return a random time error between -(milliseconds/2) and +(milliseconds/2).
 * This is useful in order to desynchronize multiple nodes competing for the
 * same operation in a best-effort way. */
mstime_t randomTimeError(mstime_t milliseconds) {
    return rand()%milliseconds - milliseconds/2;
}

