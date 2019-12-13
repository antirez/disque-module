/* Copyright (c) 2014-2019, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved. This code is under the AGPL license, check the
 * LICENSE file for more info. */

#ifndef __DISQUE_ACK_H
#define __DISQUE_ACK_H

void acknowledgeJob(RedisModuleCtx *ctx, job *j);
void tryJobGC(RedisModuleCtx *ctx, job *job);
void gotAckReceived(RedisModuleCtx *ctx, const char *sender, job *job, int known);
mstime_t getNextGCRetryTime(job *job);
int ackjobCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int fastackCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif
