/*
 * Copyright (c) 2014, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 */


#ifndef __DISQUE_ACK_H
#define __DISQUE_ACK_H

void acknowledgeJob(job *j);
void tryJobGC(RedisModuleCtx *ctx, job *job);
void gotAckReceived(RedisModuleCtx *ctx, const char *sender, job *job, int known);
mstime_t getNextGCRetryTime(job *job);
int ackjobCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int fastackCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif
