/*
 * Copyright (c) 2014, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 */


#ifndef __DISQUE_ACK_H
#define __DISQUE_ACK_H

void acknowledgeJob(job *j);
void tryJobGC(job *j);
void gotAckReceived(clusterNode *sender, job *job, int known);
mstime_t getNextGCRetryTime(job *job);

#endif
