/* Copyright (c) 2014, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved. This code is under the AGPL license, check the
 * LICENSE file for more info. */

#ifndef __DISQUE_QUEUE_H
#define __DISQUE_QUEUE_H

#include "skiplist.h"
#include "adlist.h"

#define QUEUE_FLAG_PAUSED_IN (1<<0)
#define QUEUE_FLAG_PAUSED_OUT (1<<1)
#define QUEUE_FLAG_PAUSED_ALL (QUEUE_FLAG_PAUSED_IN|QUEUE_FLAG_PAUSED_OUT)

typedef struct queue {
    sds name;        /* Queue name */
    skiplist *sl;    /* The skiplist with the queued jobs. */
    uint32_t ctime;  /* Creation time of this queue object. */
    uint32_t atime;  /* Last access time. Updated when a new element is
                        queued or when a new client fetches elements or
                        blocks for elements to arrive. */
    list *clients;   /* RedisModuleBlockedClient references of clients
                        blocked here. */

    /* === Federation related fields === */
    mstime_t needjobs_bcast_time; /* Last NEEDJOBS cluster broadcast. */
    mstime_t needjobs_adhoc_time; /* Last NEEDJOBS to notable nodes. */

    /* Set of nodes that provided jobs. The key is the node, the value
     * is the unix time of the last time we received data from this node
     * about this queue. */
    rax *needjobs_responders;

    /* Tracking of incoming messages rate (messages received from other
     * nodes because of NEEDJOBS).
     *
     * This is not going to be perfect, but we need to be memory
     * efficient, and have some rought ideas to optimize NEEDJOBS messages.
     *
     * As soon as we receive jobs in the current second, we increment
     * the current count. Otherwise we store the current data into the
     * previous data, and set the current time to the current unix time.
     *
     * Instantaneous receives jobs/sec is just:
     *
     *     jobs_sec = (current_count+prev_count) * 1000 / (now-prev_time)
     */
    mstime_t current_import_jobs_time;
    mstime_t prev_import_jobs_time;
    uint32_t current_import_jobs_count;
    uint32_t prev_import_jobs_count;
    uint32_t needjobs_bcast_attempt; /* Num of tries without new nodes. */
    uint32_t needjobs_adhoc_attempt; /* Num of tries without new jobs. */
    uint64_t jobs_in, jobs_out;      /* Num of jobs enqueued and dequeued. */
    uint32_t flags;                  /* Queue flags. QUEUE_FLAG_* macros. */
    uint32_t padding;                /* Not used. Makes alignment obvious. */
} queue;

struct clusterNode;

#define GETJOB_FLAG_NONE 0
#define GETJOB_FLAG_WITHCOUNTERS (1<<0)

#define NEEDJOBS_CLIENTS_WAITING 0 /* Called because clients are waiting. */
#define NEEDJOBS_REACHED_ZERO 1    /* Called since we just ran out of jobs. */

queue *lookupQueue(const char *name, size_t namelen);
int destroyQueue(const char *name, size_t namelen);
int enqueueJob(RedisModuleCtx *ctx, job *job, int nack);
int dequeueJob(job *job);
job *queueFetchJob(RedisModuleCtx *ctx, queue *q, unsigned long *qlen);
unsigned long queueLength(queue *q);
unsigned long queueNameLength(const char *qname, size_t qnamelen);
void cleanupClientBlockedForJobs(RedisModuleCtx *ctx, RedisModuleBlockedClient *bc);
void handleClientsBlockedOnQueues(void);
void needJobsForQueue(RedisModuleCtx *ctx, queue *q, int type);
void needJobsForQueueName(RedisModuleCtx *ctx, RedisModuleString *qname, int type);
void receiveYourJobs(RedisModuleCtx *ctx, const char *node, uint32_t numjobs, unsigned char *serializedjobs, uint32_t serializedlen);
void receiveNeedJobs(RedisModuleCtx *ctx, const char *node, const char *qname, size_t qnamelen, uint32_t count);
void queueChangePausedState(RedisModuleCtx *ctx, queue *q, int flag, int set);
void receivePauseQueue(RedisModuleCtx *ctx, const char *qname, size_t qnamelen, uint32_t flags);
int clientsCronSendNeedJobs(RedisModuleCtx *ctx);
int evictIdleQueues(RedisModuleCtx *ctx);
int qlenCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int getjobCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int enqueueCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int dequeueCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int nackCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int qpeekCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int workingCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int qstatCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int pauseCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif
