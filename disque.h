/* Copyright (c) 2014-2019, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved. This code is under the AGPL license, check the
 * LICENSE file for more info. */

#ifndef DISQUE_H
#define DISQUE_H

#include <ctype.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>

typedef long long mstime_t;

#ifndef MAIN_MODULE_FILE
#include "externmodule.h"
#endif

#include "job.h"
#include "queue.h"
#include "ack.h"
#include "utils.h"
#include "sha1.h"
#include "endianconv.h"
#include "skiplist.h"

/* See module.c for a descriptions of the following globals. */
extern rax *Jobs;
extern rax *Queues;
extern rax *BlockedClients;
extern rax *BlockedOnRepl;
extern int ClusterMyselfLeaving;
extern int ClusterReachableNodesCount;
extern char **ClusterReachableNodes;
extern rax *ClusterLeavingNodes;
extern unsigned char JobIDSeed[20];
extern skiplist *AwakeList;

#define DISQUE_PERSIST_DEQUEUED_NONE 0
#define DISQUE_PERSIST_DEQUEUED_ATMOSTONCE 1
#define DISQUE_PERSIST_DEQUEUED_ALL 2
extern int ConfigPersistDequeued;
extern int ConfigLoadQueuedState;

#define UNUSED(V) ((void) V)

#define C_ERR -1
#define C_OK 0

#define DISQUE_MSG_REPLJOB 1
#define DISQUE_MSG_DELJOB 2
#define DISQUE_MSG_YOURJOBS 3
#define DISQUE_MSG_GOTJOB 4
#define DISQUE_MSG_SETACK 5
#define DISQUE_MSG_GOTACK 6
#define DISQUE_MSG_ENQUEUE 7
#define DISQUE_MSG_WORKING 8
#define DISQUE_MSG_QUEUED 9
#define DISQUE_MSG_WILLQUEUE 10
#define DISQUE_MSG_NEEDJOBS 11
#define DISQUE_MSG_PAUSE 12

#define DISQUE_MSG_NOFLAGS 0
#define DISQUE_MSG_FLAG0_NOREPLY    (1<<0) /* Don't reply to this message. */
#define DISQUE_MSG_FLAG0_INCR_NACKS (1<<1) /* Increment job nacks counter. */
#define DISQUE_MSG_FLAG0_INCR_DELIV (1<<2) /* Increment job delivers counter. */

#define DISQUE_TIME_ERR          500     /* Desynchronization (in ms) */



/* Message format for simple messages just having a type, an ID and an aux
 * field. */
typedef struct clusterJobIDMessage {
    uint8_t flags[4];    /* Flags: see DISQUE_MSG_FLAG... */
    uint32_t aux;        /* Aux field used with certain message types:
                          * SETACK: Number of nodes that may have this message.
                          * QUEUEJOB: Delay starting from msg reception. */
    char id[JOB_ID_LEN]; /* Job ID. */
} clusterJobIDMessage;

/* Message format used for REPLJOB and YOURJOBS. This format includes serialized
 * jobs inside. */
typedef struct clusterSerializedJobMessage {
    uint8_t flags[4];    /* Flags: see DISQUE_MSG_FLAG... */
    uint32_t numjobs;   /* Number of jobs stored here. */
    uint32_t datasize;  /* Number of bytes following to describe jobs. */
    /* The variable data section here is composed of 4 bytes little endian
     * prefixed length + serialized job data for each job:
     * [4 bytes len] + [serialized job] + [4 bytes len] + [serialized job] ...
     * For a total of exactly 'datasize' bytes. */
    unsigned char jobs_data[8]; /* Defined as 8 bytes just for alignment. */
} clusterSerializedJobMessage;

/* NEEDJOBS and PAUSE message format. Includes a queue name and an aux
   field that may change meaning depending on the message type. */
typedef struct clusterQueueMessage {
    uint8_t flags[4];    /* Flags: see DISQUE_MSG_FLAG... */
    uint32_t aux;       /* For NEEDJOB, how many jobs we request.
                         * FOR PAUSE, the pause flags to set on the queue. */
    uint32_t qnamelen;  /* Queue name total length. */
    char qname[8];      /* Defined as 8 bytes just for alignment. */
} clusterQueueMessage;

void refreshReachableNodes(RedisModuleCtx *ctx);
void clusterSendSetAck(RedisModuleCtx *ctx, const char *receiver, job *j);
void clusterBroadcastDelJob(RedisModuleCtx *ctx, job *j);
void clusterBroadcastJobIDMessage(RedisModuleCtx *ctx, rax *nodes, const char *id, int type, uint32_t aux, unsigned char flags);
void clusterBroadcastWorking(RedisModuleCtx *ctx, job *j);
void clusterBroadcastQueued(RedisModuleCtx *ctx, job *j, unsigned char flags);
void clusterSendWillQueue(RedisModuleCtx *ctx, job *j);
void clusterSendEnqueue(RedisModuleCtx *ctx, const char *receiver, job *j, uint32_t delay);
void clusterSendGotJob(RedisModuleCtx *ctx, const char *receiver, job *j);
int clusterReplicateJob(RedisModuleCtx *ctx, job *j, int repl, int noreply);
void clusterSendNeedJobs(RedisModuleCtx *ctx, sds qname, int numjobs, rax *nodes);
void clusterSendYourJobs(RedisModuleCtx *ctx, const char *node, job **jobs, uint32_t count);
void clusterBroadcastPause(RedisModuleCtx *ctx, const char *qname, size_t qnamelen, uint32_t flags);
void clusterSendGotAck(RedisModuleCtx *ctx, const char *receiver, char *jobid, int known);
int myselfLeaving(void);
mstime_t getNextGCRetryTime(job *job);
int getMemoryWarningLevel(RedisModuleCtx *ctx);
void flushAllJobsAndQueues(RedisModuleCtx *ctx);

void REPLJOBcallback(RedisModuleCtx *ctx, const char *sender_id, uint8_t msgtype, const unsigned char *payload, uint32_t len);
void YOURJOBScallback(RedisModuleCtx *ctx, const char *sender_id, uint8_t msgtype, const unsigned char *payload, uint32_t len);
void GOTJOBcallback(RedisModuleCtx *ctx, const char *sender_id, uint8_t msgtype, const unsigned char *payload, uint32_t len);
void SETACKcallback(RedisModuleCtx *ctx, const char *sender_id, uint8_t msgtype, const unsigned char *payload, uint32_t len);
void GOTACKcallback(RedisModuleCtx *ctx, const char *sender_id, uint8_t msgtype, const unsigned char *payload, uint32_t len);
void ENQUEUEcallback(RedisModuleCtx *ctx, const char *sender_id, uint8_t msgtype, const unsigned char *payload, uint32_t len);
void WORKINGcallback(RedisModuleCtx *ctx, const char *sender_id, uint8_t msgtype, const unsigned char *payload, uint32_t len);
void QUEUEDcallback(RedisModuleCtx *ctx, const char *sender_id, uint8_t msgtype, const unsigned char *payload, uint32_t len);
void WILLQUEUEcallback(RedisModuleCtx *ctx, const char *sender_id, uint8_t msgtype, const unsigned char *payload, uint32_t len);
void NEEDJOBScallback(RedisModuleCtx *ctx, const char *sender_id, uint8_t msgtype, const unsigned char *payload, uint32_t len);
void PAUSEcallback(RedisModuleCtx *ctx, const char *sender_id, uint8_t msgtype, const unsigned char *payload, uint32_t len);
void DELJOBcallback(RedisModuleCtx *ctx, const char *sender_id, uint8_t msgtype, const unsigned char *payload, uint32_t len);

int disqueCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int loadjobCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#define UNIT_SECONDS 0
#define UNIT_MILLISECONDS 1
int getTimeoutFromObjectOrReply(RedisModuleCtx *ctx, RedisModuleString *object, mstime_t *timeout, int unit);

void DisqueRDBAuxSave(RedisModuleIO *rdb, int when);
int DisqueRDBAuxLoad(RedisModuleIO *rdb, int encver, int when);

#endif
