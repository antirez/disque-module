/* Queue implementation. In Disque a given node can have active jobs
 * (not ACKed) that are not queued. Only queued jobs can be retrieved by
 * workers via GETJOB. This file implements the local node data structures
 * and functions to model the queue.
 *
 * ---------------------------------------------------------------------------
 *
 * Copyright (c) 2014-2019, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved. This code is under the AGPL license, check the
 * LICENSE file for more info. */

#include "disque.h"

#include <math.h>

void signalQueueAsReady(RedisModuleCtx *ctx, queue *q);

/* ------------------------ Low level queue functions ----------------------- */

/* Job comparison inside a skiplist: by ctime, if ctime is the same by
 * job ID. */
int skiplistCompareJobsInQueue(const void *a, const void *b) {
    const job *ja = a, *jb = b;

    if (ja->ctime > jb->ctime) return 1;
    if (jb->ctime > ja->ctime) return -1;
    return memcmp(ja->id,jb->id,JOB_ID_LEN);
}

/* Crete a new queue, register it in the queue hash tables.
 * On success the pointer to the new queue is returned. If a queue with the
 * same name already NULL is returned. */
queue *createQueue(const char *name, size_t namelen) {
    mstime_t now_ms = mstime();

    if (raxFind(Queues,(unsigned char*)name,namelen) != raxNotFound)
        return NULL;

    queue *q = RedisModule_Alloc(sizeof(queue));
    q->name = sdsnewlen(name,namelen);
    q->flags = 0;
    q->sl = skiplistCreate(skiplistCompareJobsInQueue);
    q->ctime = q->atime = now_ms/1000;
    q->needjobs_bcast_time = 0;
    q->needjobs_bcast_attempt = 0;
    q->needjobs_adhoc_time = 0;
    q->needjobs_adhoc_attempt = 0;
    q->needjobs_responders = NULL; /* Created on demand to save memory. */
    q->clients = NULL; /* Created on demand to save memory. */

    q->current_import_jobs_time = now_ms;
    q->current_import_jobs_count = 0;
    q->prev_import_jobs_time = now_ms;
    q->prev_import_jobs_count = 0;
    q->jobs_in = 0;
    q->jobs_out = 0;

    raxInsert(Queues,(unsigned char*)name,namelen,q,NULL);
    return q;
}

/* Return the queue by name, or NULL if it does not exist. */
queue *lookupQueue(const char *name, size_t namelen) {
    queue *q = raxFind(Queues,(unsigned char*)name,namelen);
    if (q == raxNotFound) return NULL;
    return q;
}

/* Destroy a queue and unregisters it. On success C_OK is returned,
 * otherwise if no queue exists with the specified name, C_ERR is
 * returned. */
int destroyQueue(const char *name, size_t namelen) {
    queue *q = lookupQueue(name,namelen);
    if (!q) return C_ERR;

    raxRemove(Queues,(unsigned char*)name,namelen,NULL);
    sdsfree(q->name);
    skiplistFree(q->sl);
    if (q->needjobs_responders) raxFree(q->needjobs_responders);
    if (q->clients) {
        RedisModule_Assert(listLength(q->clients) == 0);
        listRelease(q->clients);
    }
    RedisModule_Free(q);
    return C_OK;
}

/* Send a job as a return value of a command. This is about jobs but inside
 * queue.c since this is the format used in order to return a job from a
 * queue, as an array [queue_name,id,body]. */
void addReplyJob(RedisModuleCtx *ctx, job *j, int flags) {
    int arraylen = 3;

    if (flags & GETJOB_FLAG_WITHCOUNTERS) arraylen += 4;
    RedisModule_ReplyWithArray(ctx,arraylen);

    RedisModule_ReplyWithStringBuffer(ctx,j->queue,sdslen(j->queue));
    RedisModule_ReplyWithStringBuffer(ctx,j->id,JOB_ID_LEN);
    RedisModule_ReplyWithStringBuffer(ctx,j->body,sdslen(j->body));
    /* Job additional information is returned as key-value pairs. */
    if (flags & GETJOB_FLAG_WITHCOUNTERS) {
        RedisModule_ReplyWithSimpleString(ctx,"nacks");
        RedisModule_ReplyWithLongLong(ctx,j->num_nacks);

        RedisModule_ReplyWithSimpleString(ctx,"additional-deliveries");
        RedisModule_ReplyWithLongLong(ctx,j->num_deliv);
    }
}

/* ------------------------ Queue higher level API -------------------------- */

/* Queue the job and change its state accordingly. If the job is already
 * in QUEUED state, or the job has retry set to 0 and the JOB_FLAG_DELIVERED
 * flat set, C_ERR is returned, otherwise C_OK is returned and the operation
 * succeeds.
 *
 * The nack argument is set to 1 if the enqueue is the result of a client
 * negative acknowledge. */
int enqueueJob(RedisModuleCtx *ctx, job *job, int nack) {
    if (job->state == JOB_STATE_QUEUED || job->qtime == 0) return C_ERR;
    if (job->retry == 0 && job->flags & JOB_FLAG_DELIVERED) return C_ERR;

    mstime_t now_ms = mstime();
    RedisModule_Log(ctx,"verbose","QUEUED %.*s", JOB_ID_LEN, job->id);

    job->state = JOB_STATE_QUEUED;

    /* Put the job into the queue and update the time we'll queue it again. */
    if (job->retry) {
        job->flags |= JOB_FLAG_BCAST_WILLQUEUE;
        job->qtime = now_ms +
                     job->retry*1000 +
                     randomTimeError(DISQUE_TIME_ERR);
    } else {
        job->qtime = 0; /* Never re-queue at most once jobs. */
    }

    /* The first time a job is queued we don't need to broadcast a QUEUED
     * message, to save bandwidth. But the next times, when the job is
     * re-queued for lack of acknowledge, this is useful to (best effort)
     * avoid multiple nodes to re-queue the same job. */
    if (job->flags & JOB_FLAG_BCAST_QUEUED || nack) {
        unsigned char flags = nack ? DISQUE_MSG_FLAG0_INCR_NACKS :
                                     DISQUE_MSG_FLAG0_INCR_DELIV;
        clusterBroadcastQueued(ctx,job,flags);
        /* Other nodes will increment their NACKs / additional deliveries
         * counters when they'll receive the QUEUED message. We need to
         * do the same for the local copy of the job. */
        if (nack)
            job->num_nacks++;
        else
            job->num_deliv++;
    } else {
        job->flags |= JOB_FLAG_BCAST_QUEUED; /* Next time, broadcast. */
    }

    updateJobAwakeTime(job,0);
    queue *q = lookupQueue(job->queue,sdslen(job->queue));
    if (!q) q = createQueue(job->queue,sdslen(job->queue));
    RedisModule_Assert(skiplistInsert(q->sl,job) != NULL);
    q->atime = now_ms/1000;
    q->jobs_in++;
    if (!(q->flags & QUEUE_FLAG_PAUSED_OUT)) signalQueueAsReady(ctx,q);
    return C_OK;
}

/* Remove a job from the queue. Returns C_OK if the job was there and
 * is now removed (updating the job state back to ACTIVE), otherwise
 * C_ERR is returned. */
int dequeueJob(job *job) {
    if (job->state != JOB_STATE_QUEUED) return C_ERR;
    queue *q = lookupQueue(job->queue,sdslen(job->queue));
    if (!q) return C_ERR;
    RedisModule_Assert(skiplistDelete(q->sl,job));
    job->state = JOB_STATE_ACTIVE; /* Up to the caller to override this. */
    RedisModule_Log(NULL,"verbose","DE-QUEUED %.*s", JOB_ID_LEN, job->id);
    return C_OK;
}

/* Fetch a job from the specified queue if any, updating the job state
 * as it gets fetched back to ACTIVE. If there are no jobs pending in the
 * specified queue, NULL is returned.
 *
 * The returned job is, among the jobs available, the one with lower
 * 'ctime'.
 *
 * If 'qlen' is not NULL, the residual length of the queue is stored
 * at *qlen. */
job *queueFetchJob(RedisModuleCtx *ctx, queue *q, unsigned long *qlen) {
    if (skiplistLength(q->sl) == 0) return NULL;
    job *j = skiplistPopHead(q->sl);
    j->state = JOB_STATE_ACTIVE;
    j->flags |= JOB_FLAG_DELIVERED;
    q->atime = time(NULL);
    q->jobs_out++;
    if (qlen) *qlen = skiplistLength(q->sl);

    /* Jobs that have a retry set to 0 (at most once delivery semantics)
     * need to change state in the AOF as well: this way after a restart
     * we don't risk putting it into the queue again.
     *
     * Note that however when the AOF fsync policy is not strong enough,
     * after a crash the job may end in the queue again, so Disque offers
     * an option to load jobs in "active" state instead of "queued" state
     * for additional safety.
     *
     * Disque can also be configured to log all the dequeue operations in
     * order to provide a better crash-recovery experience (less duplicated
     * jobs on restart). */
    if ((ConfigPersistDequeued == DISQUE_PERSIST_DEQUEUED_ATMOSTONCE &&
        j->retry == 0) ||
        ConfigPersistDequeued == DISQUE_PERSIST_DEQUEUED_ALL)
    {
        AOFDequeueJob(ctx,j);
    }
    return j;
}

/* Return the length of the queue, or zero if NULL is passed here. */
unsigned long queueLength(queue *q) {
    if (!q) return 0;
    return skiplistLength(q->sl);
}

/* Queue length by queue name. The function returns 0 if the queue does
 * not exist. */
unsigned long queueNameLength(const char *qname, size_t qnamelen) {
    return queueLength(lookupQueue(qname,qnamelen));
}

/* Remove a queue that was not accessed for enough time, has no clients
 * blocked, has no jobs inside. If the queue is removed C_OK is
 * returned, otherwise C_ERR is returned. */
#define QUEUE_MAX_IDLE_TIME (60*5)
int GCQueue(queue *q, time_t max_idle_time) {
    time_t idle = time(NULL) - q->atime;
    if (idle < max_idle_time) return C_ERR;
    if (q->clients && listLength(q->clients) != 0) return C_ERR;
    if (skiplistLength(q->sl)) return C_ERR;
    if (q->flags & QUEUE_FLAG_PAUSED_ALL) return C_ERR;
    destroyQueue(q->name,sdslen(q->name));
    return C_OK;
}

/* This function is called from serverCron() in order to incrementally remove
 * from memory queues which are found to be idle and empty. */
int evictIdleQueues(RedisModuleCtx *ctx) {
    mstime_t start = mstime();
    time_t max_idle_time = QUEUE_MAX_IDLE_TIME;
    long sampled = 0, evicted = 0;

    if (getMemoryWarningLevel(ctx) > 0) max_idle_time /= 30;
    if (getMemoryWarningLevel(ctx) > 1) max_idle_time = 2;

    /* XXX: TODO: It is better to remember the last queue name scanned and
     * continue from there instead of using a random walk. */
    raxIterator ri;
    raxStart(&ri,Queues);
    while (raxSize(Queues) != 0) {
        raxSeek(&ri,"^",NULL,0);
        raxRandomWalk(&ri,0);
        queue *q = ri.data;

        sampled++;
        if (GCQueue(q,max_idle_time) == C_OK) evicted++;

        /* First exit condition: we are able to expire less than 10% of
         * entries. */
        if (sampled > 10 && (evicted * 10) < sampled) break;

        /* Second exit condition: we are looping for some time and maybe
         * we are using more than one or two milliseconds of time. */
        if (((sampled+1) % 1000) == 0 && mstime()-start > 1) break;
    }
    raxStop(&ri);
    return evicted;
}

/* -------------------------- Blocking on queues ---------------------------- */

/* GETJOB timeout reply. */
int getjobClientReply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    RedisModule_ReplyWithNull(ctx);
    RedisModuleBlockedClient *bc = RedisModule_GetBlockedClientHandle(ctx);
    cleanupClientBlockedForJobs(ctx,bc);
    return REDISMODULE_OK;
}

/* This is the structure we use as value in the dictionary of BlockedClients,
 * associating blocked client pointers to metadata about the client
 * blocked. */
typedef struct BlockedClientData {
    RedisModuleBlockedClient *bc;
    int flags;
    RedisModuleString **queues;
    int numqueues;
} BlockedClientData;

/* Create a valid BlockedClientData structure performing the needed
 * allocations. */
BlockedClientData *createBlockedClientData(RedisModuleCtx *ctx, RedisModuleBlockedClient *bc, RedisModuleString **queues, int numqueues, int flags)
{
    BlockedClientData *bcd = RedisModule_Alloc(sizeof(*bcd));
    bcd->bc = bc;
    bcd->flags = flags;
    bcd->numqueues = numqueues;
    bcd->queues = RedisModule_Alloc(sizeof(RedisModuleString*)*numqueues);
    for (int j = 0; j < numqueues; j++) {
        bcd->queues[j] = queues[j];
        RedisModule_RetainString(ctx,queues[j]);
    }
    return bcd;
}

/* Free the object returned by createBlockedClientData(). */
void freeBlockedClientData(RedisModuleCtx *ctx, BlockedClientData *bcd) {
    for (int j = 0; j < bcd->numqueues; j++)
        RedisModule_FreeString(ctx,bcd->queues[j]);
    RedisModule_Free(bcd->queues);
    RedisModule_Free(bcd);
}

/* Remove the client from all the queues it is blocking for.
 * Also remove the client from the module dictionary of blocked
 * clients, and free the associated data structures. */
void cleanupClientBlockedForJobs(RedisModuleCtx *ctx, RedisModuleBlockedClient *bc) {
    BlockedClientData *bcd = raxFind(BlockedClients,(unsigned char*)&bc,sizeof(bc));
    for (int j = 0; j < bcd->numqueues; j++) {
        size_t qnamelen;
        const char *qname = RedisModule_StringPtrLen(bcd->queues[j],&qnamelen);
        queue *q = lookupQueue(qname,qnamelen);
        RedisModule_Assert(q != NULL);

        listDelNode(q->clients,listSearchKey(q->clients,bc));
        if (listLength(q->clients) == 0) {
            listRelease(q->clients);
            q->clients = NULL;
            GCQueue(q,QUEUE_MAX_IDLE_TIME);
        }
    }
    freeBlockedClientData(ctx,bcd);
    raxRemove(BlockedClients,(unsigned char*)&bc,sizeof(bc),NULL);
}

/* Handle blocking if GETJOB found no jobs in the specified queues.
 *
 * 1) We set q->clients to the list of clients blocking for this queue
 *    (the value of the list items is a BlockedClientData structure).
 * 2) We set BlockedClients as well, as a dictionary of queues a client
 *    is blocked for. So we can resolve queues from clients. This is useful
 *    in order to clear the blocked client list when the client
 *    disconnects.
 * 3) When elements are added to queues with blocked clients, we call
 *    signalQueueAsReady(), that will send data to clients blocked in such
 *    queues.
 */
void blockForJobs(RedisModuleCtx *ctx, RedisModuleString **queues, int numqueues, mstime_t timeout, uint64_t flags) {

    RedisModuleBlockedClient *bc;
    BlockedClientData *bcd;

    bc = RedisModule_BlockClient(ctx,NULL,getjobClientReply,NULL,timeout);
    bcd = createBlockedClientData(ctx,bc,queues,numqueues,flags);
    RedisModule_SetDisconnectCallback(bc,cleanupClientBlockedForJobs);
    raxInsert(BlockedClients,(unsigned char*)&bc,sizeof(bc),bcd,NULL);

    /* Create the queues that do not exist yet, add the client to them.
     * XXX FIXME: to block to the same queue multiple times should not be
     * allowed. */
    for (int j = 0; j < numqueues; j++) {
        size_t qnamelen;
        const char *qname = RedisModule_StringPtrLen(queues[j],&qnamelen);
        queue *q = lookupQueue(qname,qnamelen);
        if (!q) q = createQueue(qname,qnamelen);

        /* Add this client to the list of clients in the queue. */
        if (q->clients == NULL) q->clients = listCreate();
        listAddNodeTail(q->clients,bc);
    }
}

/* Send data to the clients blocked on the specified queue. */
void handleClientsBlockedOnQueue(RedisModuleCtx *ctx, queue *q) {
    int numclients = listLength(q->clients);
    while(numclients--) {
        unsigned long qlen;
        listNode *ln = listFirst(q->clients);
        RedisModuleBlockedClient *bc = ln->value;
        job *j = queueFetchJob(ctx,q,&qlen);

        if (!j) return; /* There are no longer jobs in this queue. */
        if (qlen == 0) needJobsForQueue(ctx,q,NEEDJOBS_REACHED_ZERO);

        BlockedClientData *bcd = raxFind(BlockedClients,(unsigned char*)&bc,sizeof(bc));
        RedisModuleCtx *tsc = RedisModule_GetThreadSafeContext(bc);
        RedisModule_ReplyWithArray(tsc,1);
        addReplyJob(tsc,j,bcd->flags);
        RedisModule_FreeThreadSafeContext(tsc);

        RedisModule_UnblockClient(bc,NULL);
        cleanupClientBlockedForJobs(ctx,bc);
    }
}

/* Unblock clients waiting for a given queue if it received messages. */
void signalQueueAsReady(RedisModuleCtx *ctx, queue *q) {
    if (q->clients == NULL || listLength(q->clients) == 0) return;
    handleClientsBlockedOnQueue(ctx,q);
}

/* We need to scan blocked clients periodically in order to send GETJOB messages
 * around about this queue: other nodes may have messages for the clients
 * blocked here in this node. */
int clientsCronSendNeedJobs(RedisModuleCtx *ctx) {
    raxIterator ri;
    raxStart(&ri,BlockedClients);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        BlockedClientData *bcd = ri.data;
        for (int j = 0; j < bcd->numqueues; j++) {
            needJobsForQueueName(ctx,bcd->queues[j],NEEDJOBS_CLIENTS_WAITING);
        }
    }
    return 0;
}

/* ------------------------------ Federation -------------------------------- */

/* Return a very rough estimate of the current import message rate.
 * Imported messages are messaged received as NEEDJOBS replies. */
#define IMPORT_RATE_WINDOW 5000 /* 5 seconds max window. */
uint32_t getQueueImportRate(queue *q) {
    time_t now_ms = mstime();
    double elapsed = now_ms - q->prev_import_jobs_time;
    double messages = (double)q->prev_import_jobs_count +
                              q->current_import_jobs_count;

    /* If we did not received any message in the latest few seconds,
     * consider the import rate zero. */
    if ((now_ms - q->current_import_jobs_time) > IMPORT_RATE_WINDOW)
        return 0;

    /* Min interval is 50 ms in order to never overestimate. */
    if (elapsed < 50) elapsed = 50;

    return ceil((double)messages*1000/elapsed);
}

/* Called every time we import a job, this will update our counters
 * and state in order to update the import/sec estimate. */
void updateQueueImportRate(queue *q) {
    time_t now_ms = mstime();

    /* If the current second no longer matches the current counter
     * timestamp, copy the old timestamp/counter into 'prev', and
     * start a new counter with an updated time. */
    if (now_ms - q->current_import_jobs_time > 1000) {
        q->prev_import_jobs_time = q->current_import_jobs_time;
        q->prev_import_jobs_count = q->current_import_jobs_count;
        q->current_import_jobs_time = now_ms;
        q->current_import_jobs_count = 0;
    }
    /* Anyway, update the current counter. */
    q->current_import_jobs_count++;
}

/* Check the queue source nodes list (nodes that replied with jobs to our
 * NEEDJOBS request), purge the ones that timed out, and return the number
 * of sources which are still valid. */
unsigned long getQueueValidResponders(queue *q) {
    time_t now = time(NULL);
    if (q->needjobs_responders == NULL ||
        raxSize(q->needjobs_responders) == 0) return 0;

    raxIterator ri;
    raxStart(&ri,q->needjobs_responders);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        time_t lastmsg = (long)ri.data;
        if (now-lastmsg > 5) {
            raxRemove(q->needjobs_responders,ri.key,ri.key_len,NULL);
            raxSeek(&ri,">",ri.key,ri.key_len);
        }
    }
    raxStop(&ri);
    return raxSize(q->needjobs_responders);
}

/* This function is called every time we realize we need jobs for a given
 * queue, because we have clients blocked into this queue which are currently
 * empty.
 *
 * Calling this function may result into NEEDJOBS messages send to specific
 * nodes that are remembered as potential sources for messages in this queue,
 * or more rarely into NEEDJOBS messages to be broadcast cluster-wide in
 * order to discover new nodes that may be source of messages.
 *
 * This function in called in two different contests:
 *
 * 1) When a client attempts to fetch messages for an empty queue.
 * 2) From time to time for every queue we have clients blocked into.
 * 3) When a queue reaches 0 jobs since the last was fetched.
 *
 * When called in case 1 and 2, type is set to NEEDJOBS_CLIENTS_WAITING,
 * for case 3 instead type is set to NEEDJOBS_REACHED_ZERO, and in this
 * case the node may send a NEEDJOBS message to the set of known sources
 * for this queue, regardless of needjobs_adhoc_time value (that is, without
 * trying to throttle the requests, since there is an active flow of messages
 * between this node and source nodes).
 */

/* Min and max amount of jobs we request to other nodes. */
#define NEEDJOBS_MIN_REQUEST 5
#define NEEDJOBS_MAX_REQUEST 100
#define NEEDJOBS_BCAST_ALL_MIN_DELAY 2000     /* 2 seconds. */
#define NEEDJOBS_BCAST_ALL_MAX_DELAY 30000    /* 30 seconds. */
#define NEEDJOBS_BCAST_ADHOC_MIN_DELAY 25     /* 25 milliseconds. */
#define NEEDJOBS_BCAST_ADHOC_MAX_DELAY 2000   /* 2 seconds. */

void needJobsForQueue(RedisModuleCtx *ctx, queue *q, int type) {
    uint32_t import_per_sec; /* Jobs import rate in the latest secs. */
    uint32_t to_fetch;       /* Number of jobs we should try to obtain. */
    unsigned long num_responders = 0;
    mstime_t bcast_delay, adhoc_delay;
    mstime_t now = mstime();

    /* Don't ask for jobs if we are leaving the cluster. */
    if (myselfLeaving()) return;

    import_per_sec = getQueueImportRate(q);

    /* When called with NEEDJOBS_REACHED_ZERO, we have to do something only
     * if there is some active traffic, in order to improve latency.
     * Otherwise we wait for the first client to block, that will trigger
     * a new call to this function, but with NEEDJOBS_CLIENTS_WAITING type. */
    if (type == NEEDJOBS_REACHED_ZERO && import_per_sec == 0) return;

    /* Guess how many replies we need from each node. If we already have
     * a list of sources, assume that each source is capable of providing
     * some message. */
    num_responders = getQueueValidResponders(q);
    to_fetch = NEEDJOBS_MIN_REQUEST;
    if (num_responders > 0)
        to_fetch = import_per_sec / num_responders;

    /* Trim number of jobs to request to min/max values. */
    if (to_fetch < NEEDJOBS_MIN_REQUEST) to_fetch = NEEDJOBS_MIN_REQUEST;
    else if (to_fetch > NEEDJOBS_MAX_REQUEST) to_fetch = NEEDJOBS_MAX_REQUEST;

    /* Broadcast the message cluster from time to time.
     * We use exponential intervals (with a max time limit) */
    bcast_delay = NEEDJOBS_BCAST_ALL_MIN_DELAY *
                  (1 << q->needjobs_bcast_attempt);
    if (bcast_delay > NEEDJOBS_BCAST_ALL_MAX_DELAY)
        bcast_delay = NEEDJOBS_BCAST_ALL_MAX_DELAY;

    if (now - q->needjobs_bcast_time > bcast_delay) {
        q->needjobs_bcast_time = now;
        q->needjobs_bcast_attempt++;
        /* Cluster-wide broadcasts are just to discover nodes,
         * ask for a single job in this case. */
        clusterSendNeedJobs(ctx,q->name,1,NULL);
    }

    /* If the queue reached zero, or if the delay elapsed and we
     * have at least a source node, send an ad-hoc message to
     * nodes known to be sources for this queue.
     *
     * We use exponential delays here as well (but don't care about
     * the delay if the queue just dropped to zero), however with
     * much shorter times compared to the cluster-wide broadcast. */
    adhoc_delay = NEEDJOBS_BCAST_ADHOC_MIN_DELAY *
                  (1 << q->needjobs_adhoc_attempt);
    if (adhoc_delay > NEEDJOBS_BCAST_ADHOC_MAX_DELAY)
        adhoc_delay = NEEDJOBS_BCAST_ADHOC_MAX_DELAY;

    if ((type == NEEDJOBS_REACHED_ZERO ||
         now - q->needjobs_adhoc_time > adhoc_delay) &&
         num_responders > 0)
    {
        q->needjobs_adhoc_time = now;
        q->needjobs_adhoc_attempt++;
        clusterSendNeedJobs(ctx,q->name,to_fetch,q->needjobs_responders);
    }
}

/* needJobsForQueue() wrapper taking a queue name instead of a queue
 * structure. The queue will be created automatically if non existing. */
void needJobsForQueueName(RedisModuleCtx *ctx, RedisModuleString *qname, int type) {
    size_t namelen;
    const char *name = RedisModule_StringPtrLen(qname,&namelen);
    queue *q = lookupQueue(name,namelen);

    /* Create the queue if it does not exist. We need the queue structure
     * to store meta-data needed to broadcast NEEDJOBS messages anyway. */
    if (!q) q = createQueue(name,namelen);
    needJobsForQueue(ctx,q,type);
}

/* Called from cluster.c when a YOURJOBS message is received. */
void receiveYourJobs(RedisModuleCtx *ctx, const char *node, uint32_t numjobs, unsigned char *serializedjobs, uint32_t serializedlen) {
    queue *q;
    uint32_t j;
    unsigned char *nextjob = serializedjobs;

    for (j = 0; j < numjobs; j++) {
        uint32_t remlen = serializedlen - (nextjob-serializedjobs);
        job *job, *sj = deserializeJob(ctx,nextjob,remlen,&nextjob,SER_MESSAGE);

        if (sj == NULL) {
            RedisModule_Log(ctx,"warning",
                "The %d-th job received via YOURJOBS from %.40s is corrupted.",
                (int)j+1, node);
            return;
        }

        /* If the job does not exist, we need to add it to our jobs.
         * Otherwise just get a reference to the job we already have
         * in memory and free the deserialized one. */
        job = lookupJob(sj->id);
        if (job) {
            freeJob(sj);
        } else {
            job = sj;
            job->state = JOB_STATE_ACTIVE;
            registerJob(job);
        }
        /* Don't need to send QUEUED when adding this job into the queue,
         * we are just moving from the queue of one node to another. */
        job->flags &= ~JOB_FLAG_BCAST_QUEUED;

        /* If we are receiving a job with retry set to 0, let's set
         * job->qtime to non-zero, to force enqueueJob() to queue the job
         * the first time. As a side effect the function will set the qtime
         * value to 0, preventing a successive enqueue of the job */
        if (job->retry == 0)
            job->qtime = mstime(); /* Any value will do. */

        if (enqueueJob(ctx,job,0) == C_ERR) continue;

        /* Update queue stats needed to optimize nodes federation. */
        q = lookupQueue(job->queue,sdslen(job->queue));
        if (!q) q = createQueue(job->queue,sdslen(job->queue));
        if (q->needjobs_responders == NULL)
            q->needjobs_responders = raxNew();

        if (raxInsert(q->needjobs_responders,
                      (unsigned char*)node, REDISMODULE_NODE_ID_LEN,
                      (void*)(long)time(NULL), NULL))
        {
            /* That's a new node! We reset the broadcast attempt counter, that
             * will model the delay to wait before every cluster-wide
             * broadcast, every time we receive jobs from a node not already
             * known as a source. */
            q->needjobs_bcast_attempt = 0;
        }

        updateQueueImportRate(q);
        q->needjobs_adhoc_attempt = 0;
    }
}

/* Called from cluster.c when a NEEDJOBS message is received. */
void receiveNeedJobs(RedisModuleCtx *ctx, const char *node, const char *qname, size_t qnamelen, uint32_t count) {
    queue *q = lookupQueue(qname,qnamelen);
    unsigned long qlen = queueLength(q);
    uint32_t replyjobs = count; /* Number of jobs we are willing to provide. */
    uint32_t j;

    /* Ignore requests for jobs if:
     * 1) No such queue here, or queue is empty.
     * 2) We are actively importing jobs ourselves for this queue. */
    if (qlen == 0 || getQueueImportRate(q) > 0) return;

    /* Ignore request if queue is paused in output. */
    if (q->flags & QUEUE_FLAG_PAUSED_OUT) return;

    /* To avoid that a single node is able to deplete our queue easily,
     * we provide the number of jobs requested only if we have more than
     * 2 times what it requested. Otherwise we provide at max half the jobs
     * we have, but always at least a single job. */
    if (qlen < count*2) replyjobs = qlen/2;
    if (replyjobs == 0) replyjobs = 1;

    job *jobs[NEEDJOBS_MAX_REQUEST];
    for (j = 0; j < replyjobs; j++) {
        jobs[j] = queueFetchJob(ctx,q,NULL);
        RedisModule_Assert(jobs[j] != NULL);
    }
    clusterSendYourJobs(ctx,node,jobs,replyjobs);

    /* It's possible that we sent jobs with retry=0. Remove them from
     * the local node since to take duplicates does not make sense for
     * jobs having the replication level of 1 by contract. */
    for (j = 0; j < replyjobs; j++) {
        job *job = jobs[j];
        if (job->retry == 0) {
            unregisterJob(ctx,job);
            freeJob(job);
        }
    }
}

/* ------------------------------ Queue pausing -----------------------------
 *
 * There is very little here since pausing a queue is basically just changing
 * its flags. Then what changing the PAUSE flags means, is up to the different
 * parts of Disque implementing the behavior of queues. */

/* Changes the paused state of the queue and handles serving again blocked
 * clients if needed.
 *
 * 'flag' must be QUEUE_FLAG_PAUSED_IN or QUEUE_FLAG_PAUSED_OUT
 * 'set' is true if we have to set this state or 0 if we have
 * to clear this state. */
void queueChangePausedState(RedisModuleCtx *ctx, queue *q, int flag, int set) {
    uint32_t orig_flags = q->flags;

    if (set) q->flags |= flag;
    else     q->flags &= ~flag;

    if ((orig_flags & QUEUE_FLAG_PAUSED_OUT) &&
        !(q->flags & QUEUE_FLAG_PAUSED_OUT))
    {
        signalQueueAsReady(ctx,q);
    }
}

/* Called from cluster.c when a PAUSE message is received. */
void receivePauseQueue(RedisModuleCtx *ctx, const char *qname, size_t qnamelen, uint32_t flags) {
    queue *q = lookupQueue(qname,qnamelen);

    /* If the queue does not exist, and flags are cleared, there is nothing
     * to do. Otherwise we have to create the queue. */
    if (!q) {
        if (flags == 0) return;
        q = createQueue(qname,qnamelen);
    }

    /* Replicate the sender pause flag in our queue. */
    queueChangePausedState(ctx,q,QUEUE_FLAG_PAUSED_IN,
        (flags & QUEUE_FLAG_PAUSED_IN) != 0);
    queueChangePausedState(ctx,q,QUEUE_FLAG_PAUSED_OUT,
        (flags & QUEUE_FLAG_PAUSED_OUT) != 0);
}

/* Return the string "in", "out", "all" or "none" depending on the paused
 * state of the specified queue flags. */
char *queueGetPausedStateString(uint32_t qflags) {
    qflags &= QUEUE_FLAG_PAUSED_ALL;
    if (qflags == QUEUE_FLAG_PAUSED_ALL) {
        return "all";
    } else if (qflags == QUEUE_FLAG_PAUSED_IN) {
        return "in";
    } else if (qflags == QUEUE_FLAG_PAUSED_OUT) {
        return "out";
    } else {
        return "none";
    }
}

/* This function completely deletes all the data in Disque: jobs and queues.
 * It has no effects in the persistence, so it's up to the caller to
 * replicate the command to flush all the data if needed. */
void flushAllJobsAndQueues(RedisModuleCtx *ctx) {
    /* Free all the jobs. as a side effect this should also unblock
     * all the clients blocked in jobs that are being replicated. */
    raxIterator ri;
    raxStart(&ri,Jobs);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        job *job = ri.data;
        unregisterJob(ctx,job);
        freeJob(job);
        raxSeek(&ri,">",ri.key,ri.key_len);
    }
    raxStop(&ri);

    /* Unblock all the clients blocked on queues: there will be no
     * queues soon. */
    raxStart(&ri,BlockedClients);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        RedisModuleBlockedClient *bc;
        memcpy(&bc,ri.key,sizeof(bc));
        RedisModuleCtx *tsc = RedisModule_GetThreadSafeContext(bc);
        RedisModule_ReplyWithNull(tsc);
        RedisModule_FreeThreadSafeContext(tsc);
        cleanupClientBlockedForJobs(ctx,bc);
        raxSeek(&ri,">",ri.key,ri.key_len);
    }
    raxStop(&ri);

    /* Destroy all the queues. */
    raxStart(&ri,Queues);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        destroyQueue((char*)ri.key,ri.key_len);
        raxSeek(&ri,">",ri.key,ri.key_len);
    }
    raxStop(&ri);
}

/* ------------------------- Queue related commands ------------------------- */

/* QLEN <qname> -- Return the number of jobs queued. */
int qlenCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    size_t qnamelen;
    const char *qname = RedisModule_StringPtrLen(argv[1],&qnamelen);
    RedisModule_ReplyWithLongLong(ctx,queueNameLength(qname,qnamelen));
    return REDISMODULE_OK;
}

/* GETJOB [NOHANG] [TIMEOUT <ms>] [COUNT <count>] FROM <qname1>
 *        <qname2> ... <qnameN>.
 *
 * Get jobs from the specified queues. By default COUNT is 1, so just one
 * job will be returned. If there are no jobs in any of the specified queues
 * the command will block.
 *
 * When there are jobs in more than one of the queues, the command guarantees
 * to return jobs in the order the queues are specified. If COUNT allows
 * more jobs to be returned, queues are scanned again and again in the same
 * order popping more elements. */
int getjobCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 2) return RedisModule_WrongArity(ctx);

    mstime_t timeout = 0; /* Block forever by default. */
    long long count = 1, emitted_jobs = 0;
    int nohang = 0; /* Don't block even if all the queues are empty. */
    int withcounters = 0; /* Also return NACKs and deliveries counters. */
    RedisModuleString **queues = NULL;
    int j, numqueues = 0;

    /* Parse args. */
    for (j = 1; j < argc; j++) {
        const char *opt = RedisModule_StringPtrLen(argv[j],NULL);
        int lastarg = j == argc-1;
        if (!strcasecmp(opt,"nohang")) {
            nohang = 1;
        } else if (!strcasecmp(opt,"withcounters")) {
            withcounters = 1;
        } else if (!strcasecmp(opt,"timeout") && !lastarg) {
            if (getTimeoutFromObjectOrReply(ctx,argv[j+1],&timeout,
                UNIT_MILLISECONDS) != C_OK) return REDISMODULE_OK;
            j++;
        } else if (!strcasecmp(opt,"count") && !lastarg) {
            int retval = RedisModule_StringToLongLong(argv[j+1],&count);
            if (retval != REDISMODULE_OK || count <= 0) {
                RedisModule_ReplyWithError(ctx,
                    "ERR COUNT must be a number greater than zero");
                return REDISMODULE_OK;
            }
            j++;
        } else if (!strcasecmp(opt,"from")) {
            queues = argv+j+1;
            numqueues = argc - j - 1;
            break; /* Don't process options after this. */
        } else {
            return RedisModule_ReplyWithError(ctx,
                "ERR Unrecognized option given");
        }
    }

    /* FROM is mandatory. */
    if (queues == NULL || numqueues == 0) {
        return RedisModule_ReplyWithError(ctx,"ERR FROM is mandatory");
    }

    /* First: try to avoid blocking if there is at least one job in at
     * least one queue. */

    while(1) {
        long old_emitted = emitted_jobs;
        for (j = 0; j < numqueues; j++) {
            unsigned long qlen;
            size_t qnamelen;
            const char *qname = RedisModule_StringPtrLen(queues[j],&qnamelen);
            queue *q = lookupQueue(qname,qnamelen);
            job *job = NULL;

            if (q && !(q->flags & QUEUE_FLAG_PAUSED_OUT))
                job = queueFetchJob(ctx,q,&qlen);

            if (!job) {
                if (!q)
                    needJobsForQueueName(ctx,queues[j],NEEDJOBS_CLIENTS_WAITING);
                else
                    needJobsForQueue(ctx,q,NEEDJOBS_CLIENTS_WAITING);
                continue;
            } else if (job && qlen == 0) {
                needJobsForQueue(ctx,q,NEEDJOBS_REACHED_ZERO);
            }
            if (emitted_jobs == 0)
                RedisModule_ReplyWithArray(ctx,REDISMODULE_POSTPONED_ARRAY_LEN);

            addReplyJob(ctx,job,withcounters ? GETJOB_FLAG_WITHCOUNTERS :
                                             GETJOB_FLAG_NONE);
            count--;
            emitted_jobs++;
            if (count == 0) break;
        }
        /* When we reached count or when we are no longer making
         * progresses (no jobs left in our queues), stop. */
        if (count == 0 || old_emitted == emitted_jobs) break;
    }

    /* Set the array length and return if we emitted jobs. */
    if (emitted_jobs) {
        RedisModule_ReplySetArrayLength(ctx,emitted_jobs);
        return REDISMODULE_OK;
    }

    /* If NOHANG was given and there are no jobs, return NULL. */
    if (nohang)
        return RedisModule_ReplyWithNull(ctx);

    /* If this node is leaving the cluster, we can't block waiting for
     * jobs: this would trigger the federation with other nodes in order
     * to import jobs here. Just return a -LEAVING error. */
    if (myselfLeaving())
        return RedisModule_ReplyWithError(ctx,
            "LEAVING this node is leaving the cluster. "
            "Try another one please.");

    /* If we reached this point, we need to block. */
    blockForJobs(ctx,queues,numqueues,timeout,
            withcounters ? GETJOB_FLAG_WITHCOUNTERS : GETJOB_FLAG_NONE);
    return REDISMODULE_OK;
}

/* ENQUEUE job-id-1 job-id-2 ... job-id-N
 * NACK job-id-1 job-id-2 ... job-id-N
 *
 * If the job is active, queue it if job retry != 0.
 * If the job is in any other state, do nothing.
 * If the job is not known, do nothing.
 *
 * NOTE: Even jobs with retry set to 0 are enqueued! Be aware that
 * using this command may violate the at-most-once contract.
 *
 * Return the number of jobs actually move from active to queued state.
 *
 * The difference between ENQUEUE and NACK is that the latter will propagate
 * cluster messages in a way that makes the nacks counter in the receiver
 * to increment. */
int enqueueGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int nack) {
    int j, enqueued = 0;

    if (validateJobIDs(ctx,argv+1,argc-1) == C_ERR)
        return REDISMODULE_OK;

    /* Enqueue all the jobs in active state. */
    for (j = 1; j < argc; j++) {
        const char *jobid = RedisModule_StringPtrLen(argv[j],NULL);
        job *job = lookupJob(jobid);
        if (job == NULL) continue;

        if (job->state == JOB_STATE_ACTIVE && enqueueJob(ctx,job,nack) == C_OK)
            enqueued++;
    }
    return RedisModule_ReplyWithLongLong(ctx,enqueued);
}

/* See enqueueGenericCommand(). */
int enqueueCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return enqueueGenericCommand(ctx,argv,argc,0);
}

/* See enqueueGenericCommand(). */
int nackCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return enqueueGenericCommand(ctx,argv,argc,1);
}

/* DEQUEUE job-id-1 job-id-2 ... job-id-N
 *
 * If the job is queued, remove it from queue and change state to active.
 * If the job is in any other state, do nothing.
 * If the job is not known, do nothing.
 *
 * Return the number of jobs actually moved from queue to active state. */
int dequeueCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    int j, dequeued = 0;

    if (validateJobIDs(ctx,argv+1,argc-1) == C_ERR) return REDISMODULE_OK;

    /* Enqueue all the jobs in active state. */
    for (j = 1; j < argc; j++) {
        const char *jobid = RedisModule_StringPtrLen(argv[j],NULL);
        job *job = lookupJob(jobid);
        if (job == NULL) continue;

        if (job->state == JOB_STATE_QUEUED && dequeueJob(job) == C_OK)
            dequeued++;
    }
    return RedisModule_ReplyWithLongLong(ctx,dequeued);
}

/* QPEEK <queue> <count>
 *
 * Return an array of at most "count" jobs available inside the queue "queue"
 * without removing the jobs from the queue. This is basically an introspection
 * and debugging command.
 *
 * Normally jobs are returned from the oldest to the newest (according to the
 * job creation time field), however if "count" is negative , jobs are
 * returned from newset to oldest instead.
 *
 * Each job is returned as a two elements array with the Job ID and body. */
int qpeekCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) return RedisModule_WrongArity(ctx);

    int newjobs = 0; /* Return from newest to oldest if true. */
    long long count, returned = 0;

    if (RedisModule_StringToLongLong(argv[2],&count) == REDISMODULE_ERR)
        return RedisModule_ReplyWithError(ctx,"ERR Invalid count");

    if (count < 0) {
        count = -count;
        newjobs = 1;
    }

    skiplistNode *sn = NULL;
    size_t qnamelen;
    const char *qname = RedisModule_StringPtrLen(argv[1],&qnamelen);
    queue *q = lookupQueue(qname,qnamelen);

    if (q != NULL)
        sn = newjobs ? q->sl->tail : q->sl->header->level[0].forward;

    if (sn == NULL) return RedisModule_ReplyWithEmptyArray(ctx);

    RedisModule_ReplyWithArray(ctx,REDISMODULE_POSTPONED_ARRAY_LEN);
    while(count-- && sn) {
        job *j = sn->obj;
        addReplyJob(ctx, j, GETJOB_FLAG_NONE);
        returned++;
        if (newjobs)
            sn = sn->backward;
        else
            sn = sn->level[0].forward;
    }
    RedisModule_ReplySetArrayLength(ctx,returned);
    return REDISMODULE_OK;
}

/* WORKING job-id
 *
 * If the job is queued, remove it from queue and change state to active.
 * Postpone the job requeue time in the future so that we'll wait the retry
 * time before enqueueing again.
 *
 * Also, as a side effect of calling this command, a WORKING message gets
 * broadcast to all the other nodes that have a copy according to our local
 * job information.
 *
 * Return how much time the worker likely have before the next requeue event
 * or an error:
 *
 * -ACKED     The job is already acknowledged, so was processed already.
 * -NOJOB     We don't know about this job. The job was either already
 *            acknowledged and purged, or this node never received a copy.
 * -TOOLATE   50% of the job TTL already elapsed, is no longer possible to
 *            delay it.
 */
int workingCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);

    if (validateJobIDs(ctx,argv+1,1) == C_ERR) return REDISMODULE_OK;

    const char *jobid = RedisModule_StringPtrLen(argv[1],NULL);
    job *job = lookupJob(jobid);
    if (job == NULL)
        return RedisModule_ReplyWithError(ctx,
            "NOJOB Job not known in the context of this node.\r\n");

    /* Don't allow to postpone jobs that have less than 50% of time to live
     * left, in order to prevent a worker from monopolizing a job for all its
     * lifetime. */
    mstime_t ttl = ((mstime_t)job->etime*1000) - (job->ctime/1000000);
    mstime_t elapsed = mstime() - (job->ctime/1000000);
    if (ttl > 0 && elapsed > ttl/2)
        return RedisModule_ReplyWithError(ctx,
            "TOOLATE Half of job TTL already elapsed, "
            "you are no longer allowed to postpone the "
            "next delivery.\r\n");

    if (job->state == JOB_STATE_QUEUED) dequeueJob(job);
    job->flags |= JOB_FLAG_BCAST_WILLQUEUE;
    updateJobRequeueTime(job,mstime()+
                         job->retry*1000+
                         randomTimeError(DISQUE_TIME_ERR));
    clusterBroadcastWorking(ctx,job);
    return RedisModule_ReplyWithLongLong(ctx,job->retry);
}

/* QSTAT queue-name
 *
 * Returns statistics and information about the specified queue. */
int qstatCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);

    size_t qnamelen;
    const char *qname = RedisModule_StringPtrLen(argv[1],&qnamelen);
    queue *q = lookupQueue(qname,qnamelen);
    if (!q) return RedisModule_ReplyWithNull(ctx);

    time_t idle = time(NULL) - q->atime;
    time_t age = time(NULL) - q->ctime;
    if (idle < 0) idle = 0;
    if (age < 0) age = 0;

    RedisModule_ReplyWithArray(ctx,20);

    RedisModule_ReplyWithSimpleString(ctx,"name");
    RedisModule_ReplyWithStringBuffer(ctx,q->name,sdslen(q->name));

    RedisModule_ReplyWithSimpleString(ctx,"len");
    RedisModule_ReplyWithLongLong(ctx,queueLength(q));

    RedisModule_ReplyWithSimpleString(ctx,"age");
    RedisModule_ReplyWithLongLong(ctx,age);

    RedisModule_ReplyWithSimpleString(ctx,"idle");
    RedisModule_ReplyWithLongLong(ctx,idle);

    RedisModule_ReplyWithSimpleString(ctx,"blocked");
    RedisModule_ReplyWithLongLong(ctx,(q->clients == NULL) ? 0 : listLength(q->clients));

    RedisModule_ReplyWithSimpleString(ctx,"import-from");
    if (q->needjobs_responders) {
        RedisModule_ReplyWithArray(ctx,raxSize(q->needjobs_responders));
        raxIterator ri;
        raxStart(&ri,q->needjobs_responders);
        raxSeek(&ri,"^",NULL,0);
        while(raxNext(&ri))
            RedisModule_ReplyWithStringBuffer(ctx,(char*)ri.key,ri.key_len);
        raxStop(&ri);
    } else {
        RedisModule_ReplyWithEmptyArray(ctx);
    }

    RedisModule_ReplyWithSimpleString(ctx,"import-rate");
    RedisModule_ReplyWithLongLong(ctx,getQueueImportRate(q));

    RedisModule_ReplyWithSimpleString(ctx,"jobs-in");
    RedisModule_ReplyWithLongLong(ctx,q->jobs_in);

    RedisModule_ReplyWithSimpleString(ctx,"jobs-out");
    RedisModule_ReplyWithLongLong(ctx,q->jobs_out);

    RedisModule_ReplyWithSimpleString(ctx,"pause");
    RedisModule_ReplyWithSimpleString(ctx,queueGetPausedStateString(q->flags));

    return REDISMODULE_OK;
}

/* PAUSE queue option1 [option2 ... optionN]
 *
 * Change queue paused state. */
int pauseCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3) return RedisModule_WrongArity(ctx);

    int j, bcast = 0, update = 0;
    uint32_t old_flags = 0, new_flags = 0;

    size_t qnamelen;
    const char *qname = RedisModule_StringPtrLen(argv[1],&qnamelen);
    queue *q = lookupQueue(qname,qnamelen);
    if (q) old_flags = q->flags;

    for (j = 2; j < argc; j++) {
        const char *opt = RedisModule_StringPtrLen(argv[j],NULL);
        if (!strcasecmp(opt,"none")) {
            new_flags = 0;
            update = 1;
        } else if (!strcasecmp(opt,"in")) {
            new_flags |= QUEUE_FLAG_PAUSED_IN; update = 1;
        } else if (!strcasecmp(opt,"out")) {
            new_flags |= QUEUE_FLAG_PAUSED_OUT; update = 1;
        } else if (!strcasecmp(opt,"all")) {
            new_flags |= QUEUE_FLAG_PAUSED_ALL; update = 1;
        } else if (!strcasecmp(opt,"state")) {
            /* Nothing to do, we reply with the state regardless. */
        } else if (!strcasecmp(opt,"bcast")) {
            bcast = 1;
        } else {
            return RedisModule_ReplyWithError(ctx,"ERR syntax error");
        }
    }

    /* Update the queue pause state, if needed. */
    if (!q && update && old_flags != new_flags)
        q = createQueue(qname,qnamelen);
    if (q && update) {
        queueChangePausedState(ctx,q,QUEUE_FLAG_PAUSED_IN,
            (new_flags & QUEUE_FLAG_PAUSED_IN) != 0);
        queueChangePausedState(ctx,q,QUEUE_FLAG_PAUSED_OUT,
            (new_flags & QUEUE_FLAG_PAUSED_OUT) != 0);
    }

    /* Get the queue flags after the operation. */
    new_flags = q ? q->flags : 0;
    new_flags &= QUEUE_FLAG_PAUSED_ALL;

    /* Broadcast a PAUSE command if the user specified BCAST. */
    if (bcast) clusterBroadcastPause(ctx,qname,qnamelen,new_flags);

    /* Always reply with the current queue state. */
    return RedisModule_ReplyWithSimpleString(ctx,
                queueGetPausedStateString(new_flags));
}
