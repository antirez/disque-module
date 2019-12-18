/* Jobs handling and commands.
 *
 * Copyright (c) 2014-2019, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved. This code is under the AGPL license, check the
 * LICENSE file for more info. */

#include "disque.h"

/* ------------------------- Low level jobs functions ----------------------- */

/* Generate a new Job ID and writes it to the string pointed by 'id'
 * (NOT including a null term), that must be JOB_ID_LEN or more.
 *
 * An ID is 40 bytes string composed as such:
 *
 * +--+-----------------+-+--------------------- --------+-+-----+
 * |D-| 8 bytes Node ID |-| 144-bit ID (base64: 24 bytes)|-| TTL |
 * +--+-----------------+-+------------------------------+-+-----+
 *
 * "D-" is just a fixed string. All Disque job IDs start with this
 * two bytes.
 *
 * Node ID is the first 8 bytes of the hexadecimal Node ID where the
 * message was created. The main use for this is that a consumer receiving
 * messages from a given queue can collect stats about where the producers
 * are connected, and switch to improve the cluster efficiency.
 *
 * The 144 bit ID is the unique message ID, encoded in base 64 with
 * the standard charset "A-Za-z0-9+/".
 *
 * The TTL is a big endian 16 bit unsigned number ceiled to 2^16-1
 * if greater than that, and is only used in order to expire ACKs
 * when the job is no longer available. It represents the TTL of the
 * original job in *minutes*, not seconds, and is encoded in as a
 * 4 digits hexadecimal number.
 *
 * The TTL is even if the job retry value is 0 (at most once jobs),
 * otherwise is odd, so the actual precision of the value is 2 minutes.
 * This is useful since the receiver of an ACKJOB command can avoid
 * creating a "dummy ack" for unknown job IDs for at most once jobs.
 */
void generateJobID(char *id, int ttl, int retry) {
    char *b64cset = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                    "abcdefghijklmnopqrstuvwxyz"
                    "0123456789+/";
    char *hexcset = "0123456789abcdef";
    SHA1_CTX ctx;
    unsigned char ttlbytes[2], hash[20];
    int j;
    static uint64_t counter;

    /* Get the pseudo random bytes using SHA1 in counter mode. */
    counter++;
    SHA1Init(&ctx);
    SHA1Update(&ctx,(unsigned char*)JobIDSeed,sizeof(JobIDSeed));
    SHA1Update(&ctx,(unsigned char*)&counter,sizeof(counter));
    SHA1Final(hash,&ctx);

    ttl /= 60; /* Store TTL in minutes. */
    if (ttl > 65535) ttl = 65535;
    if (ttl < 0) ttl = 1;

    /* Force the TTL to be odd if retry > 0, even if retry == 0. */
    ttl = (retry > 0) ? (ttl|1) : (ttl & ~1);

    ttlbytes[0] = (ttl&0xff00)>>8;
    ttlbytes[1] = ttl&0xff;

    *id++ = 'D';
    *id++ = '-';

    /* 8 bytes from Node ID + separator */
    const char *myself = RedisModule_GetMyClusterID();
    for (j = 0; j < 8; j++) *id++ = myself[j];
    *id++ = '-';

    /* Pseudorandom Message ID + separator. We encode 4 base64 chars
     * per loop (3 digest bytes), and each char encodes 6 bits, so we have
     * to loop 6 times to encode all the 144 bits into 24 destination chars. */
    unsigned char *h = hash;
    for (j = 0; j < 6; j++) {
        id[0] = b64cset[h[0]>>2];
        id[1] = b64cset[(h[0]<<4|h[1]>>4)&63];
        id[2] = b64cset[(h[1]<<2|h[2]>>6)&63];
        id[3] = b64cset[h[3]&63];
        id += 4;
        h += 3;
    }
    *id++ = '-';

    /* 4 TTL bytes in hex. */
    id[0] = hexcset[(ttlbytes[0]&0xf0)>>4];
    id[1] = hexcset[ttlbytes[0]&0xf];
    id[2] = hexcset[(ttlbytes[1]&0xf0)>>4];
    id[3] = hexcset[ttlbytes[1]&0xf];
    id += 4;
}

/* Helper function for setJobTTLFromID() in order to extract the TTL stored
 * as hex big endian number in the Job ID. The function is only used for this
 * but is more generic. 'p' points to the first digit for 'count' hex digits.
 * The number is assumed to be stored in big endian format. For each byte
 * the first hex char is the most significant. If invalid digits are found
 * considered to be zero, however errno is set to EINVAL if this happens. */
uint64_t hexToInt(const char *p, size_t count) {
    uint64_t value = 0;
    char *charset = "0123456789abcdef";

    errno = 0;
    while(count--) {
        int c = tolower(*p++);
        char *pos = strchr(charset,c);
        int v;
        if (!pos) {
            errno = EINVAL;
            v = 0;
        } else {
            v = pos-charset;
        }
        value = (value << 4) | v;
    }
    return value;
}

/* Disque aims to avoid to deliver duplicated message whenever possible, so
 * it is always desirable that a given message is not queued by multiple owners
 * at the same time. This cannot be guaranteed because of partitions, but one
 * of the best-effort things we do is that, when a QUEUED message is received
 * by a node about a job, the node IDs of the sender and the receiver are
 * compared: If the sender has a greater node ID, we drop the message from our
 * queue (but retain a copy of the message to retry again later).
 *
 * However comparing nodes just by node ID means that a given node is always
 * greater than the other. So before comparing the node IDs, we mix the IDs
 * with the pseudorandom part of the Job ID, using the XOR function. This way
 * the comparison depends on the job. */
int compareNodeIDsByJob(const char *nodea, const char *nodeb, job *j) {
    int i;
    char ida[REDISMODULE_NODE_ID_LEN], idb[REDISMODULE_NODE_ID_LEN];
    memcpy(ida,nodea,REDISMODULE_NODE_ID_LEN);
    memcpy(idb,nodeb,REDISMODULE_NODE_ID_LEN);
    for (i = 0; i < REDISMODULE_NODE_ID_LEN; i++) {
        /* The Job ID has 24 bytes of pseudo random bits starting at
         * offset 11. */
        ida[i] ^= j->id[11 + i%24];
        idb[i] ^= j->id[11 + i%24];
    }
    return memcmp(ida,idb,REDISMODULE_NODE_ID_LEN);
}

/* Return the raw TTL (in minutes) from a well-formed Job ID.
 * The caller should do sanity check on the job ID before calling this
 * function. Note that the 'id' field of a a job structure is always valid. */
int getRawTTLFromJobID(const char *id) {
    return hexToInt(id+36,4);
}

/* Set the job ttl from the encoded ttl in its ID. This is useful when we
 * create a new job just to store the fact it's acknowledged. Thanks to
 * the TTL encoded in the ID we are able to set the expire time for the job
 * regardless of the fact we have no info about the job. */
void setJobTTLFromID(job *job) {
    int expire_minutes = getRawTTLFromJobID(job->id);
    /* Convert back to absolute unix time. */
    job->etime = time(NULL) + expire_minutes*60;
}

/* Validate the string 'id' as a job ID. 'len' is the number of bytes the
 * string is composed of. The function just checks length and prefix/suffix.
 * It's pretty pointless to use more CPU to validate it better since anyway
 * the lookup will fail. */
int validateJobID(const char *id, size_t len) {
    if (len != JOB_ID_LEN) return C_ERR;
    if (id[0] != 'D' ||
        id[1] != '-' ||
        id[10] != '-' ||
        id[35] != '-') return C_ERR;
    return C_OK;
}

/* Like validateJobID() but if the ID is invalid an error message is sent
 * to the client 'c' if not NULL. */
int validateJobIdOrReply(RedisModuleCtx *ctx, const char *id, size_t len) {
    int retval = validateJobID(id,len);
    if (retval == C_ERR && ctx)
        RedisModule_ReplyWithError(ctx,"BADID Invalid Job ID format");
    return retval;
}

/* Create a new job in a given state. If 'id' is NULL, a new ID will be
 * created as assigned, otherwise the specified ID is used.
 * The 'ttl' and 'retry' arguments are only used if 'id' is not NULL.
 *
 * This function only creates the job without any body, the only populated
 * fields are the ID and the state. */
job *createJob(const char *id, int state, int ttl, int retry) {
    job *j = RedisModule_Alloc(sizeof(job));

    /* Generate a new Job ID if not specified by the caller. */
    if (id == NULL)
        generateJobID(j->id,ttl,retry);
    else
        memcpy(j->id,id,JOB_ID_LEN);

    j->queue = NULL;
    j->state = state;
    j->gc_retry = 0;
    j->flags = 0;
    j->body = NULL;
    j->nodes_delivered = raxNew();
    j->nodes_confirmed = NULL; /* Only created later on-demand. */
    j->awakeme = 0; /* Not yet registered in awakeme skiplist. */
    /* Number of NACKs and additional deliveries start at zero and
     * are incremented as QUEUED messages are received or sent. */
    j->num_nacks = 0;
    j->num_deliv = 0;
    j->bc = NULL;
    return j;
}

/* Free a job. Does not automatically unregister it. */
void freeJob(job *j) {
    if (j == NULL) return;
    if (j->queue) sdsfree(j->queue);
    if (j->body) sdsfree(j->body);
    if (j->nodes_delivered) raxFree(j->nodes_delivered);
    if (j->nodes_confirmed) raxFree(j->nodes_confirmed);
    RedisModule_Free(j);
}

/* Add the job in the jobs hash table, so that we can use lookupJob()
 * (by job ID) later. If a node knows about a job, the job must be registered
 * and can be retrieved via lookupJob(), regardless of is state.
 *
 * On success C_OK is returned. If there is already a job with the
 * specified ID, no operation is performed and the function returns
 * C_ERR. */
int registerJob(job *j) {
    if (raxFind(Jobs, (unsigned char*)j->id, JOB_ID_LEN) != raxNotFound)
        return C_ERR;
    raxInsert(Jobs, (unsigned char*)j->id, JOB_ID_LEN, j, NULL);
    updateJobAwakeTime(j,0);
    return C_OK;
}

/* Lookup a job by ID. */
job *lookupJob(const char *id) {
    job *j = raxFind(Jobs, (unsigned char*)id, JOB_ID_LEN);
    return j != raxNotFound ? j : NULL;
}

/* Remove job references from the system, without freeing the job itself.
 * If the job was already unregistered, C_ERR is returned, otherwise
 * C_OK is returned. */
int unregisterJob(RedisModuleCtx *ctx, job *j) {
    j = lookupJob(j->id);
    if (!j) return C_ERR;

    /* Emit a DELJOB command for all the job states but WAITREPL (no
     * ADDJOB emitted yer), and ACKED (DELJOB already emitted). */
    if (j->state >= JOB_STATE_ACTIVE && j->state != JOB_STATE_ACKED)
        AOFDelJob(ctx,j);

    /* Remove from awake skip list. */
    if (j->awakeme) RedisModule_Assert(skiplistDelete(AwakeList,j));

    /* If the job is queued, remove from queue. */
    if (j->state == JOB_STATE_QUEUED) dequeueJob(j);

    /* If there is a client blocked for this job, inform it that the job
     * got deleted, and unblock it. This should only happen when the job
     * gets expired before the requested replication level is reached. */
    if (j->state == JOB_STATE_WAIT_REPL) {
        RedisModuleBlockedClient *bc = j->bc;
        if (bc) {
            RedisModuleCtx *tsc = RedisModule_GetThreadSafeContext(bc);
            j->bc = NULL;
            RedisModule_ReplyWithError(tsc,
                "NOREPL job removed (expired?) before the requested "
                "replication level was achieved");
            /* Change job state otherwise unblockClientWaitingJobRepl() will
             * try to remove the job itself. */
            j->state = JOB_STATE_ACTIVE;
            RedisModule_FreeThreadSafeContext(tsc);
            RedisModule_UnblockClient(bc,NULL);
        }
        clusterBroadcastDelJob(ctx,j);
    }

    /* Remove the job from the dictionary of jobs. */
    raxRemove(Jobs, (unsigned char*)j->id, JOB_ID_LEN, NULL);
    return C_OK;
}

/* Return the job state as a C string pointer. This is mainly useful for
 * reporting / debugging tasks. */
char *jobStateToString(int state) {
    char *states[] = {"wait-repl","active","queued","acked"};
    if (state < 0 || state > JOB_STATE_ACKED) return "unknown";
    return states[state];
}

/* Return the state number for the specified C string, or -1 if
 * there is no match. */
int jobStateFromString(char *state) {
    if (!strcasecmp(state,"wait-repl")) return JOB_STATE_WAIT_REPL;
    else if (!strcasecmp(state,"active")) return JOB_STATE_ACTIVE;
    else if (!strcasecmp(state,"queued")) return JOB_STATE_QUEUED;
    else if (!strcasecmp(state,"acked")) return JOB_STATE_ACKED;
    else return -1;
}

/* ----------------------------- Awakeme list ------------------------------
 * Disque needs to perform periodic tasks on registered jobs, for example
 * we need to remove expired jobs (TTL reached), requeue existing jobs that
 * where not acknowledged in time, schedule the job garbage collection after
 * the job is acknowledged, and so forth.
 *
 * To simplify the handling of periodic operations without adding multiple
 * timers for each job, jobs are put into a skip list that order jobs for
 * the unix time we need to take some action about them.
 *
 * Every registered job is into this list. After we update some job field
 * that is related to scheduled operations on the job, or when it's state
 * is updated, we need to call updateJobAwakeTime() again in order to move
 * the job into the appropriate place in the awakeme skip list.
 *
 * processJobs() takes care of handling the part of the awakeme list which
 * has an awakeme time <= to the current time. As a result of processing a
 * job, we expect it to likely be updated to be processed in the future
 * again, or deleted at all. */

/* Ask the system to update the time the job will be called again as an
 * argument of awakeJob() in order to handle delayed tasks for this job.
 * If 'at' is zero, the function computes the next time we should check
 * the job status based on the next queue time (qtime), expire time, garbage
 * collection if it's an ACK, and so forth.
 *
 * Otherwise if 'at' is non-zero, it's up to the caller to set the time
 * at which the job will be awake again. */
void updateJobAwakeTime(job *j, mstime_t at) {
    if (at == 0) {
        /* Best case is to handle it for eviction. One second more is added
         * in order to make sure when the job is processed we found it to
         * be already expired. */
        at = (mstime_t)j->etime*1000+1000;

        if (j->state == JOB_STATE_ACKED) {
            /* Try to garbage collect this ACKed job again in the future. */
            mstime_t retry_gc_again = getNextGCRetryTime(j);
            if (retry_gc_again < at) at = retry_gc_again;
        } else if ((j->state == JOB_STATE_ACTIVE ||
                    j->state == JOB_STATE_QUEUED) && j->qtime) {
            /* Schedule the job to be queued, and if the job is flagged
             * BCAST_WILLQUEUE, make sure to awake the job a bit earlier
             * to broadcast a WILLQUEUE message. */
            mstime_t qtime = j->qtime;
            if (j->flags & JOB_FLAG_BCAST_WILLQUEUE)
                qtime -= JOB_WILLQUEUE_ADVANCE;
            if (qtime < at) at = qtime;
        }
    }

    /* Only update the job position into the skiplist if needed. */
    if (at != j->awakeme) {
        /* Remove from skip list. */
        if (j->awakeme) {
            RedisModule_Assert(skiplistDelete(AwakeList,j));
        }
        /* Insert it back again in the skip list with the new awake time. */
        j->awakeme = at;
        skiplistInsert(AwakeList,j);
    }
}

/* Set the specified unix time at which a job will be queued again
 * in the local node. */
void updateJobRequeueTime(job *j, mstime_t qtime) {
    /* Don't violate at-most-once (retry == 0) contract in case of bugs. */
    if (j->retry == 0 || j->qtime == 0) return;
    j->qtime = qtime;
    updateJobAwakeTime(j,0);
}

/* Job comparison inside the awakeme skiplist: by awakeme time. If it is the
 * same jobs are compared by ctime. If the same again, by job ID. */
int skiplistCompareJobsToAwake(const void *a, const void *b) {
    const job *ja = a, *jb = b;

    if (ja->awakeme > jb->awakeme) return 1;
    if (jb->awakeme > ja->awakeme) return -1;
    if (ja->ctime > jb->ctime) return 1;
    if (jb->ctime > ja->ctime) return -1;
    return memcmp(ja->id,jb->id,JOB_ID_LEN);
}

/* Used to show jobs info for debugging or under unexpected conditions. */
void logJobsDebugInfo(RedisModuleCtx *ctx, char *level, char *msg, job *j) {
    RedisModule_Log(ctx,level,
        "%s %.*s: state=%d retry=%d delay=%d replicate=%d flags=%d now=%lld awake=%lld (%lld) qtime=%lld etime=%lld",
        msg,
        JOB_ID_LEN, j->id,
        (int)j->state,
        (int)j->retry,
        (int)j->delay,
        (int)j->repl,
        (int)j->flags,
        (long long)mstime(),
        (long long)j->awakeme-mstime(),
        (long long)j->awakeme,
        (long long)j->qtime-mstime(),
        (long long)j->etime*1000-mstime()
        );
}

/* Process the specified job to perform asynchronous operations on it.
 * Check processJobs() for more info. */
void processJob(RedisModuleCtx *ctx, job *j) {
    mstime_t old_awakeme = j->awakeme;
    mstime_t now_ms = mstime();
    time_t now = now_ms / 1000;

    logJobsDebugInfo(ctx,"verbose","PROCESSING",j);

    /* Remove expired jobs. */
    if (j->etime <= now) {
        RedisModule_Log(ctx,"verbose","EVICT %.*s", JOB_ID_LEN, j->id);
        unregisterJob(ctx,j);
        freeJob(j);
        return;
    }

    /* Broadcast WILLQUEUE to inform other nodes we are going to re-queue
     * the job shortly. */
    if ((j->state == JOB_STATE_ACTIVE ||
         j->state == JOB_STATE_QUEUED) &&
         j->flags & JOB_FLAG_BCAST_WILLQUEUE &&
         j->qtime-JOB_WILLQUEUE_ADVANCE <= now_ms)
    {
        if (j->state != JOB_STATE_QUEUED) clusterSendWillQueue(ctx,j);
        /* Clear the WILLQUEUE flag, so that the job will be rescheduled
         * for when we need to queue it (otherwise it is scheduled
         * JOB_WILLQUEUE_ADVANCE milliseconds before). */
        j->flags &= ~JOB_FLAG_BCAST_WILLQUEUE;
        updateJobAwakeTime(j,0);
    }

    /* Requeue job if needed. This will also care about putting the job
     * into the queue for the first time for delayed jobs, including the
     * ones with retry=0. */
    if (j->state == JOB_STATE_ACTIVE && j->qtime <= now_ms) {
        queue *q;

        /* We need to check if the queue is paused in input. If that's
         * the case, we do:
         *
         * If retry != 0, postpone the enqueue-time of "retry" time.
         *
         * If retry == 0 (at most once job), this is a job with a delay that
         * will never be queued again, and we are the only owner.
         * In such a case, put it into the queue, or the job will be leaked. */
        if (j->retry != 0 &&
            (q = lookupQueue(j->queue,sdslen(j->queue))) != NULL &&
             q->flags & QUEUE_FLAG_PAUSED_IN)
        {
            updateJobRequeueTime(j,now_ms+
                                 j->retry*1000+
                                 randomTimeError(DISQUE_TIME_ERR));
        } else {
            enqueueJob(ctx,j,0);
        }
    }

    /* Update job re-queue time if job is already queued. */
    if (j->state == JOB_STATE_QUEUED && j->qtime <= now_ms &&
        j->retry)
    {
        j->flags |= JOB_FLAG_BCAST_WILLQUEUE;
        j->qtime = now_ms +
                   j->retry*1000 +
                   randomTimeError(DISQUE_TIME_ERR);
        updateJobAwakeTime(j,0);
    }

    /* Try a job garbage collection. */
    if (j->state == JOB_STATE_ACKED) {
        tryJobGC(ctx,j);
        updateJobAwakeTime(j,0);
    }

    if (old_awakeme == j->awakeme)
        logJobsDebugInfo(ctx,"warning", "~~~WARNING~~~ NOT PROCESSABLE JOB", j);
}

void processJobs(RedisModuleCtx *ctx, void *clientData) {
    int period = 100; /* 100 ms default period. */
    int max = 10000; /* 10k jobs * 1000 milliseconds = 10M jobs/sec max. */
    mstime_t now_ms = mstime();
    skiplistNode *current, *next;
    UNUSED(clientData);

#ifdef DEBUG_SCHEDULER
    static time_t last_log = 0;
    int canlog = 0;
    if (time(NULL) != last_log) {
        last_log = time(NULL);
        canlog = 1;
    }

    if (canlog) RedisModule_Log(ctx,"verbose","--- LEN: %d ---",
        (int) skiplistLength(AwakeList));
#endif

    current = AwakeList->header->level[0].forward;
    while(current && max--) {
        job *j = current->obj;

#ifdef DEBUG_SCHEDULER
        if (canlog) {
            RedisModule_Log(ctx,"verbose","%.*s %d (in %d) [%s]",
                JOB_ID_LEN, j->id,
                (int) j->awakeme,
                (int) (j->awakeme-server.mstime),
                jobStateToString(j->state));
        }
#endif

        if (j->awakeme > now_ms) break;
        next = current->level[0].forward;
        processJob(ctx,j);
        current = next;
    }

    /* Try to block between 1 and 100 milliseconds depending on how near
     * in time is the next async event to process. Note that because of
     * received commands or change in state jobs state may be modified so
     * we set a max time of 100 milliseconds to wakeup anyway. */
    current = AwakeList->header->level[0].forward;
    if (current) {
        job *j = current->obj;
        period = now_ms - j->awakeme;
        if (period < 1) period = 1;
        else if (period > 100) period = 100;
    }
#ifdef DEBUG_SCHEDULER
    if (canlog) RedisModule_Log(ctx,"verbose","---");
#endif

    /* Add nodes to jobs that are slow to get replicated. */
    handleDelayedJobReplication(ctx);

    /* Get scheduled to be called again after 'period' milliseconds. */
    RedisModule_CreateTimer(ctx,period,processJobs,NULL);
}

/* ---------------------------  Jobs serialization -------------------------- */

/* Serialize an SDS string as a little endian 32 bit count followed
 * by the bytes representing the string. The serialized string is
 * written to the memory pointed by 'p'. The return value of the function
 * is the original 'p' advanced of 4 + sdslen(s) bytes, in order to
 * be ready to store the next value to serialize. */
char *serializeSdsString(char *p, sds s) {
    size_t len = s ? sdslen(s) : 0;
    uint32_t count = intrev32ifbe(len);

    memcpy(p,&count,sizeof(count));
    if (s) memcpy(p+sizeof(count),s,len);
    return p + sizeof(count) + len;
}

/* Serialize the job pointed by 'j' appending the serialized version of
 * the job into the passed SDS string 'jobs'.
 *
 * The serialization may be performed in two slightly different ways
 * depending on the 'type' argument:
 *
 * If type is SER_MESSAGE the expire time field is serialized using
 * the relative TTL still remaining for the job. This serialization format
 * is suitable for sending messages to other nodes that may have non
 * synchronized clocks. If instead SER_STORAGE is used as type, the expire
 * time filed is serialized using an absolute unix time (as it is normally
 * in the job structure representation). This makes the job suitable to be
 * loaded at a latter time from disk, and is used in order to emit
 * LOADJOB commands in the AOF file.
 *
 * Moreover if SER_MESSAGE is used, the JOB_FLAG_DELIVERED is cleared before
 * the serialization, since this is a local node flag and should not be
 * propagated.
 *
 * When the job is deserialized with deserializeJob() function call, the
 * appropriate type must be passed, depending on how the job was serialized.
 *
 * Serialization format
 * ---------------------
 *
 * len | struct | queuename | job | nodes
 *
 * len: The first 4 bytes are a little endian 32 bit unsigned
 * integer that announces the full size of the serialized job.
 *
 * struct: JOB_STRUCT_SER_LEN bytes of the 'job' structure
 * with fields fixed to be little endian regardless of the arch of the
 * system.
 *
 * queuename: uint32_t little endian len + actual bytes of the queue
 * name string.
 *
 * job: uint32_t little endian len + actual bytes of the job body.
 *
 * nodes: List of nodes that may have a copy of the message. uint32_t
 * little endian with the count of N node names following. Then N
 * fixed length node names of CLUSTER_NODE_NAMELEN characters each.
 *
 * The message is concatenated to the existing sds string 'jobs'.
 * Just use sdsempty() as first argument to get a single job serialized.
 *
 * ----------------------------------------------------------------------
 *
 * Since each job has a prefixed length it is possible to glue multiple
 * jobs one after the other in a single string. */
sds serializeJob(sds jobs, job *j, int sertype) {
    size_t len;
    struct job *sj;
    char *p, *msg;
    uint32_t count;

    /* Compute the total length of the serialized job. */
    len = 4;                    /* Prefixed length of the serialized bytes. */
    len += JOB_STRUCT_SER_LEN;  /* Structure header directly serializable. */
    len += 4;                   /* Queue name length field. */
    len += j->queue ? sdslen(j->queue) : 0; /* Queue name bytes. */
    len += 4;                   /* Body length field. */
    len += j->body ? sdslen(j->body) : 0; /* Body bytes. */
    len += 4;                   /* Node IDs (that may have a copy) count. */
    len += raxSize(j->nodes_delivered) * REDISMODULE_NODE_ID_LEN;

    /* Make room at the end of the SDS buffer to hold our message. */
    jobs = sdsMakeRoomFor(jobs,len);
    msg = jobs + sdslen(jobs); /* Concatenate to the end of buffer. */
    sdsIncrLen(jobs,len); /* Adjust SDS string final length. */

    /* Total serialized length prefix, not including the length itself. */
    count = intrev32ifbe(len-4);
    memcpy(msg,&count,sizeof(count));

    /* The serializable part of the job structure is copied, and fields
     * fixed to be little endian (no op in little endian CPUs). */
    sj = (job*) (msg+4);
    memcpy(sj,j,JOB_STRUCT_SER_LEN);
    memrev16ifbe(&sj->repl);
    memrev64ifbe(&sj->ctime);
    /* Use a relative expire time for serialization, but only for the
     * type SER_MESSAGE. When we want to target storage, it's better to use
     * absolute times in every field. */
    time_t now = time(NULL);
    if (sertype == SER_MESSAGE) {
        if (sj->etime >= now)
            sj->etime = sj->etime - now + 1;
        else
            sj->etime = 1;
        sj->flags &= ~JOB_FLAG_DELIVERED;
    }
    memrev32ifbe(&sj->etime);
    memrev32ifbe(&sj->delay);
    memrev32ifbe(&sj->retry);
    memrev16ifbe(&sj->num_nacks);
    memrev16ifbe(&sj->num_deliv);

    /* p now points to the start of the variable part of the serialization. */
    p = msg + 4 + JOB_STRUCT_SER_LEN;

    /* Queue name is 4 bytes prefixed len in little endian + actual bytes. */
    p = serializeSdsString(p,j->queue);

    /* Body is 4 bytes prefixed len in little endian + actual bytes. */
    p = serializeSdsString(p,j->body);

    /* Node IDs that may have a copy of the message: 4 bytes count in little
     * endian plus (count * REDISMODULE_NODE_ID_LEN) bytes. */
    count = intrev32ifbe(raxSize(j->nodes_delivered));
    memcpy(p,&count,sizeof(count));
    p += sizeof(count);

    raxIterator ri;
    raxStart(&ri,j->nodes_delivered);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        memcpy(p,ri.key,ri.key_len);
        p += REDISMODULE_NODE_ID_LEN;
    }
    raxStop(&ri);

    /* Make sure we wrote exactly the intended number of bytes. */
    RedisModule_Assert(len == (size_t)(p-msg));
    return jobs;
}

/* Deserialize a job serialized with serializeJob. Note that this only
 * deserializes the first job even if the input buffer contains multiple
 * jobs, but it stores the pointer to the next job (if any) into
 * '*next'. If there are no more jobs, '*next' is set to NULL.
 * '*next' is not updated if 'next' is a NULL pointer.
 *
 * The return value is the job structure populated with all the fields
 * present in the serialized structure. On deserialization error (wrong
 * format) NULL is returned.
 *
 * Arguments: 'p' is the pointer to the start of the job (the 4 bytes
 * where the job serialized length is stored). While 'len' is the total
 * number of bytes the buffer contains (that may be larger than the
 * serialized job 'p' is pointing to).
 *
 * The 'sertype' field specifies the serialization type the job was
 * serialized with, by serializeJob() call.
 *
 * When the serialization type is SER_STORAGE, the job state is loaded
 * as it is, otherwise when SER_MESSAGE is used, the job state is set
 * to JOB_STATE_ACTIVE.
 *
 * In both cases the gc retry field is reset to 0. */
job *deserializeJob(RedisModuleCtx *ctx, unsigned char *p, size_t len, unsigned char **next, int sertype) {
    job *j = RedisModule_Calloc(1,sizeof(*j));
    unsigned char *start = p; /* To check total processed bytes later. */
    uint32_t joblen, aux;

    /* Min len is: 4 (joblen) + JOB_STRUCT_SER_LEN + 4 (queue name len) +
     * 4 (body len) + 4 (Node IDs count) */
    if (len < 4+JOB_STRUCT_SER_LEN+4+4+4) goto fmterr;

    /* Get total length. */
    memcpy(&joblen,p,sizeof(joblen));
    p += sizeof(joblen);
    len -= sizeof(joblen);
    joblen = intrev32ifbe(joblen);
    if (len < joblen) goto fmterr;

    /* Deserialize the static part just copying and fixing endianess. */
    memcpy(j,p,JOB_STRUCT_SER_LEN);
    memrev16ifbe(j->repl);
    memrev64ifbe(j->ctime);
    memrev32ifbe(j->etime);
    if (sertype == SER_MESSAGE) {
        /* Convert back to absolute time if needed. */
        j->etime = time(NULL) + j->etime;
    }
    memrev32ifbe(j->delay);
    memrev32ifbe(j->retry);
    memrev16ifbe(&sj->num_nacks);
    memrev16ifbe(&sj->num_deliv);
    p += JOB_STRUCT_SER_LEN;
    len -= JOB_STRUCT_SER_LEN;

    /* GC attempts are always reset, while the state will be likely set to
     * the caller, but otherwise, we assume the job is active if this message
     * is received from another node. When loading a message from disk instead
     * (SER_STORAGE serializaiton type), the state is left untouched. */
    if (sertype == SER_MESSAGE) j->state = JOB_STATE_ACTIVE;
    j->gc_retry = 0;

    /* Compute next queue time from known parameters. */
    if (j->retry) {
        j->flags |= JOB_FLAG_BCAST_WILLQUEUE;
        j->qtime = mstime() +
                   j->delay*1000 +
                   j->retry*1000 +
                   randomTimeError(DISQUE_TIME_ERR);
    } else {
        j->qtime = 0;
    }

    /* Queue name. */
    memcpy(&aux,p,sizeof(aux));
    p += sizeof(aux);
    len -= sizeof(aux);
    aux = intrev32ifbe(aux);

    if (len < aux) goto fmterr;
    j->queue = sdsnewlen((char*)p,aux);
    p += aux;
    len -= aux;

    /* Job body. */
    memcpy(&aux,p,sizeof(aux));
    p += sizeof(aux);
    len -= sizeof(aux);
    aux = intrev32ifbe(aux);

    if (len < aux) goto fmterr;
    j->body = sdsnewlen(p,aux);
    p += aux;
    len -= aux;

    /* Nodes IDs. */
    memcpy(&aux,p,sizeof(aux));
    p += sizeof(aux);
    len -= sizeof(aux);
    aux = intrev32ifbe(aux);

    if (len < aux*REDISMODULE_NODE_ID_LEN) goto fmterr;
    j->nodes_delivered = raxNew();
    while(aux--) {
        if (RedisModule_GetClusterNodeInfo(ctx,(char*)p,NULL,NULL,NULL,NULL) !=
            REDISMODULE_ERR)
        {
            raxInsert(j->nodes_delivered,p,REDISMODULE_NODE_ID_LEN,NULL,NULL);
        }
        p += REDISMODULE_NODE_ID_LEN;
        len -= REDISMODULE_NODE_ID_LEN;
    }

    if ((uint32_t)(p-start)-sizeof(joblen) != joblen) goto fmterr;
    if (len && next) *next = p;
    return j;

fmterr:
    freeJob(j);
    return NULL;
}

/* This function is called when the job id at 'j' may be duplicated and we
 * likely already have the job, but we want to update the list of nodes
 * that may have the message by taking the union of our list with the
 * job 'j' list. */
void updateJobNodes(job *j) {
    job *old = lookupJob(j->id);
    if (!old) return;

    raxIterator ri;
    raxStart(&ri,j->nodes_delivered);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        raxInsert(old->nodes_delivered,ri.key,ri.key_len,NULL,NULL);
    }
    raxStop(&ri);
}

/* ----------------------------  Utility functions -------------------------- */

/* Validate a set of job IDs. Return C_OK if all the IDs are valid,
 * otherwise C_ERR is returned.
 *
 * When C_ERR is returned, an error is send to the client 'c' if not
 * NULL. */
int validateJobIDs(RedisModuleCtx *ctx, RedisModuleString **ids, int count) {
    int j;

    /* Mass-validate the Job IDs, so if we have to stop with an error, nothing
     * at all is processed. */
    for (j = 0; j < count; j++) {
        size_t idlen;
        const char *id = RedisModule_StringPtrLen(ids[j],&idlen);
        if (validateJobIdOrReply(ctx,id,idlen) == C_ERR) return C_ERR;
    }
    return C_OK;
}

/* --------------------------  Jobs related commands ------------------------ */

/* This is called when a client blocked in ADDJOB is unblocked. Our private
 * data in this case is just a string holding the job ID. */
void addjobClientFree(RedisModuleCtx *ctx, void *privdata) {
    REDISMODULE_NOT_USED(ctx);
    if (privdata) RedisModule_Free(privdata);
}

/* Called when a client blocked on ADDJOB disconnected before the timeout
 * or the successful replication were reached. This is not called if the
 * client timeout or was explicitly unblocked by the module.
 *
 * This is also called by the timeout handler in order to evict the job
 * when the replication was not reached. */
void addjobDisconnected(RedisModuleCtx *ctx, RedisModuleBlockedClient *bc) {
    job *j = raxFind(BlockedOnRepl,(unsigned char*)&bc,sizeof(bc));
    if (j != raxNotFound) {
        raxRemove(BlockedOnRepl,(unsigned char*)&j->bc,sizeof(j->bc),NULL);
        j->bc = NULL; /* Avoid that we try to unblock the client. */
        unregisterJob(ctx,j);
        freeJob(j);
    }
}

/* Reply to ADDJOB client after we successful unblocked or timed out. */
int addjobClientReply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    RedisModuleBlockedClient *bc = RedisModule_GetBlockedClientHandle(ctx);
    const char *id = RedisModule_GetBlockedClientPrivateData(ctx);
    if (RedisModule_IsBlockedTimeoutRequest(ctx)) {
        /* If we are here the job state is JOB_STATE_WAIT_REPL. */
        RedisModule_ReplyWithNull(ctx);
        addjobDisconnected(ctx,bc);
    } else {
        /* If we are here the job state is JOB_STATE_ACTIVE. */
        if (id != NULL) RedisModule_ReplyWithStringBuffer(ctx,id,JOB_ID_LEN);
        raxRemove(BlockedOnRepl,(unsigned char*)&bc,sizeof(bc),NULL);
    }
    return REDISMODULE_OK;
}

/* Return a simple string reply with the Job ID. */
void addReplyJobID(RedisModuleCtx *ctx, job *j) {
    RedisModule_ReplyWithStringBuffer(ctx,j->id,JOB_ID_LEN);
}

/* Send an ENQUEUE message to a random node among the ones that we believe
 * have a copy. This is used when we want to discard a job but want it to
 * be processed in a short time by another node, without waiting for the
 * retry. */
void clusterSendEnqueueToRandomNode(RedisModuleCtx *ctx, job *j) {
    if (raxSize(j->nodes_confirmed) > 0) {
        raxIterator ri;
        raxStart(&ri,j->nodes_confirmed);
        raxSeek(&ri,"^",NULL,0);
        raxRandomWalk(&ri,0);
        clusterSendEnqueue(ctx,(char*)ri.key,j,j->delay);
        raxStop(&ri);
    }
}

/* This function is called by cluster.c when the job was replicated
 * and the replication acknowledged at least job->repl times.
 *
 * Here we need to queue the job, and unblock the client waiting for the job
 * if it still exists.
 *
 * This function is only called if the job is in JOB_STATE_WAIT_REPL state..
 * The function also assumes that there is a client waiting to be
 * unblocked if this function is called, since if the blocked client is
 * released, the job is deleted (and a best effort try is made to remove
 * copies from other nodes), to avoid non acknowledged jobs to be active
 * when possible.
 *
 * Return value: if the job is retained after the function is called
 * (normal replication) then C_OK is returned. Otherwise if the
 * function removes the job from the node, since the job is externally
 * replicated, C_ERR is returned, in order to signal the client further
 * accesses to the job are not allowed. */
int jobReplicationAchieved(RedisModuleCtx *ctx, job *j) {
    RedisModule_Log(ctx,"verbose","Replication ACHIEVED %.*s",JOB_ID_LEN,j->id);

    /* Change the job state to active. This is critical to avoid the job
     * will be freed by unblockClient() if found still in the old state. */
    j->state = JOB_STATE_ACTIVE;

    /* Reply to the blocked client with the Job ID and unblock the client. */
    RedisModuleBlockedClient *bc = j->bc;
    j->bc = NULL;
    char *id = RedisModule_Alloc(JOB_ID_LEN);
    memcpy(id,j->id,JOB_ID_LEN);
    RedisModule_UnblockClient(bc,id);

    /* If the job was externally replicated, send a QUEUE message to one of
     * the nodes that acknowledged to have a copy, and forget about it ASAP. */
    const char *myself = RedisModule_GetMyClusterID();
    if (raxFind(j->nodes_delivered,(unsigned char*)myself,REDISMODULE_NODE_ID_LEN) == raxNotFound) {
        clusterSendEnqueueToRandomNode(ctx,j);
        unregisterJob(ctx,j);
        freeJob(j);
        return C_ERR;
    }

    /* If set, cleanup nodes_confirmed to free memory. We'll reuse this
     * hash table again for ACKs tracking in order to garbage collect the
     * job once processed. */
    if (j->nodes_confirmed) {
        raxFree(j->nodes_confirmed);
        j->nodes_confirmed = NULL;
    }

    /* Queue the job locally. */
    if (j->delay == 0)
        enqueueJob(ctx,j,0); /* Will change the job state. */
    else
        updateJobAwakeTime(j,0); /* Queue with delay. */

    AOFLoadJob(ctx,j);
    return C_OK;
}

/* This function is called periodically by Disque. Its goal is to
 * check if a job synchronous replication is taking too time, and add a new
 * node to the set of nodes contacted in order to replicate the job.
 * This way some of the nodes initially contacted are not reachable, are
 * slow, or are out of memory (and are not accepting our job), we have a
 * chance to make the ADDJOB call succeed using other nodes. */
#define DELAYED_JOB_ADD_NODE_MIN_PERIOD 50 /* 50 milliseconds. */
#define DELAYED_JOB_MAX_ITERATION 100 /* Don't check too many jobs per cycle. */
void handleDelayedJobReplication(RedisModuleCtx *ctx) {
    mstime_t now = mstime();
    static unsigned char cursor[sizeof(RedisModuleBlockedClient*)];

    raxIterator ri;
    raxStart(&ri,BlockedOnRepl);
    raxSeek(&ri,">",cursor,sizeof(cursor));
    int maxiter = DELAYED_JOB_MAX_ITERATION;
    while(maxiter > 0 && raxNext(&ri)) {
        maxiter--;
        job *j = ri.data;
        mstime_t elapsed = now - j->added_node_time;
        if (elapsed >= DELAYED_JOB_ADD_NODE_MIN_PERIOD) {
            if (clusterReplicateJob(ctx,j,1,0) != 0)
                RedisModule_Log(ctx,"verbose",
                    "Delayed job %*s replicated to one more node.",
                    JOB_ID_LEN, j->id);
        }
    }
    /* If we scanned the full list. Restart from the first one next time. */
    if (raxEOF(&ri)) memset(cursor,0,sizeof(cursor));
    raxStop(&ri);
}

/* Copy a Redis module string and return it as an SDS string. */
sds RedisModule_StringToSds(RedisModuleString *s) {
    size_t len;
    const char *ptr = RedisModule_StringPtrLen(s,&len);
    return sdsnewlen(ptr,len);
}

/* Utility function to parse timeouts in ADDJOB and other commands.
 * The timeout is stored in '*timeout' in milliseconds, so the caller
 * must specify the input 'unit'. On format errors the client associated
 * with the context will receive an error, and REDISMODULE_ERR will be
 * returned by the function. */
int getTimeoutFromObjectOrReply(RedisModuleCtx *ctx, RedisModuleString *object, mstime_t *timeout, int unit) {
    long long tval;

    if (RedisModule_StringToLongLong(object,&tval) == REDISMODULE_ERR) {
        RedisModule_ReplyWithError(ctx,
            "ERR timeout is not an integer or out of range");
        return REDISMODULE_ERR;
    }

    if (tval < 0) {
        RedisModule_ReplyWithError(ctx,
            "ERR timeout is negative");
        return REDISMODULE_ERR;
    }

    if (tval > 0) if (unit == UNIT_SECONDS) tval *= 1000;
    *timeout = tval;

    return REDISMODULE_OK;
}

/* ADDJOB queue job timeout [REPLICATE <n>] [TTL <sec>] [RETRY <sec>] [ASYNC]
 *
 * The function changes replication strategy if the memory warning level
 * is greater than zero.
 *
 * When there is no memory pressure:
 * 1) A copy of the job is replicated locally.
 * 2) The job is queued locally.
 * 3) W-1 copies of the job are replicated to other nodes, synchronously
 *    or asynchronously if ASYNC is provided.
 *
 * When there is memory pressure:
 * 1) The job is replicated only to W external nodes.
 * 2) The job is queued to a random external node sending a QUEUE message.
 * 3) QUEUE is sent ASAP for asynchronous jobs, for synchronous jobs instead
 *    QUEUE is sent by jobReplicationAchieved to one of the nodes that
 *    acknowledged to have a copy of the job.
 * 4) The job is discareded by the local node ASAP, that is, when the
 *    selected replication level is achieved or before to returning to
 *    the caller for asynchronous jobs. */
int addjobCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 4) return RedisModule_WrongArity(ctx);

    int cluster_size = RedisModule_GetClusterSize();
    long long replicate = cluster_size > 3 ? 3 : cluster_size;
    long long ttl = 3600*24;
    long long retry = -1;
    long long delay = 0;
    long long maxlen = 0; /* Max queue length for job to be accepted. */
    mstime_t timeout, now_ms = mstime();
    int j, retval;
    int async = 0;  /* Asynchronous request? */
    int leaving = myselfLeaving();
    static uint64_t prev_ctime = 0;

    /* Replicate externally? */
    int extrepl = RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_OOM; 

    /* Another case for external replication, other than memory pressure, is
     * if this node is leaving the cluster. In this case we don't want to create
     * new messages here. */
    if (leaving) extrepl = 1;

    /* Parse args. */
    for (j = 4; j < argc; j++) {
        const char *opt = RedisModule_StringPtrLen(argv[j],NULL);
        int lastarg = (j == argc-1);
        if (!strcasecmp(opt,"replicate") && !lastarg) {
            retval = RedisModule_StringToLongLong(argv[j+1],&replicate);
            if (retval != REDISMODULE_OK || replicate <= 0 || replicate > 65535)
            {
                return RedisModule_ReplyWithError(ctx,
                    "ERR REPLICATE must be between 1 and 65535");
            }
            j++;
        } else if (!strcasecmp(opt,"ttl") && !lastarg) {
            retval = RedisModule_StringToLongLong(argv[j+1],&ttl);
            if (retval != REDISMODULE_OK || ttl <= 0) {
                return RedisModule_ReplyWithError(ctx,
                    "ERR TTL must be a number > 0");
            }
            j++;
        } else if (!strcasecmp(opt,"retry") && !lastarg) {
            retval = RedisModule_StringToLongLong(argv[j+1],&retry);
            if (retval != REDISMODULE_OK || retry < 0) {
                return RedisModule_ReplyWithError(ctx,
                    "ERR RETRY time must be a non negative number");
            }
            j++;
        } else if (!strcasecmp(opt,"delay") && !lastarg) {
            retval = RedisModule_StringToLongLong(argv[j+1],&delay);
            if (retval != REDISMODULE_OK || delay < 0) {
                return RedisModule_ReplyWithError(ctx,
                    "ERR DELAY time must be a non negative number");
            }
            j++;
        } else if (!strcasecmp(opt,"maxlen") && !lastarg) {
            retval = RedisModule_StringToLongLong(argv[j+1],&maxlen);
            if (retval != REDISMODULE_OK || maxlen <= 0) {
                return RedisModule_ReplyWithError(ctx,
                    "ERR MAXLEN must be a positive number");
            }
            j++;
        } else if (!strcasecmp(opt,"async")) {
            async = 1;
        } else {
            return RedisModule_ReplyWithError(ctx,"ERR Syntax error");
        }
    }

    /* Parse the timeout argument. */
    if (getTimeoutFromObjectOrReply(ctx,argv[3],&timeout,UNIT_MILLISECONDS)
        != REDISMODULE_OK) return REDISMODULE_OK;

    /* REPLICATE > 1 and RETRY set to 0 does not make sense, why to replicate
     * the job if it will never try to be re-queued if case the job processing
     * is not acknowledged? */
    if (replicate > 1 && retry == 0) {
       return RedisModule_ReplyWithError(ctx,
            "ERR With RETRY set to 0 please explicitly set  "
            "REPLICATE to 1 (at-most-once delivery)");
    }

    /* DELAY greater or equal to TTL is silly. */
    if (delay >= ttl) {
        return RedisModule_ReplyWithError(ctx,
            "ERR The specified DELAY is greater than TTL. Job refused "
            "since would never be delivered");
    }

    /* When retry is not specified, it defaults to 1/10 of the TTL, with
     * an hard limit of JOB_DEFAULT_RETRY_MAX seconds (5 minutes normally). */
    if (retry == -1) {
        retry = ttl/10;
        if (retry > JOB_DEFAULT_RETRY_MAX) retry = JOB_DEFAULT_RETRY_MAX;
        if (retry == 0) retry = 1;
    }

    /* Check if REPLICATE can't be honoured at all. */
    int additional_nodes = extrepl ? replicate : replicate-1;

    if (additional_nodes > ClusterReachableNodesCount) {
        if (extrepl &&
            additional_nodes-1 == ClusterReachableNodesCount)
        {
            return RedisModule_ReplyWithError(ctx,
                       "NOREPL Not enough reachable nodes "
                       "for the requested replication level, since I'm unable "
                       "to hold a copy of the message (OOM or leaving the "
                       "cluster)");
        } else {
            return RedisModule_ReplyWithError(ctx,
                        "NOREPL Not enough reachable nodes "
                        "for the requested replication level");
        }
    }

    /* Lookup the queue by the name, in order to perform checks for
     * MAXLEN and to check for paused queue. */
    size_t qnamelen;
    const char *qname = RedisModule_StringPtrLen(argv[1],&qnamelen);
    queue *q = lookupQueue(qname,qnamelen);

    /* If maxlen was specified, check that the local queue len is
     * within the requested limits. */
    if (maxlen && q && queueLength(q) >= (unsigned long) maxlen) {
        return RedisModule_ReplyWithError(ctx,
                "MAXLEN Queue is already longer than "
                "the specified MAXLEN count");
    }

    /* If the queue is paused in input, refuse the job. */
    if (q && q->flags & QUEUE_FLAG_PAUSED_IN) {
        return RedisModule_ReplyWithError(ctx,
                "PAUSED Queue paused in input, try later");
    }

    /* Are we going to discard the local copy before to return to the caller?
     * This happens when the job is at the same type asynchronously
     * replicated AND because of memory warning level we are going to
     * replicate externally without taking a copy. */
    int discard_local_copy = async && extrepl;
    const char *myself = RedisModule_GetMyClusterID();

    /* Create a new job. */
    job *job = createJob(NULL,JOB_STATE_WAIT_REPL,ttl,retry);
    job->queue = RedisModule_StringToSds(argv[1]);
    job->repl = replicate;

    /* If no external replication is used, add myself to the list of nodes
     * that have a copy of the job. */
    if (!extrepl) {
        raxInsert(job->nodes_delivered,(unsigned char*)myself,
                  REDISMODULE_NODE_ID_LEN,NULL,NULL);
    }

    /* Job ctime is milliseconds * 1000000. Jobs created in the same
     * millisecond gets an incremental ctime. The ctime is used to sort
     * queues, so we have some weak sorting semantics for jobs: non-requeued
     * jobs are delivered roughly in the order they are added into a given
     * node. */
    job->ctime = now_ms*1000000;
    if (job->ctime <= prev_ctime) job->ctime = prev_ctime+1;
    prev_ctime = job->ctime;

    job->etime = now_ms/1000 + ttl;
    job->delay = delay;
    job->retry = retry;
    job->body = RedisModule_StringToSds(argv[2]);
    job->added_node_time = now_ms;

    /* Set the next time the job will be queued. Note that once we call
     * enqueueJob() the first time, this will be set to 0 (never queue
     * again) for jobs that have a zero retry value (at most once jobs). */
    if (delay) {
        job->qtime = now_ms + delay*1000;
    } else {
        /* This will be updated anyway by enqueueJob(). */
        job->qtime = now_ms + retry*1000;
    }

    /* Register the job locally, unless we are going to remove it locally. */
    if (!discard_local_copy && registerJob(job) == C_ERR) {
        /* A job ID with the same name? Practically impossible but
         * let's handle it to trap possible bugs in a cleaner way. */
        RedisModule_Log(ctx,"warning","ID already existing in ADDJOB command!");
        freeJob(job);
        return RedisModule_ReplyWithError(ctx,
            "ERR Internal error creating the job, check server logs");
    }

    /* For replicated messages where ASYNC option was not asked, block
     * the client, and wait for acks. Otherwise if no synchronous replication
     * is used or if we don't have additional copies to deliver, we just queue
     * the job and return to the client ASAP.
     *
     * Note that for REPLICATE > 1 and ASYNC the replication process is
     * best effort. */
    if ((replicate > 1 || extrepl) && !async) {
        job->bc = RedisModule_BlockClient(ctx,addjobClientReply,addjobClientReply,addjobClientFree,timeout);
        raxInsert(BlockedOnRepl,(unsigned char*)&job->bc,sizeof(job->bc),
            job,NULL);
        RedisModule_SetDisconnectCallback(job->bc,addjobDisconnected);
        /* Create the nodes_confirmed dictionary only if we actually need
         * it for synchronous replication. It will be released later
         * when we move away from JOB_STATE_WAIT_REPL. */
        job->nodes_confirmed = raxNew();
        /* Confirm itself as an acknowledged receiver if this node will
         * retain a copy of the job. */
        if (!extrepl) {
            raxInsert(job->nodes_confirmed,(unsigned char*)myself,
                      REDISMODULE_NODE_ID_LEN,NULL,NULL);
        }
    } else {
        if (job->delay == 0) {
            if (!extrepl) enqueueJob(ctx,job,0); /* Will change the job state */
        } else {
            /* Delayed jobs that don't wait for replication can move
             * forward to ACTIVE state ASAP, and get scheduled for
             * queueing. */
            job->state = JOB_STATE_ACTIVE;
            if (!discard_local_copy) updateJobAwakeTime(job,0);
        }
        addReplyJobID(ctx,job);
        if (!extrepl) AOFLoadJob(ctx,job);
    }

    /* If the replication factor is > 1, send REPLJOB messages to REPLICATE-1
     * nodes. */
    if (additional_nodes > 0)
        clusterReplicateJob(ctx, job, additional_nodes, async);

    /* If the job is asynchronously and externally replicated at the same time,
     * send an ENQUEUE message ASAP to one random node, and delete the job from
     * this node right now. */
    if (discard_local_copy) {
        clusterSendEnqueueToRandomNode(ctx,job);
        /* We don't have to unregister the job since we did not registered
         * it if it's async + extrepl. */
        freeJob(job);
    }
    return REDISMODULE_OK;
}

/* Client reply function for SHOW and JSCAN. */
void addReplyJobInfo(RedisModuleCtx *ctx, job *j) {
    RedisModule_ReplyWithArray(ctx,30);

    RedisModule_ReplyWithSimpleString(ctx,"id");
    RedisModule_ReplyWithStringBuffer(ctx,j->id,JOB_ID_LEN);

    RedisModule_ReplyWithSimpleString(ctx,"queue");
    if (j->queue)
        RedisModule_ReplyWithStringBuffer(ctx,j->queue,sdslen(j->queue));
    else
        RedisModule_ReplyWithNull(ctx);

    RedisModule_ReplyWithSimpleString(ctx,"state");
    RedisModule_ReplyWithSimpleString(ctx,jobStateToString(j->state));

    RedisModule_ReplyWithSimpleString(ctx,"repl");
    RedisModule_ReplyWithLongLong(ctx,j->repl);

    int64_t ttl = j->etime - time(NULL);
    if (ttl < 0) ttl = 0;
    RedisModule_ReplyWithSimpleString(ctx,"ttl");
    RedisModule_ReplyWithLongLong(ctx,ttl);

    RedisModule_ReplyWithSimpleString(ctx,"ctime");
    RedisModule_ReplyWithLongLong(ctx,j->ctime);

    RedisModule_ReplyWithSimpleString(ctx,"delay");
    RedisModule_ReplyWithLongLong(ctx,j->delay);

    RedisModule_ReplyWithSimpleString(ctx,"retry");
    RedisModule_ReplyWithLongLong(ctx,j->retry);

    RedisModule_ReplyWithSimpleString(ctx,"nacks");
    RedisModule_ReplyWithLongLong(ctx,j->num_nacks);

    RedisModule_ReplyWithSimpleString(ctx,"additional-deliveries");
    RedisModule_ReplyWithLongLong(ctx,j->num_deliv);

    RedisModule_ReplyWithSimpleString(ctx,"nodes-delivered");
    if (j->nodes_delivered) {
        RedisModule_ReplyWithArray(ctx,raxSize(j->nodes_delivered));
        raxIterator ri;
        raxStart(&ri,j->nodes_delivered);
        raxSeek(&ri,"^",NULL,0);
        while(raxNext(&ri)) {
            RedisModule_ReplyWithStringBuffer(ctx,(char*)ri.key,ri.key_len);
        }
        raxStop(&ri);
    } else {
        RedisModule_ReplyWithArray(ctx,0);
    }

    RedisModule_ReplyWithSimpleString(ctx,"nodes-confirmed");
    if (j->nodes_confirmed) {
        RedisModule_ReplyWithArray(ctx,raxSize(j->nodes_confirmed));
        raxIterator ri;
        raxStart(&ri,j->nodes_confirmed);
        raxSeek(&ri,"^",NULL,0);
        while(raxNext(&ri)) {
            RedisModule_ReplyWithStringBuffer(ctx,(char*)ri.key,ri.key_len);
        }
        raxStop(&ri);
    } else {
        RedisModule_ReplyWithArray(ctx,0);
    }

    mstime_t next_requeue = j->qtime - mstime();
    if (next_requeue < 0) next_requeue = 0;
    RedisModule_ReplyWithSimpleString(ctx,"next-requeue-within");
    if (j->qtime == 0)
        RedisModule_ReplyWithNull(ctx);
    else
        RedisModule_ReplyWithLongLong(ctx,next_requeue);

    mstime_t next_awake = j->awakeme - mstime();
    if (next_awake < 0) next_awake = 0;
    RedisModule_ReplyWithSimpleString(ctx,"next-awake-within");
    if (j->awakeme == 0)
        RedisModule_ReplyWithNull(ctx);
    else
        RedisModule_ReplyWithLongLong(ctx,next_awake);

    RedisModule_ReplyWithSimpleString(ctx,"body");
    if (j->body)
        RedisModule_ReplyWithStringBuffer(ctx,j->body,sdslen(j->body));
    else
        RedisModule_ReplyWithNull(ctx);
}

/* SHOW <job-id> */
int showCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);

    REDISMODULE_NOT_USED(argc);
    size_t idlen;
    const char *id = RedisModule_StringPtrLen(argv[1],&idlen);
    if (validateJobIdOrReply(ctx,id,idlen) == C_ERR) return REDISMODULE_OK;

    const char *jobid = RedisModule_StringPtrLen(argv[1],NULL);
    job *j = lookupJob(jobid);
    if (!j) return RedisModule_ReplyWithNull(ctx);
    addReplyJobInfo(ctx,j);
    return REDISMODULE_OK;
}

/* DELJOB jobid_1 jobid_2 ... jobid_N
 *
 * Evict (and possibly remove from queue) all the jobs in memory
 * matching the specified job IDs. Jobs are evicted whatever their state
 * is, since this command is mostly used inside the AOF or for debugging
 * purposes.
 *
 * The return value is the number of jobs evicted.
 */
int deljobCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 2) return RedisModule_WrongArity(ctx);

    int j, evicted = 0;

    if (validateJobIDs(ctx,argv+1,argc-1) == C_ERR) return REDISMODULE_OK;

    /* Perform the appropriate action for each job. */
    for (j = 1; j < argc; j++) {
        const char *jobid = RedisModule_StringPtrLen(argv[j],NULL);
        job *job = lookupJob(jobid);
        if (job == NULL) continue;
        unregisterJob(ctx,job);
        freeJob(job);
        evicted++;
    }
    return RedisModule_ReplyWithLongLong(ctx,evicted);
}

