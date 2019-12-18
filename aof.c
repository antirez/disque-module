/* Copyright (c) 2014-2019, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved. This code is under the AGPL license, check the
 * LICENSE file for more info. */

#include "disque.h"

/* AOF implementation is work in progress. */

void AOFLoadJob(RedisModuleCtx *ctx, job *job) {
    sds serialized = serializeJob(sdsempty(),job,SER_STORAGE);
    RedisModule_Replicate(ctx,"LOADJOB","b",serialized,sdslen(serialized));
    sdsfree(serialized);
}

void AOFDelJob(RedisModuleCtx *ctx, job *job) {
    RedisModule_Replicate(ctx,"DELJOB","b",job->id,JOB_ID_LEN);
}

void AOFAckJob(RedisModuleCtx *ctx, job *job) {
    RedisModule_Replicate(ctx,"ACKJOB","b",job->id,JOB_ID_LEN);
}

void AOFDequeueJob(RedisModuleCtx *ctx, job *job) {
    RedisModule_Replicate(ctx,"DEQUEUE","b",job->id,JOB_ID_LEN);
}

#define DISQUE_RDB_OPCODE_EOF 1
#define DISQUE_RDB_OPCODE_MOREJOBS 2

/* Serialized all the jobs in the RDB file: since we force the user to
 * configure the Redis RDB preamble with AOF, this is our de-facto
 * AOF rewriting function. */
void DisqueRDBAuxSave(RedisModuleIO *rdb, int when) {
    UNUSED(when);
    raxIterator ri;
    raxStart(&ri,Jobs);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        job *job = ri.data;
        /* We need to persist only jobs that are in a state interesting
         * to load back. Jobs that are yet replicating or that are acknowledged
         * are not persisted. */
        RedisModule_SaveUnsigned(rdb,DISQUE_RDB_OPCODE_MOREJOBS);
        if (job->state != JOB_STATE_ACTIVE &&
            job->state != JOB_STATE_QUEUED) continue;
        sds serialized = serializeJob(sdsempty(),job,SER_STORAGE);
        RedisModule_SaveStringBuffer(rdb,serialized,sdslen(serialized));
    }
    raxStop(&ri);
    RedisModule_SaveUnsigned(rdb,DISQUE_RDB_OPCODE_EOF);
}

/* Load the jobs back from the RDB AOF preamble. */
int DisqueRDBAuxLoad(RedisModuleIO *rdb, int encver, int when) {
    UNUSED(when);
    UNUSED(encver);

    RedisModuleCtx *ctx = RedisModule_GetContextFromIO(rdb);
    time_t now = time(NULL);
    while(1) {
        /* Load the opcode, and stop if it end-of-jobs. */
        int opcode = RedisModule_LoadUnsigned(rdb);
        if (RedisModule_IsIOError(rdb)) return REDISMODULE_ERR;
        if (opcode == DISQUE_RDB_OPCODE_EOF) break;

        /* Load the serialized job. */
        RedisModuleString *serialized = RedisModule_LoadString(rdb);
        if (RedisModule_IsIOError(rdb)) return REDISMODULE_ERR;

        /* Decode it. */
        size_t len;
        const char *buf = RedisModule_StringPtrLen(serialized,&len);
        job *job = deserializeJob(ctx,
            (unsigned char*)buf,len,NULL,SER_STORAGE);
        if (job == NULL) {
            RedisModule_Log(ctx,"warning",
                "Deserialization error while reading job from RDB");
            return REDISMODULE_ERR;
        }
        RedisModule_FreeString(ctx,serialized);

        /* We'll enqueue the job if the state is queued, unless Disque was
         * configured to never put jobs back on queue, for at-most-once jobs
         * safety guarantees under weak AOF settings. */
        int enqueue_job = 0;
        if (job->state == JOB_STATE_QUEUED) {
            if (ConfigLoadQueuedState) enqueue_job = 1;
            job->state = JOB_STATE_ACTIVE;
        }

	/* Check if the job expired before registering it. */
	if (job->etime <= now) {
	    freeJob(job);
	    continue;
	}

	/* Register the job, and if needed enqueue it: we put jobs back into
	 * queues only if enqueue-jobs-at-next-restart option is set, that is,
	 * when a controlled restart happens. */
	if (registerJob(job) == C_OK && enqueue_job)
	    enqueueJob(ctx,job,0);
    }
    return REDISMODULE_OK;
}

/* The LOADJOB command is emitted in the AOF to load serialized jobs at
 * restart, and is only processed while loading AOFs. Clients calling this
 * command get an error.
 *
 * This is basically the same as DisqueRDBAuxLoad(), but getting the
 * serialized job from the command. Unfortunately refactoring the two
 * in a single command would raise the complexity instead of lowering it. */
int loadjobCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);

    if (!RedisModule_IsAOFClient(RedisModule_GetClientId(ctx))) {
        return RedisModule_ReplyWithError(ctx,
            "ERR LOADJOB is a special command only "
            "processed from AOF");
    }

    size_t serlen;
    const char *serialized = RedisModule_StringPtrLen(argv[1],&serlen);

    job *job = deserializeJob(ctx,
        (unsigned char*)serialized,serlen,NULL,SER_STORAGE);
    if (job == NULL) {
        RedisModule_Log(ctx,"warning",
            "LOADJOB deserialization error while reading job from AOF");
    }

    /* We'll enqueue the job if the state is queued, unless Disque was
     * configured to never put jobs back on queue, for at-most-once jobs
     * safety guarantees under weak AOF settings. */
    int enqueue_job = 0;
    if (job->state == JOB_STATE_QUEUED) {
        if (ConfigLoadQueuedState) enqueue_job = 1;
        job->state = JOB_STATE_ACTIVE;
    }

    /* Check if the job expired before registering it. */
    if (job->etime <= time(NULL)) {
        freeJob(job);
        return RedisModule_ReplyWithSimpleString(ctx,"OK");
    }

    /* Register the job, and if needed enqueue it: we put jobs back into
     * queues only if enqueue-jobs-at-next-restart option is set, that is,
     * when a controlled restart happens. */
    if (registerJob(job) == C_OK && enqueue_job)
        enqueueJob(ctx,job,0);
    return RedisModule_ReplyWithSimpleString(ctx,"OK");
}
