/* Copyright (c) 2014-2019, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved. This code is under the AGPL license, check the
 * LICENSE file for more info. */

#include "disque.h"

/* AOF implementation is work in progress. */

void AOFLoadJob(job *job) { UNUSED(job); }
void AOFDelJob(job *job) { UNUSED(job); }
void AOFAckJob(job *job) { UNUSED(job); }

#if 0

/* ----------------------------------  AOF ---------------------------------- */

/* Emit a LOADJOB command into the AOF. which is used explicitly to load
 * serialized jobs form disk: LOADJOB <serialize-job-string>. */
void AOFLoadJob(job *job) {
    if (server.aof_state == AOF_OFF) return;

    sds serialized = serializeJob(sdsempty(),job,SER_STORAGE);
    RedisModuleString *seRedisModuleString = createObject(OBJ_STRING,serialized);
    RedisModuleString *argv[2] = {shared.loadjob, seRedisModuleString};
    feedAppendOnlyFile(argv,2);
    decrRefCount(seRedisModuleString);
}

/* Emit a DELJOB command into the AOF. This function is called in the following
 * two cases:
 *
 * 1) As a side effect of the job being acknowledged, when AOFAckJob()
 *    is called.
 * 2) When the server evicts a job from memory, but only if the state is one
 *    of active or queued. Yet not replicated jobs are not written into the
 *    AOF so there is no need to send a DELJOB, while already acknowledged
 *    jobs are handled by point "1". */
void AOFDelJob(job *job) {
    if (server.aof_state == AOF_OFF) return;

    RedisModuleString *jobid = createStringObject(job->id,JOB_ID_LEN);
    RedisModuleString *argv[2] = {shared.deljob, jobid};
    feedAppendOnlyFile(argv,2);
    decrRefCount(jobid);
}

/* Emit a DELJOB command, since this is how we handle acknowledged jobs from
 * the point of view of AOF. We are not interested in loading back acknowledged
 * jobs, nor we include them on AOF rewrites, since ACKs garbage collection
 * works anyway if nodes forget about ACKs and dropping ACKs is not a safety
 * violation, it may just result into multiple deliveries of the same
 * message.
 *
 * However we keep the API separated, so it will be simple if we change our
 * mind or we want to have a feature to persist ACKs. */
void AOFAckJob(job *job) {
    if (server.aof_state == AOF_OFF) return;
    AOFDelJob(job);
}

/* The LOADJOB command is emitted in the AOF to load serialized jobs at
 * restart, and is only processed while loading AOFs. Clients calling this
 * command get an error. */
void loadjobCommand(client *c) {
    if (!(c->flags & CLIENT_AOF_CLIENT)) {
        addReplyError(c,"LOADJOB is a special command only processed from AOF");
        return;
    }
    job *job = deserializeJob(ctx,c->argv[1]->ptr,sdslen(c->argv[1]->ptr),NULL,SER_STORAGE);

    /* We expect to be able to read back what we serialized. */
    if (job == NULL) {
        RedisModule_Log(ctx,"warning",
            "Unrecoverable error loading AOF: corrupted LOADJOB data.");
        exit(1);
    }

    int enqueue_job = 0;
    if (job->state == JOB_STATE_QUEUED) {
        if (server.aof_enqueue_jobs_once) enqueue_job = 1;
        job->state = JOB_STATE_ACTIVE;
    }

    /* Check if the job expired before registering it. */
    if (job->etime <= time(NULL)) {
        freeJob(job);
        return;
    }

    /* Register the job, and if needed enqueue it: we put jobs back into
     * queues only if enqueue-jobs-at-next-restart option is set, that is,
     * when a controlled restart happens. */
    if (registerJob(job) == C_OK && enqueue_job)
        enqueueJob(job,0);
}

#endif
