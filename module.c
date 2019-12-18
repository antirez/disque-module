/* Disque module entry point.
 *
 * -----------------------------------------------------------------------------
 *
 * Copyright (c) 2014-2019, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved. This code is under the AGPL license, check the
 * LICENSE file for more info. */

#define MAIN_MODULE_FILE
#define REDISMODULE_EXPERIMENTAL_API
#include "redismodule.h"
#include "disque.h"

rax *Jobs;                          /* Our dictionary of jobs, by ID. */
rax *Queues;                        /* Our queues by name. */
rax *BlockedClients;                /* 'bc' pointer -> list of queues. */
rax *BlockedOnRepl;                 /* Jobs waiting to reach the specified
                                       replication level. 'bc' -> job. */
int ClusterMyselfLeaving;           /* True if this node is leaving. */
int ClusterReachableNodesCount;     /* Number of reachable nodes. */
char **ClusterReachableNodes;       /* IDs of reachable nodes. */
rax *ClusterLeavingNodes;           /* Nodes that are leaving the cluster. */
unsigned char JobIDSeed[20];        /* Seed to generate random IDs. */
skiplist *AwakeList;                /* Job processing skiplist. */
RedisModuleType *DisqueModuleType;  /* The type we declare, even if we don't
                                       have any visible key in the Redis key
                                       space, in order to have the RDB aux
                                       data hooks during the AOF rewrite. */
int RedisConfigIsValid;             /* True if in the last check the Redis
                                       AOF configuration was found to be
                                       valid: AOF is on, RDB preamble for AOF
                                       is active. */

/* Options that you can configure when loading the module. */
int ConfigPersistDequeued = DISQUE_PERSIST_DEQUEUED_ALL;
int ConfigLoadQueuedState = 1; /* By default put jobs back in queue on
                                  restart. */

void initDisque(void) {
    Jobs = raxNew();
    Queues = raxNew();
    BlockedClients = raxNew();
    BlockedOnRepl = raxNew();
    ClusterLeavingNodes = raxNew();
    ClusterMyselfLeaving = 0;
    ClusterReachableNodesCount = 0;
    ClusterReachableNodes = NULL;
    RedisModule_GetRandomBytes(JobIDSeed,sizeof(JobIDSeed));
    AwakeList = skiplistCreate(skiplistCompareJobsToAwake);
    RedisConfigIsValid = 0;
}

/* Check if Redis is correctly configured for Disque and sets the
 * RedisConfigIsValid global variable accordingly. */
void disqueVerifyRedisConfigValidity(RedisModuleCtx *ctx) {
    int errors = 0;
    const char *configs[3] = {"appendonly",
                              "aof-use-rdb-preamble",
                              "cluster-enabled"};

    /* AOF and RDB AOF preamble must be on. */
    for (int j = 0; j < 3; j++) {
        RedisModuleCallReply *reply, *yesnoreply;
        const char *str;
        size_t strlen;

        reply = RedisModule_Call(ctx,"CONFIG","cc","GET",configs[j]);
        if (RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY) {
            yesnoreply = RedisModule_CallReplyArrayElement(reply,1);
            if (RedisModule_CallReplyType(yesnoreply) ==
                REDISMODULE_REPLY_STRING)
            {
                str = RedisModule_CallReplyStringPtr(yesnoreply,&strlen);
                if (strlen != 3 || memcmp(str,"yes",3) != 0) errors++;
            } else {
                errors++;
            }
        } else {
            errors++;
        }
        RedisModule_FreeCallReply(reply);
    }

    /* We are fine if no errors were detected. */
    RedisConfigIsValid = (errors == 0);
}

/* Return 0 if the instance has no memory issues.
 * Return 1 if less than 25% of memory is still available.
 * Return 2 if we are over the "maxmemory" limit. */
int getMemoryWarningLevel(RedisModuleCtx *ctx) {
    int flags = RedisModule_GetContextFlags(ctx);
    if (flags & REDISMODULE_CTX_FLAGS_OOM) return 2;
    else if (flags & REDISMODULE_CTX_FLAGS_OOM_WARNING) return 1;
    else return 0;
}

/* This is called every second. */
void disqueCron(RedisModuleCtx *ctx, void *data) {
    REDISMODULE_NOT_USED(data);

    /* Refresh the list of reachable nodes. */
    refreshReachableNodes(ctx);

    /* Purge "leaving" entries not refreshed for more than one minute. */
    /* XXX: TODO, we yet don't propagate the leaving flag. */

    /* Evict idle queues without jobs. */
    evictIdleQueues(ctx);

    /* Check if other nodes have jobs about queues we have clients blocked
     * for. */
    clientsCronSendNeedJobs(ctx);

    /* Setup the timer for the next call. */
    RedisModule_CreateTimer(ctx,1000,disqueCron,NULL);

    /* Check the configuration validity periodically since the user may
     * reconfigure Redis on the fly via "CONFIG SET". */
    disqueVerifyRedisConfigValidity(ctx);
}

/* Return true if we are in "leaving" state. */
int myselfLeaving(void) {
    return ClusterMyselfLeaving;
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx,"Disque",1,REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"ADDJOB",
        addjobCommand,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"SHOW",
        showCommand,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"GETJOB",
        getjobCommand,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"ACKJOB",
        ackjobCommand,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"FASTACK",
        fastackCommand,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"DELJOB",
        deljobCommand,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"QLEN",
        qlenCommand,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"ENQUEUE",
        enqueueCommand,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"DEQUEUE",
        dequeueCommand,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"NACK",
        nackCommand,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"QPEEK",
        qpeekCommand,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"WORKING",
        workingCommand,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"QSTAT",
        qstatCommand,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"PAUSE",
        pauseCommand,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"DISQUE",
        disqueCommand,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"LOADJOB",
        loadjobCommand,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    /* Disable Redis Cluster sharding and redirections: Not really needed
     * since Disque does not use the keyspace at all, but people may want to
     * use the Redis instances where Disque is running for caching or alike. */
    RedisModule_SetClusterFlags(ctx,REDISMODULE_CLUSTER_FLAG_NO_REDIRECTION);

    /* Register our handlers for different message types. */
    RedisModule_RegisterClusterMessageReceiver(ctx,DISQUE_MSG_REPLJOB,REPLJOBcallback);
    RedisModule_RegisterClusterMessageReceiver(ctx,DISQUE_MSG_YOURJOBS,YOURJOBScallback);
    RedisModule_RegisterClusterMessageReceiver(ctx,DISQUE_MSG_GOTJOB,GOTJOBcallback);
    RedisModule_RegisterClusterMessageReceiver(ctx,DISQUE_MSG_SETACK,SETACKcallback);
    RedisModule_RegisterClusterMessageReceiver(ctx,DISQUE_MSG_GOTACK,GOTACKcallback);
    RedisModule_RegisterClusterMessageReceiver(ctx,DISQUE_MSG_WORKING,WORKINGcallback);
    RedisModule_RegisterClusterMessageReceiver(ctx,DISQUE_MSG_QUEUED,QUEUEDcallback);
    RedisModule_RegisterClusterMessageReceiver(ctx,DISQUE_MSG_WILLQUEUE,WILLQUEUEcallback);
    RedisModule_RegisterClusterMessageReceiver(ctx,DISQUE_MSG_NEEDJOBS,NEEDJOBScallback);
    RedisModule_RegisterClusterMessageReceiver(ctx,DISQUE_MSG_PAUSE,PAUSEcallback);
    RedisModule_RegisterClusterMessageReceiver(ctx,DISQUE_MSG_DELJOB,DELJOBcallback);

    /* Register the Disque type. */
    RedisModule_SetModuleOptions(ctx, REDISMODULE_OPTIONS_HANDLE_IO_ERRORS);

    RedisModuleTypeMethods tm = {
        .version = REDISMODULE_TYPE_METHOD_VERSION,
        .aux_load = DisqueRDBAuxLoad,
        .aux_save = DisqueRDBAuxSave,
        .aux_save_triggers = REDISMODULE_AUX_AFTER_RDB
    };
    DisqueModuleType = RedisModule_CreateDataType(ctx,"disque-az",1,&tm);
    if (DisqueModuleType == NULL) {
        RedisModule_Log(ctx,"warning",
            "Disque failed to register the module data type");
        return REDISMODULE_ERR;
    }

    /* Check if the Redis config is ok. */
    disqueVerifyRedisConfigValidity(ctx);
    if (!RedisConfigIsValid) {
        RedisModule_Log(ctx,"warning",
            "Disque detected that Redis is misconfigured, make sure that: "
            "Cluster mode is enabled, AOF is on, and the AOF RDB preamble is "
            "enabled.");
        return REDISMODULE_ERR;
    }

    /* Start Disque. */
    initDisque();
    disqueCron(ctx,NULL);
    processJobs(ctx,NULL);
    return REDISMODULE_OK;
}
