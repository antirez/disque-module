/* Disque Command implementation.
 *
 * Copyright (c) 2019, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved. This code is under the AGPL license, check the
 * LICENSE file for more info. */

#include "disque.h"

#define DISQUE_INFO_TYPE_INT            0
#define DISQUE_INFO_TYPE_UINT           1
#define DISQUE_INFO_TYPE_LONG           2
#define DISQUE_INFO_TYPE_ULONG          3
#define DISQUE_INFO_TYPE_LONGLONG       4
#define DISQUE_INFO_TYPE_ULONGLONG      5

/* INFO getter function for Rax size. */
void disqueInfoGetRaxSize(void *aux, void *valueptr, size_t *lenptr) {
    UNUSED(lenptr);
    unsigned long long val = raxSize(*((rax**)aux));
    memcpy(valueptr,&val,sizeof(val));
}

struct disqueInfoProperty {
    char *name;
    int type;
    void *valueptr;
    void (*getvalue)(void *aux, void *valueptr, size_t *lenptr);
    void *aux;
} DisqueInfoPropertiesTable[] = {
    /* cluster.* */
    {"cluster.nodes.reachable",
        DISQUE_INFO_TYPE_INT,
        &ClusterReachableNodesCount,NULL,NULL},

    /* jobs.* */
    {"jobs.registered",
        DISQUE_INFO_TYPE_ULONGLONG,
        NULL,disqueInfoGetRaxSize,&Jobs},

    /* queues.* */
    {"queues.registered",
        DISQUE_INFO_TYPE_ULONGLONG,
        NULL,disqueInfoGetRaxSize,&Queues},

    /* Final terminator. */
    {NULL,0,NULL,NULL,NULL}
};

/* DISQUE INFO [option name] */
int disqueInfo(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2 & argc != 3) return RedisModule_WrongArity(ctx);
    int allfields = 0, numfields = 0;
    const char *wanted = NULL;

    /* If no info property name is given, reply with a human readable string.
     * Otherwise just provide the final value of the specified info
     * property. */
    if (argc == 3) {
        wanted = RedisModule_StringPtrLen(argv[2],NULL);
    } else {
        allfields = 1;
        RedisModule_ReplyWithArray(ctx,REDISMODULE_POSTPONED_ARRAY_LEN);
    }

    /* Scan all the options: either search a match in case of single option
     * specified, or accumulate all the data in a string. */
    for (int j = 0; DisqueInfoPropertiesTable[j].name; j++) {
        /* Fetch the info. */
        struct disqueInfoProperty *dp = DisqueInfoPropertiesTable+j;
        int intval;
        unsigned long long ulonglongval;

        if (dp->type == DISQUE_INFO_TYPE_INT) {
            if (dp->getvalue) {
                dp->getvalue(dp->aux,&intval,NULL);
            } else {
                intval = *(int*)(dp->valueptr);
            }
        } else if (dp->type == DISQUE_INFO_TYPE_ULONGLONG) {
            if (dp->getvalue) {
                dp->getvalue(dp->aux,&ulonglongval,NULL);
            } else {
                ulonglongval = *(unsigned long long*)(dp->valueptr);
            }
        }


        /* Either emit every entry as an array of strings or, in case the
         * user specified a single property, emit it if there is a match. */
        if (allfields) {
            sds output = sdsempty();
            if (dp->type == DISQUE_INFO_TYPE_INT)
                output = sdscatprintf(output,"%s:%d",dp->name,intval);
            else if (dp->type == DISQUE_INFO_TYPE_ULONGLONG)
                output = sdscatprintf(output,"%s:%llu",dp->name,ulonglongval);
            RedisModule_ReplyWithStringBuffer(ctx,output,sdslen(output));
            sdsfree(output);
            numfields++;
        } else if (!strcasecmp(wanted,dp->name)) {
            if (dp->type == DISQUE_INFO_TYPE_INT)
                return RedisModule_ReplyWithLongLong(ctx,intval);
            else if (dp->type == DISQUE_INFO_TYPE_ULONGLONG)
                return RedisModule_ReplyWithLongLong(ctx,ulonglongval);
        }
    }

    /* Emit the arrays of strings or return NULL if we got no match in case the
     * caller was looking for a specific property value. */
    if (allfields) {
        RedisModule_ReplySetArrayLength(ctx,numfields);
    } else {
        RedisModule_ReplyWithNull(ctx);
    }
    return REDISMODULE_OK;
}

/* The DISQUE command implements all the disque specific features. */
int disqueCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 2) return RedisModule_WrongArity(ctx);
    const char *opt = RedisModule_StringPtrLen(argv[1],NULL);

    if (!strcasecmp(opt,"info")) {
        return disqueInfo(ctx,argv,argc);
    } else if (!strcasecmp(opt,"flushall")) {
        flushAllJobsAndQueues(ctx);
        RedisModule_ReplyWithSimpleString(ctx,"OK");
    } else {
        RedisModule_ReplyWithError(ctx,"ERR Unknown DISQUE subcommand");
    }
    return REDISMODULE_OK;
}
