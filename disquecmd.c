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

struct disqueInfoProperty {
    char *name;
    int type;
    void *valueptr;
    void (*getvalue)(void *valeptr, size_t *lenptr);
} DisqueInfoPropertiesTable[] = {
    {"cluster.nodes.reachable",
        DISQUE_INFO_TYPE_INT,
        &ClusterReachableNodesCount,
        NULL},
    /* Terminator. */
    {NULL,0,NULL,NULL}
};

/* DISQUE INFO [option name] */
int disqueInfo(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2 & argc != 3) return RedisModule_WrongArity(ctx);
    int stringout = 0;
    sds output = NULL;
    const char *wanted = NULL;

    /* If no info property name is given, reply with a human readable string.
     * Otherwise just provide the final value of the specified info
     * property. */
    if (argc == 3) {
        wanted = RedisModule_StringPtrLen(argv[2],NULL);
    } else {
        stringout = 1;
        output = sdsempty();
    }

    /* Scan all the options: either search a match in case of single option
     * specified, or accumulate all the data in a string. */
    for (int j = 0; DisqueInfoPropertiesTable[j].name; j++) {
        /* Fetch the info. */
        struct disqueInfoProperty *dp = DisqueInfoPropertiesTable+j;
        int intval;

        if (dp->type == DISQUE_INFO_TYPE_INT) {
            if (dp->getvalue) {
                dp->getvalue(&intval,NULL);
            } else {
                intval = *(int*)(dp->valueptr);
            }
        }

        /* Either accumulate it as a string or emit it if there is
         * a match. */
        if (stringout) {
            if (dp->type == DISQUE_INFO_TYPE_INT)
                output = sdscatprintf(output,"%s:%d\n",dp->name,intval);
        } else if (!strcasecmp(wanted,dp->name)) {
            if (dp->type == DISQUE_INFO_TYPE_INT)
                return RedisModule_ReplyWithLongLong(ctx,intval);
        }
    }

    /* Emit the string or return NULL if we got no match in case the
     * caller was looking for a specific property value. */
    if (stringout) {
        RedisModule_ReplyWithVerbatimString(ctx,output,sdslen(output));
        sdsfree(output);
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
