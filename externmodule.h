/* This file implements the extern prototypes of the function pointers for
 * the Redis module system, in order to have the module APIs defined in
 * different C files. It should go away once we have a better redismodule.h
 * implementation. */

#include <stdint.h>

typedef uint64_t RedisModuleTimerID;
typedef struct RedisModuleCtx RedisModuleCtx;
typedef struct RedisModuleKey RedisModuleKey;
typedef struct RedisModuleString RedisModuleString;
typedef struct RedisModuleCallReply RedisModuleCallReply;
typedef struct RedisModuleIO RedisModuleIO;
typedef struct RedisModuleType RedisModuleType;
typedef struct RedisModuleDigest RedisModuleDigest;
typedef struct RedisModuleBlockedClient RedisModuleBlockedClient;
typedef struct RedisModuleClusterInfo RedisModuleClusterInfo;
typedef struct RedisModuleDict RedisModuleDict;
typedef struct RedisModuleDictIter RedisModuleDictIter;
typedef struct RedisModuleCommandFilterCtx RedisModuleCommandFilterCtx;
typedef struct RedisModuleCommandFilter RedisModuleCommandFilter;
typedef struct RedisModuleInfoCtx RedisModuleInfoCtx;
typedef struct RedisModuleServerInfoData RedisModuleServerInfoData;
typedef struct RedisModuleScanCursor RedisModuleScanCursor;

typedef int (*RedisModuleCmdFunc)(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
typedef void (*RedisModuleDisconnectFunc)(RedisModuleCtx *ctx, RedisModuleBlockedClient *bc);
typedef int (*RedisModuleNotificationFunc)(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key);
typedef void *(*RedisModuleTypeLoadFunc)(RedisModuleIO *rdb, int encver);
typedef void (*RedisModuleTypeSaveFunc)(RedisModuleIO *rdb, void *value);
typedef int (*RedisModuleTypeAuxLoadFunc)(RedisModuleIO *rdb, int encver, int when);
typedef void (*RedisModuleTypeAuxSaveFunc)(RedisModuleIO *rdb, int when);
typedef void (*RedisModuleTypeRewriteFunc)(RedisModuleIO *aof, RedisModuleString *key, void *value);
typedef size_t (*RedisModuleTypeMemUsageFunc)(const void *value);
typedef void (*RedisModuleTypeDigestFunc)(RedisModuleDigest *digest, void *value);
typedef void (*RedisModuleTypeFreeFunc)(void *value);
typedef void (*RedisModuleClusterMessageReceiver)(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len);
typedef void (*RedisModuleTimerProc)(RedisModuleCtx *ctx, void *data);
typedef void (*RedisModuleCommandFilterFunc) (RedisModuleCommandFilterCtx *filter);
typedef void (*RedisModuleForkDoneHandler) (int exitcode, int bysignal, void *user_data);
typedef void (*RedisModuleInfoFunc)(RedisModuleInfoCtx *ctx, int for_crash_report);
typedef void (*RedisModuleScanCB)(RedisModuleCtx *ctx, RedisModuleString *keyname, RedisModuleKey *key, void *privdata);
typedef void (*RedisModuleScanKeyCB)(RedisModuleKey *key, RedisModuleString *field, RedisModuleString *value, void *privdata);

#define REDISMODULE_TYPE_METHOD_VERSION 2
typedef struct RedisModuleTypeMethods RedisModuleTypeMethods;
typedef struct RedisModuleEvent RedisModuleEvent;

struct RedisModuleCtx;
typedef void (*RedisModuleEventCallback)(struct RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data);

extern void *(*RedisModule_Alloc)(size_t bytes);
extern void *(*RedisModule_Realloc)(void *ptr, size_t bytes);
extern void (*RedisModule_Free)(void *ptr);
extern void *(*RedisModule_Calloc)(size_t nmemb, size_t size);
extern char *(*RedisModule_Strdup)(const char *str);
extern int (*RedisModule_GetApi)(const char *, void *);
extern int (*RedisModule_CreateCommand)(RedisModuleCtx *ctx, const char *name, RedisModuleCmdFunc cmdfunc, const char *strflags, int firstkey, int lastkey, int keystep);
extern void (*RedisModule_SetModuleAttribs)(RedisModuleCtx *ctx, const char *name, int ver, int apiver);
extern int (*RedisModule_IsModuleNameBusy)(const char *name);
extern int (*RedisModule_WrongArity)(RedisModuleCtx *ctx);
extern int (*RedisModule_ReplyWithLongLong)(RedisModuleCtx *ctx, long long ll);
extern int (*RedisModule_GetSelectedDb)(RedisModuleCtx *ctx);
extern int (*RedisModule_SelectDb)(RedisModuleCtx *ctx, int newid);
extern void *(*RedisModule_OpenKey)(RedisModuleCtx *ctx, RedisModuleString *keyname, int mode);
extern void (*RedisModule_CloseKey)(RedisModuleKey *kp);
extern int (*RedisModule_KeyType)(RedisModuleKey *kp);
extern size_t (*RedisModule_ValueLength)(RedisModuleKey *kp);
extern int (*RedisModule_ListPush)(RedisModuleKey *kp, int where, RedisModuleString *ele);
extern RedisModuleString *(*RedisModule_ListPop)(RedisModuleKey *key, int where);
extern RedisModuleCallReply *(*RedisModule_Call)(RedisModuleCtx *ctx, const char *cmdname, const char *fmt, ...);
extern const char *(*RedisModule_CallReplyProto)(RedisModuleCallReply *reply, size_t *len);
extern void (*RedisModule_FreeCallReply)(RedisModuleCallReply *reply);
extern int (*RedisModule_CallReplyType)(RedisModuleCallReply *reply);
extern long long (*RedisModule_CallReplyInteger)(RedisModuleCallReply *reply);
extern size_t (*RedisModule_CallReplyLength)(RedisModuleCallReply *reply);
extern RedisModuleCallReply *(*RedisModule_CallReplyArrayElement)(RedisModuleCallReply *reply, size_t idx);
extern RedisModuleString *(*RedisModule_CreateString)(RedisModuleCtx *ctx, const char *ptr, size_t len);
extern RedisModuleString *(*RedisModule_CreateStringFromLongLong)(RedisModuleCtx *ctx, long long ll);
extern RedisModuleString *(*RedisModule_CreateStringFromLongDouble)(RedisModuleCtx *ctx, long double ld, int humanfriendly);
extern RedisModuleString *(*RedisModule_CreateStringFromString)(RedisModuleCtx *ctx, const RedisModuleString *str);
extern RedisModuleString *(*RedisModule_CreateStringPrintf)(RedisModuleCtx *ctx, const char *fmt, ...);
extern void (*RedisModule_FreeString)(RedisModuleCtx *ctx, RedisModuleString *str);
extern const char *(*RedisModule_StringPtrLen)(const RedisModuleString *str, size_t *len);
extern int (*RedisModule_ReplyWithError)(RedisModuleCtx *ctx, const char *err);
extern int (*RedisModule_ReplyWithSimpleString)(RedisModuleCtx *ctx, const char *msg);
extern int (*RedisModule_ReplyWithArray)(RedisModuleCtx *ctx, long len);
extern int (*RedisModule_ReplyWithNullArray)(RedisModuleCtx *ctx);
extern int (*RedisModule_ReplyWithEmptyArray)(RedisModuleCtx *ctx);
extern void (*RedisModule_ReplySetArrayLength)(RedisModuleCtx *ctx, long len);
extern int (*RedisModule_ReplyWithStringBuffer)(RedisModuleCtx *ctx, const char *buf, size_t len);
extern int (*RedisModule_ReplyWithCString)(RedisModuleCtx *ctx, const char *buf);
extern int (*RedisModule_ReplyWithString)(RedisModuleCtx *ctx, RedisModuleString *str);
extern int (*RedisModule_ReplyWithEmptyString)(RedisModuleCtx *ctx);
extern int (*RedisModule_ReplyWithVerbatimString)(RedisModuleCtx *ctx, const char *buf, size_t len);
extern int (*RedisModule_ReplyWithNull)(RedisModuleCtx *ctx);
extern int (*RedisModule_ReplyWithDouble)(RedisModuleCtx *ctx, double d);
extern int (*RedisModule_ReplyWithLongDouble)(RedisModuleCtx *ctx, long double d);
extern int (*RedisModule_ReplyWithCallReply)(RedisModuleCtx *ctx, RedisModuleCallReply *reply);
extern int (*RedisModule_StringToLongLong)(const RedisModuleString *str, long long *ll);
extern int (*RedisModule_StringToDouble)(const RedisModuleString *str, double *d);
extern int (*RedisModule_StringToLongDouble)(const RedisModuleString *str, long double *d);
extern void (*RedisModule_AutoMemory)(RedisModuleCtx *ctx);
extern int (*RedisModule_Replicate)(RedisModuleCtx *ctx, const char *cmdname, const char *fmt, ...);
extern int (*RedisModule_ReplicateVerbatim)(RedisModuleCtx *ctx);
extern const char *(*RedisModule_CallReplyStringPtr)(RedisModuleCallReply *reply, size_t *len);
extern RedisModuleString *(*RedisModule_CreateStringFromCallReply)(RedisModuleCallReply *reply);
extern int (*RedisModule_DeleteKey)(RedisModuleKey *key);
extern int (*RedisModule_UnlinkKey)(RedisModuleKey *key);
extern int (*RedisModule_StringSet)(RedisModuleKey *key, RedisModuleString *str);
extern char *(*RedisModule_StringDMA)(RedisModuleKey *key, size_t *len, int mode);
extern int (*RedisModule_StringTruncate)(RedisModuleKey *key, size_t newlen);
extern mstime_t (*RedisModule_GetExpire)(RedisModuleKey *key);
extern int (*RedisModule_SetExpire)(RedisModuleKey *key, mstime_t expire);
extern void (*RedisModule_ResetDataset)(int restart_aof, int async);
extern unsigned long long (*RedisModule_DbSize)(RedisModuleCtx *ctx);
extern RedisModuleString *(*RedisModule_RandomKey)(RedisModuleCtx *ctx);
extern int (*RedisModule_ZsetAdd)(RedisModuleKey *key, double score, RedisModuleString *ele, int *flagsptr);
extern int (*RedisModule_ZsetIncrby)(RedisModuleKey *key, double score, RedisModuleString *ele, int *flagsptr, double *newscore);
extern int (*RedisModule_ZsetScore)(RedisModuleKey *key, RedisModuleString *ele, double *score);
extern int (*RedisModule_ZsetRem)(RedisModuleKey *key, RedisModuleString *ele, int *deleted);
extern void (*RedisModule_ZsetRangeStop)(RedisModuleKey *key);
extern int (*RedisModule_ZsetFirstInScoreRange)(RedisModuleKey *key, double min, double max, int minex, int maxex);
extern int (*RedisModule_ZsetLastInScoreRange)(RedisModuleKey *key, double min, double max, int minex, int maxex);
extern int (*RedisModule_ZsetFirstInLexRange)(RedisModuleKey *key, RedisModuleString *min, RedisModuleString *max);
extern int (*RedisModule_ZsetLastInLexRange)(RedisModuleKey *key, RedisModuleString *min, RedisModuleString *max);
extern RedisModuleString *(*RedisModule_ZsetRangeCurrentElement)(RedisModuleKey *key, double *score);
extern int (*RedisModule_ZsetRangeNext)(RedisModuleKey *key);
extern int (*RedisModule_ZsetRangePrev)(RedisModuleKey *key);
extern int (*RedisModule_ZsetRangeEndReached)(RedisModuleKey *key);
extern int (*RedisModule_HashSet)(RedisModuleKey *key, int flags, ...);
extern int (*RedisModule_HashGet)(RedisModuleKey *key, int flags, ...);
extern int (*RedisModule_IsKeysPositionRequest)(RedisModuleCtx *ctx);
extern void (*RedisModule_KeyAtPos)(RedisModuleCtx *ctx, int pos);
extern unsigned long long (*RedisModule_GetClientId)(RedisModuleCtx *ctx);
extern int (*RedisModule_GetClientInfoById)(void *ci, uint64_t id);
extern int (*RedisModule_PublishMessage)(RedisModuleCtx *ctx, RedisModuleString *channel, RedisModuleString *message);
extern int (*RedisModule_GetContextFlags)(RedisModuleCtx *ctx);
extern void *(*RedisModule_PoolAlloc)(RedisModuleCtx *ctx, size_t bytes);
extern RedisModuleType *(*RedisModule_CreateDataType)(RedisModuleCtx *ctx, const char *name, int encver, RedisModuleTypeMethods *typemethods);
extern int (*RedisModule_ModuleTypeSetValue)(RedisModuleKey *key, RedisModuleType *mt, void *value);
extern void *(*RedisModule_ModuleTypeReplaceValue)(RedisModuleKey *key, RedisModuleType *mt, void *new_value);
extern RedisModuleType *(*RedisModule_ModuleTypeGetType)(RedisModuleKey *key);
extern void *(*RedisModule_ModuleTypeGetValue)(RedisModuleKey *key);
extern int (*RedisModule_IsIOError)(RedisModuleIO *io);
extern void (*RedisModule_SetModuleOptions)(RedisModuleCtx *ctx, int options);
extern int (*RedisModule_SignalModifiedKey)(RedisModuleCtx *ctx, RedisModuleString *keyname);
extern void (*RedisModule_SaveUnsigned)(RedisModuleIO *io, uint64_t value);
extern uint64_t (*RedisModule_LoadUnsigned)(RedisModuleIO *io);
extern void (*RedisModule_SaveSigned)(RedisModuleIO *io, int64_t value);
extern int64_t (*RedisModule_LoadSigned)(RedisModuleIO *io);
extern void (*RedisModule_EmitAOF)(RedisModuleIO *io, const char *cmdname, const char *fmt, ...);
extern void (*RedisModule_SaveString)(RedisModuleIO *io, RedisModuleString *s);
extern void (*RedisModule_SaveStringBuffer)(RedisModuleIO *io, const char *str, size_t len);
extern RedisModuleString *(*RedisModule_LoadString)(RedisModuleIO *io);
extern char *(*RedisModule_LoadStringBuffer)(RedisModuleIO *io, size_t *lenptr);
extern void (*RedisModule_SaveDouble)(RedisModuleIO *io, double value);
extern double (*RedisModule_LoadDouble)(RedisModuleIO *io);
extern void (*RedisModule_SaveFloat)(RedisModuleIO *io, float value);
extern float (*RedisModule_LoadFloat)(RedisModuleIO *io);
extern void (*RedisModule_SaveLongDouble)(RedisModuleIO *io, long double value);
extern long double (*RedisModule_LoadLongDouble)(RedisModuleIO *io);
extern void *(*RedisModule_LoadDataTypeFromString)(const RedisModuleString *str, const RedisModuleType *mt);
extern RedisModuleString *(*RedisModule_SaveDataTypeToString)(RedisModuleCtx *ctx, void *data, const RedisModuleType *mt);
extern void (*RedisModule_Log)(RedisModuleCtx *ctx, const char *level, const char *fmt, ...);
extern void (*RedisModule_LogIOError)(RedisModuleIO *io, const char *levelstr, const char *fmt, ...);
extern void (*RedisModule__Assert)(const char *estr, const char *file, int line);
extern void (*RedisModule_LatencyAddSample)(const char *event, mstime_t latency);
extern int (*RedisModule_StringAppendBuffer)(RedisModuleCtx *ctx, RedisModuleString *str, const char *buf, size_t len);
extern void (*RedisModule_RetainString)(RedisModuleCtx *ctx, RedisModuleString *str);
extern int (*RedisModule_StringCompare)(RedisModuleString *a, RedisModuleString *b);
extern RedisModuleCtx *(*RedisModule_GetContextFromIO)(RedisModuleIO *io);
extern const RedisModuleString *(*RedisModule_GetKeyNameFromIO)(RedisModuleIO *io);
extern const RedisModuleString *(*RedisModule_GetKeyNameFromModuleKey)(RedisModuleKey *key);
extern long long (*RedisModule_Milliseconds)(void);
extern void (*RedisModule_DigestAddStringBuffer)(RedisModuleDigest *md, unsigned char *ele, size_t len);
extern void (*RedisModule_DigestAddLongLong)(RedisModuleDigest *md, long long ele);
extern void (*RedisModule_DigestEndSequence)(RedisModuleDigest *md);
extern RedisModuleDict *(*RedisModule_CreateDict)(RedisModuleCtx *ctx);
extern void (*RedisModule_FreeDict)(RedisModuleCtx *ctx, RedisModuleDict *d);
extern uint64_t (*RedisModule_DictSize)(RedisModuleDict *d);
extern int (*RedisModule_DictSetC)(RedisModuleDict *d, void *key, size_t keylen, void *ptr);
extern int (*RedisModule_DictReplaceC)(RedisModuleDict *d, void *key, size_t keylen, void *ptr);
extern int (*RedisModule_DictSet)(RedisModuleDict *d, RedisModuleString *key, void *ptr);
extern int (*RedisModule_DictReplace)(RedisModuleDict *d, RedisModuleString *key, void *ptr);
extern void *(*RedisModule_DictGetC)(RedisModuleDict *d, void *key, size_t keylen, int *nokey);
extern void *(*RedisModule_DictGet)(RedisModuleDict *d, RedisModuleString *key, int *nokey);
extern int (*RedisModule_DictDelC)(RedisModuleDict *d, void *key, size_t keylen, void *oldval);
extern int (*RedisModule_DictDel)(RedisModuleDict *d, RedisModuleString *key, void *oldval);
extern RedisModuleDictIter *(*RedisModule_DictIteratorStartC)(RedisModuleDict *d, const char *op, void *key, size_t keylen);
extern RedisModuleDictIter *(*RedisModule_DictIteratorStart)(RedisModuleDict *d, const char *op, RedisModuleString *key);
extern void (*RedisModule_DictIteratorStop)(RedisModuleDictIter *di);
extern int (*RedisModule_DictIteratorReseekC)(RedisModuleDictIter *di, const char *op, void *key, size_t keylen);
extern int (*RedisModule_DictIteratorReseek)(RedisModuleDictIter *di, const char *op, RedisModuleString *key);
extern void *(*RedisModule_DictNextC)(RedisModuleDictIter *di, size_t *keylen, void **dataptr);
extern void *(*RedisModule_DictPrevC)(RedisModuleDictIter *di, size_t *keylen, void **dataptr);
extern RedisModuleString *(*RedisModule_DictNext)(RedisModuleCtx *ctx, RedisModuleDictIter *di, void **dataptr);
extern RedisModuleString *(*RedisModule_DictPrev)(RedisModuleCtx *ctx, RedisModuleDictIter *di, void **dataptr);
extern int (*RedisModule_DictCompareC)(RedisModuleDictIter *di, const char *op, void *key, size_t keylen);
extern int (*RedisModule_DictCompare)(RedisModuleDictIter *di, const char *op, RedisModuleString *key);
extern int (*RedisModule_RegisterInfoFunc)(RedisModuleCtx *ctx, RedisModuleInfoFunc cb);
extern int (*RedisModule_InfoAddSection)(RedisModuleInfoCtx *ctx, char *name);
extern int (*RedisModule_InfoBeginDictField)(RedisModuleInfoCtx *ctx, char *name);
extern int (*RedisModule_InfoEndDictField)(RedisModuleInfoCtx *ctx);
extern int (*RedisModule_InfoAddFieldString)(RedisModuleInfoCtx *ctx, char *field, RedisModuleString *value);
extern int (*RedisModule_InfoAddFieldCString)(RedisModuleInfoCtx *ctx, char *field, char *value);
extern int (*RedisModule_InfoAddFieldDouble)(RedisModuleInfoCtx *ctx, char *field, double value);
extern int (*RedisModule_InfoAddFieldLongLong)(RedisModuleInfoCtx *ctx, char *field, long long value);
extern int (*RedisModule_InfoAddFieldULongLong)(RedisModuleInfoCtx *ctx, char *field, unsigned long long value);
extern RedisModuleServerInfoData *(*RedisModule_GetServerInfo)(RedisModuleCtx *ctx, const char *section);
extern void (*RedisModule_FreeServerInfo)(RedisModuleCtx *ctx, RedisModuleServerInfoData *data);
extern RedisModuleString *(*RedisModule_ServerInfoGetField)(RedisModuleCtx *ctx, RedisModuleServerInfoData *data, const char* field);
extern const char *(*RedisModule_ServerInfoGetFieldC)(RedisModuleServerInfoData *data, const char* field);
extern long long (*RedisModule_ServerInfoGetFieldSigned)(RedisModuleServerInfoData *data, const char* field, int *out_err);
extern unsigned long long (*RedisModule_ServerInfoGetFieldUnsigned)(RedisModuleServerInfoData *data, const char* field, int *out_err);
extern double (*RedisModule_ServerInfoGetFieldDouble)(RedisModuleServerInfoData *data, const char* field, int *out_err);
extern int (*RedisModule_SubscribeToServerEvent)(RedisModuleCtx *ctx, RedisModuleEvent event, RedisModuleEventCallback callback);
extern int (*RedisModule_SetLRU)(RedisModuleKey *key, mstime_t lru_idle);
extern int (*RedisModule_GetLRU)(RedisModuleKey *key, mstime_t *lru_idle);
extern int (*RedisModule_SetLFU)(RedisModuleKey *key, long long lfu_freq);
extern int (*RedisModule_GetLFU)(RedisModuleKey *key, long long *lfu_freq);
extern RedisModuleBlockedClient *(*RedisModule_BlockClientOnKeys)(RedisModuleCtx *ctx, RedisModuleCmdFunc reply_callback, RedisModuleCmdFunc timeout_callback, void (*free_privdata)(RedisModuleCtx*,void*), long long timeout_ms, RedisModuleString **keys, int numkeys, void *privdata);
extern void (*RedisModule_SignalKeyAsReady)(RedisModuleCtx *ctx, RedisModuleString *key);
extern RedisModuleString *(*RedisModule_GetBlockedClientReadyKey)(RedisModuleCtx *ctx);
extern RedisModuleScanCursor *(*RedisModule_ScanCursorCreate)();
extern void (*RedisModule_ScanCursorRestart)(RedisModuleScanCursor *cursor);
extern void (*RedisModule_ScanCursorDestroy)(RedisModuleScanCursor *cursor);
extern int (*RedisModule_Scan)(RedisModuleCtx *ctx, RedisModuleScanCursor *cursor, RedisModuleScanCB fn, void *privdata);
extern int (*RedisModule_ScanKey)(RedisModuleKey *key, RedisModuleScanCursor *cursor, RedisModuleScanKeyCB fn, void *privdata);
extern RedisModuleBlockedClient *(*RedisModule_BlockClient)(RedisModuleCtx *ctx, RedisModuleCmdFunc reply_callback, RedisModuleCmdFunc timeout_callback, void (*free_privdata)(RedisModuleCtx*,void*), long long timeout_ms);
extern int (*RedisModule_UnblockClient)(RedisModuleBlockedClient *bc, void *privdata);
extern int (*RedisModule_IsBlockedReplyRequest)(RedisModuleCtx *ctx);
extern int (*RedisModule_IsBlockedTimeoutRequest)(RedisModuleCtx *ctx);
extern void *(*RedisModule_GetBlockedClientPrivateData)(RedisModuleCtx *ctx);
extern RedisModuleBlockedClient *(*RedisModule_GetBlockedClientHandle)(RedisModuleCtx *ctx);
extern int (*RedisModule_AbortBlock)(RedisModuleBlockedClient *bc);
extern RedisModuleCtx *(*RedisModule_GetThreadSafeContext)(RedisModuleBlockedClient *bc);
extern void (*RedisModule_FreeThreadSafeContext)(RedisModuleCtx *ctx);
extern void (*RedisModule_ThreadSafeContextLock)(RedisModuleCtx *ctx);
extern void (*RedisModule_ThreadSafeContextUnlock)(RedisModuleCtx *ctx);
extern int (*RedisModule_SubscribeToKeyspaceEvents)(RedisModuleCtx *ctx, int types, RedisModuleNotificationFunc cb);
extern int (*RedisModule_NotifyKeyspaceEvent)(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key);
extern int (*RedisModule_GetNotifyKeyspaceEvents)();
extern int (*RedisModule_BlockedClientDisconnected)(RedisModuleCtx *ctx);
extern void (*RedisModule_RegisterClusterMessageReceiver)(RedisModuleCtx *ctx, uint8_t type, RedisModuleClusterMessageReceiver callback);
extern int (*RedisModule_SendClusterMessage)(RedisModuleCtx *ctx, char *target_id, uint8_t type, unsigned char *msg, uint32_t len);
extern int (*RedisModule_GetClusterNodeInfo)(RedisModuleCtx *ctx, const char *id, char *ip, char *master_id, int *port, int *flags);
extern char **(*RedisModule_GetClusterNodesList)(RedisModuleCtx *ctx, size_t *numnodes);
extern void (*RedisModule_FreeClusterNodesList)(char **ids);
extern RedisModuleTimerID (*RedisModule_CreateTimer)(RedisModuleCtx *ctx, mstime_t period, RedisModuleTimerProc callback, void *data);
extern int (*RedisModule_StopTimer)(RedisModuleCtx *ctx, RedisModuleTimerID id, void **data);
extern int (*RedisModule_GetTimerInfo)(RedisModuleCtx *ctx, RedisModuleTimerID id, uint64_t *remaining, void **data);
extern const char *(*RedisModule_GetMyClusterID)(void);
extern size_t (*RedisModule_GetClusterSize)(void);
extern void (*RedisModule_GetRandomBytes)(unsigned char *dst, size_t len);
extern void (*RedisModule_GetRandomHexChars)(char *dst, size_t len);
extern void (*RedisModule_SetDisconnectCallback)(RedisModuleBlockedClient *bc, RedisModuleDisconnectFunc callback);
extern void (*RedisModule_SetClusterFlags)(RedisModuleCtx *ctx, uint64_t flags);
extern int (*RedisModule_ExportSharedAPI)(RedisModuleCtx *ctx, const char *apiname, void *func);
extern void *(*RedisModule_GetSharedAPI)(RedisModuleCtx *ctx, const char *apiname);
extern RedisModuleCommandFilter *(*RedisModule_RegisterCommandFilter)(RedisModuleCtx *ctx, RedisModuleCommandFilterFunc cb, int flags);
extern int (*RedisModule_UnregisterCommandFilter)(RedisModuleCtx *ctx, RedisModuleCommandFilter *filter);
extern int (*RedisModule_CommandFilterArgsCount)(RedisModuleCommandFilterCtx *fctx);
extern const RedisModuleString *(*RedisModule_CommandFilterArgGet)(RedisModuleCommandFilterCtx *fctx, int pos);
extern int (*RedisModule_CommandFilterArgInsert)(RedisModuleCommandFilterCtx *fctx, int pos, RedisModuleString *arg);
extern int (*RedisModule_CommandFilterArgReplace)(RedisModuleCommandFilterCtx *fctx, int pos, RedisModuleString *arg);
extern int (*RedisModule_CommandFilterArgDelete)(RedisModuleCommandFilterCtx *fctx, int pos);
extern int (*RedisModule_Fork)(RedisModuleForkDoneHandler cb, void *user_data);
extern int (*RedisModule_ExitFromChild)(int retcode);
extern int (*RedisModule_KillForkChild)(int child_pid);
extern float (*RedisModule_GetUsedMemoryRatio)();
extern size_t (*RedisModule_MallocSize)(void* ptr);

#define REDISMODULE_OK 0
#define REDISMODULE_ERR 1
#define REDISMODULE_NODE_ID_LEN 40
#define REDISMODULE_NODE_MYSELF     (1<<0)
#define REDISMODULE_NODE_MASTER     (1<<1)
#define REDISMODULE_NODE_SLAVE      (1<<2)
#define REDISMODULE_NODE_PFAIL      (1<<3)
#define REDISMODULE_NODE_FAIL       (1<<4)
#define REDISMODULE_NODE_NOFAILOVER (1<<5)
#define REDISMODULE_NOT_USED(V) ((void) V)

/* The command is running in the context of a Lua script */
#define REDISMODULE_CTX_FLAGS_LUA (1<<0)
/* The command is running inside a Redis transaction */
#define REDISMODULE_CTX_FLAGS_MULTI (1<<1)
/* The instance is a master */
#define REDISMODULE_CTX_FLAGS_MASTER (1<<2)
/* The instance is a slave */
#define REDISMODULE_CTX_FLAGS_SLAVE (1<<3)
/* The instance is read-only (usually meaning it's a slave as well) */
#define REDISMODULE_CTX_FLAGS_READONLY (1<<4)
/* The instance is running in cluster mode */
#define REDISMODULE_CTX_FLAGS_CLUSTER (1<<5)
/* The instance has AOF enabled */
#define REDISMODULE_CTX_FLAGS_AOF (1<<6)
/* The instance has RDB enabled */
#define REDISMODULE_CTX_FLAGS_RDB (1<<7)
/* The instance has Maxmemory set */
#define REDISMODULE_CTX_FLAGS_MAXMEMORY (1<<8)
/* Maxmemory is set and has an eviction policy that may delete keys */
#define REDISMODULE_CTX_FLAGS_EVICT (1<<9)
/* Redis is out of memory according to the maxmemory flag. */
#define REDISMODULE_CTX_FLAGS_OOM (1<<10)
/* Less than 25% of memory available according to maxmemory. */
#define REDISMODULE_CTX_FLAGS_OOM_WARNING (1<<11)
/* The command was sent over the replication link. */
#define REDISMODULE_CTX_FLAGS_REPLICATED (1<<12)
/* Redis is currently loading either from AOF or RDB. */
#define REDISMODULE_CTX_FLAGS_LOADING (1<<13)
/* The replica has no link with its master, note that
 * there is the inverse flag as well:
 *
 *  REDISMODULE_CTX_FLAGS_REPLICA_IS_ONLINE
 *
 * The two flags are exclusive, one or the other can be set. */
#define REDISMODULE_CTX_FLAGS_REPLICA_IS_STALE (1<<14)
/* The replica is trying to connect with the master.
 * (REPL_STATE_CONNECT and REPL_STATE_CONNECTING states) */
#define REDISMODULE_CTX_FLAGS_REPLICA_IS_CONNECTING (1<<15)
/* THe replica is receiving an RDB file from its master. */
#define REDISMODULE_CTX_FLAGS_REPLICA_IS_TRANSFERRING (1<<16)
/* The replica is online, receiving updates from its master. */
#define REDISMODULE_CTX_FLAGS_REPLICA_IS_ONLINE (1<<17)
/* There is currently some background process active. */
#define REDISMODULE_CTX_FLAGS_ACTIVE_CHILD (1<<18)

#define REDISMODULE_POSTPONED_ARRAY_LEN -1

#define RedisModule_Assert(_e) ((_e)?(void)0 : (RedisModule__Assert(#_e,__FILE__,__LINE__),exit(1)))

#define RedisModule_IsAOFClient(id) ((id) == UINT64_MAX)
