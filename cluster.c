/* Disque Cluster implementation.
 *
 * Copyright (c) 2014-2019, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved. This code is under the AGPL license, check the
 * LICENSE file for more info. */

#include "disque.h"
#include "ack.h"

/* Update the list of available nodes in the cluster. */
void refreshReachableNodes(RedisModuleCtx *ctx) {
    size_t numnodes;
    char **ids = RedisModule_GetClusterNodesList(ctx,&numnodes);
    if (ids == NULL) return;

    /* Release the old list. */
    for (int j = 0; j < ClusterReachableNodesCount; j++)
        RedisModule_Free(ClusterReachableNodes[j]);
    RedisModule_Free(ClusterReachableNodes);

    /* Allocate and populate the new one. */
    ClusterReachableNodes = RedisModule_Alloc(sizeof(char*)*numnodes);
    ClusterReachableNodesCount = 0;
    for (size_t j = 0; j < numnodes; j++) {
        int flags;
        RedisModule_GetClusterNodeInfo(ctx,ids[j],NULL,NULL,NULL,&flags);
        /* XXX: Check for the LEAVING condition as well and don't include
         * the node. */
        if (flags & REDISMODULE_NODE_MYSELF ||
            flags & REDISMODULE_NODE_PFAIL ||
            flags & REDISMODULE_NODE_FAIL) continue;
        char *nodeid = RedisModule_Alloc(REDISMODULE_NODE_ID_LEN);
        memcpy(nodeid,ids[j],REDISMODULE_NODE_ID_LEN);
        ClusterReachableNodes[ClusterReachableNodesCount] = nodeid;
        ClusterReachableNodesCount++;
    }
    RedisModule_FreeClusterNodesList(ids);
}

/* Shuffle the array of reachable nodes using the Fisher Yates method so the
 * caller can just pick the first N to send messages to N random nodes.
 * */
void clusterShuffleReachableNodes(void) {
    int r, i;
    char *tmp;
    for(i = ClusterReachableNodesCount - 1; i > 0; i--) {
        r = rand() % (i + 1);
        tmp = ClusterReachableNodes[r];
        ClusterReachableNodes[r] = ClusterReachableNodes[i];
        ClusterReachableNodes[i] = tmp;
    }
}

/* Our message types callbacks: they are called when the instance receives
 * a message of the specified type via the Redis Cluster bus. */

/* This function performs common message sanity checks based on the message
 * type. If the message looks ok, 1 is returned, otherwise 0 is returned. */
int validateMessage(RedisModuleCtx *ctx, const char *sender_id, const unsigned char *payload, uint32_t len, uint8_t type) {
    /* Only handle jobs by known nodes. */
    int known_sender = RedisModule_GetClusterNodeInfo(ctx,sender_id,NULL,NULL,
                       NULL,NULL) == REDISMODULE_OK;
    if (!known_sender) return 0;

    if (type == DISQUE_MSG_REPLJOB ||
        type == DISQUE_MSG_YOURJOBS)
    {
        clusterSerializedJobMessage *hdr =
            (clusterSerializedJobMessage*) payload;

        /* Length should be at least as big as our message header. */
        if (len < sizeof(clusterSerializedJobMessage)-sizeof(hdr->jobs_data)) {
            RedisModule_Log(ctx,"warning","Wrong length in message type %d.",
                type);
            return 0;
        }

        uint32_t numjobs = ntohl(hdr->numjobs);
        uint32_t datasize = ntohl(hdr->datasize);

        /* Check that size matches the serialized jobs len. */
        if (datasize+sizeof(clusterSerializedJobMessage)-sizeof(hdr->jobs_data)
            != len)
        {
            RedisModule_Log(ctx,"warning","Wrong length in message type %d.",
                type);
            return 0;
        }

        /* Check that the number of jobs also match according to the message
         * type. */
        if (type == DISQUE_MSG_REPLJOB && numjobs != 1) return 0;
        if (type == DISQUE_MSG_YOURJOBS && numjobs == 0) return 0;
    } else if (type == DISQUE_MSG_GOTJOB ||
               type == DISQUE_MSG_SETACK ||
               type == DISQUE_MSG_GOTACK ||
               type == DISQUE_MSG_ENQUEUE ||
               type == DISQUE_MSG_WORKING ||
               type == DISQUE_MSG_QUEUED ||
               type == DISQUE_MSG_WILLQUEUE ||
               type == DISQUE_MSG_DELJOB)
    {
        /* Length should be at least as big as our message header. */
        if (len < sizeof (clusterJobIDMessage)) return 0;
    } else if (type == DISQUE_MSG_NEEDJOBS ||
               type == DISQUE_MSG_PAUSE)
    {
        clusterQueueMessage *hdr = (clusterQueueMessage*) payload;
        if (len < sizeof(clusterQueueMessage)-sizeof(hdr->qname)) {
            RedisModule_Log(ctx,"warning","Wrong length in message type %d.",
                type);
            return 0;
        }

        uint32_t qnamelen = ntohl(hdr->qnamelen);
        if (qnamelen+sizeof(clusterQueueMessage)-sizeof(hdr->qname) != len) {
            RedisModule_Log(ctx,"warning","Wrong length in message type %d.",
                type);
            return 0;
        }
    } else {
        RedisModule_Log(ctx,"warning","validateMessage() unknown message type");
    }

    return 1; /* Message looks fine. */
}

/* REPLJOB message: some node is asking us to create a copy (replicate) the
 * message it is sending to us. We reply with GOTJOB if we replicated the
 * message successfully, or if we already have it. */
void REPLJOBcallback(RedisModuleCtx *ctx, const char *sender_id, uint8_t msgtype, const unsigned char *payload, uint32_t len) {
    if (validateMessage(ctx,sender_id,payload,len,msgtype) == 0)
        return;

    clusterSerializedJobMessage *hdr = (clusterSerializedJobMessage*) payload;
    uint32_t datasize = ntohl(hdr->datasize);
    job *j;

    /* Don't replicate jobs if we got already memory issues or if we
     * are leaving the cluster. */
    if (getMemoryWarningLevel(ctx) > 0 || myselfLeaving()) return;

    j = deserializeJob(ctx,hdr->jobs_data,datasize,NULL,SER_MESSAGE);
    if (j == NULL) {
        RedisModule_Log(ctx,"warning",
            "Received corrupted job description from node %.40s",
            sender_id);
    } else {
        /* Don't replicate jobs about queues paused in input. */
        queue *q = lookupQueue(j->queue,sdslen(j->queue));
        if (q && q->flags & QUEUE_FLAG_PAUSED_IN) {
            freeJob(j);
            return;
        }
        j->flags |= JOB_FLAG_BCAST_QUEUED;
        j->state = JOB_STATE_ACTIVE;
        int retval = registerJob(j);
        if (retval == C_ERR) {
            /* The job already exists. Just update the list of nodes
             * that may have a copy. */
            updateJobNodes(j);
        } else {
            AOFLoadJob(ctx,j);
        }
        /* Reply with a GOTJOB message, even if we already did in the past.
         * The node receiving ADDJOB may try multiple times, and our
         * GOTJOB messages may get lost. */
        if (!(hdr->flags[0] & DISQUE_MSG_FLAG0_NOREPLY))
            clusterSendGotJob(ctx,sender_id,j);
        /* Release the job if we already had it. */
        if (retval == C_ERR) freeJob(j);
    }
}

/* YOURJOBS message handler. We do basic sanity checks here and then pass
 * the control to queue.c. */
void YOURJOBScallback(RedisModuleCtx *ctx, const char *sender_id, uint8_t msgtype, const unsigned char *payload, uint32_t len) {
    if (validateMessage(ctx,sender_id,payload,len,msgtype) == 0)
        return;

    clusterSerializedJobMessage *hdr = (clusterSerializedJobMessage*) payload;
    uint32_t numjobs = ntohl(hdr->numjobs);
    uint32_t datasize = ntohl(hdr->datasize);

    /* Pass control to queue.c higher level function that will decode and
     * queue the jobs. */
    receiveYourJobs(ctx,sender_id,numjobs,hdr->jobs_data,datasize);
}

/* GOTJOB message. This is an acknowledge that our job was replicated
 * correctly by the sender. */
void GOTJOBcallback(RedisModuleCtx *ctx, const char *sender_id, uint8_t msgtype, const unsigned char *payload, uint32_t len) {
    if (validateMessage(ctx,sender_id,payload,len,msgtype) == 0)
        return;

    clusterJobIDMessage *hdr = (clusterJobIDMessage*) payload;

    job *j = lookupJob(hdr->id);
    if (j && j->state == JOB_STATE_WAIT_REPL) {
        raxInsert(j->nodes_confirmed,
                  (unsigned char*)sender_id,REDISMODULE_NODE_ID_LEN,
                  NULL,NULL);
        if (raxSize(j->nodes_confirmed) == j->repl)
            jobReplicationAchieved(ctx,j);
    }
}

/* SETACK message: set the job as acknowledged and bring forward the
 * garbage collection of the job if needed, either replying with a GOTACK
 * message or by continuing the GC ourself if we are in a better condition
 * than the sender. */
void SETACKcallback(RedisModuleCtx *ctx, const char *sender_id, uint8_t msgtype, const unsigned char *payload, uint32_t len) {
    if (validateMessage(ctx,sender_id,payload,len,msgtype) == 0)
        return;

    clusterJobIDMessage *hdr = (clusterJobIDMessage*) payload;

    uint32_t mayhave = ntohl(hdr->aux);

    RedisModule_Log(ctx,"verbose","RECEIVED SETACK(%d) FROM %.40s FOR JOB %.*s",
        (int) mayhave, sender_id, JOB_ID_LEN, hdr->id);

    job *j = lookupJob(hdr->id);

    /* If we have the job, change the state to acknowledged. */
    if (j) {
        if (j->state == JOB_STATE_WAIT_REPL) {
            /* The job was acknowledged before ADDJOB achieved the
             * replication level requested! Unblock the client and
             * change the job state to active. */
            if (jobReplicationAchieved(ctx,j) == C_ERR) {
                /* The job was externally replicated and deleted from
                 * this node. Nothing to do... */
                return;
            }
        }
        /* ACK it if not already acked. */
        acknowledgeJob(ctx,j);
    }

    /* Reply according to the job exact state. */
    if (j == NULL || raxSize(j->nodes_delivered) <= mayhave) {
        /* If we don't know the job or our set of nodes that may have
         * the job is not larger than the sender, reply with GOTACK. */
        int known = j ? 1 : 0;
        clusterSendGotAck(ctx,sender_id,hdr->id,known);
    } else {
        /* We have the job but we know more nodes that may have it
         * than the sender, if we are here. Don't reply with GOTACK unless
         * mayhave is 0 (the sender just received an ACK from client about
         * a job it does not know), in order to let the sender delete it. */
        if (mayhave == 0)
            clusterSendGotAck(ctx,sender_id,hdr->id,1);
        /* Anyway, start a GC about this job. Because one of the two is
         * true:
         * 1) mayhave in the sender is 0, so it's up to us to start a GC
         *    because the sender has just a dummy ACK.
         * 2) Or, we prevented the sender from finishing the GC since
         *    we are not replying with GOTACK, and we need to take
         *    responsibility to evict the job in the cluster. */
        tryJobGC(ctx,j);
    }
}

/* GOTACK message. Acknowledge for the SETACK message. */
void GOTACKcallback(RedisModuleCtx *ctx, const char *sender_id, uint8_t msgtype, const unsigned char *payload, uint32_t len) {
    if (validateMessage(ctx,sender_id,payload,len,msgtype) == 0)
        return;

    clusterJobIDMessage *hdr = (clusterJobIDMessage*) payload;

    uint32_t known = ntohl(hdr->aux);
    job *j = lookupJob(hdr->id);
    if (j) gotAckReceived(ctx,sender_id,j,known);
}

/* DELJOB message. Just remove the job, the sender is doing garbage collection
 * for it. */
void DELJOBcallback(RedisModuleCtx *ctx, const char *sender_id, uint8_t msgtype, const unsigned char *payload, uint32_t len) {
    if (validateMessage(ctx,sender_id,payload,len,msgtype) == 0)
        return;
    clusterJobIDMessage *hdr = (clusterJobIDMessage*) payload;
    job *j = lookupJob(hdr->id);
    if (j) {
        RedisModule_Log(ctx,"verbose",
            "RECEIVED DELJOB FOR JOB %.*s",JOB_ID_LEN,j->id);
        unregisterJob(ctx,j);
        freeJob(j);
    }
}

/* ENQUEUE message. Force the receiver to put a given job into the queue for
 * delivery. This is sent by nodes that created a job for a client, but are
 * under memory pressure and don't want to take a copy: they increase the
 * replication factor by 1, and later discard the job, after sending an
 * EQUEUE message to a random node having a copy of the job. */
void ENQUEUEcallback(RedisModuleCtx *ctx, const char *sender_id, uint8_t msgtype, const unsigned char *payload, uint32_t len) {
    if (validateMessage(ctx,sender_id,payload,len,msgtype) == 0)
        return;

    clusterJobIDMessage *hdr = (clusterJobIDMessage*) payload;
    uint32_t delay = ntohl(hdr->aux);

    job *j = lookupJob(hdr->id);
    if (j && j->state < JOB_STATE_QUEUED) {
        /* Discard this message if the queue is paused in input. */
        queue *q = lookupQueue(j->queue,sdslen(j->queue));
        if (q == NULL || !(q->flags & QUEUE_FLAG_PAUSED_IN)) {
            /* We received an ENQUEUE message: consider this node as the
             * first to queue the job, so no need to broadcast a QUEUED
             * message the first time we queue it. */
            j->flags &= ~JOB_FLAG_BCAST_QUEUED;
            RedisModule_Log(ctx,"verbose","RECEIVED ENQUEUE FOR JOB %.*s",
                            JOB_ID_LEN,j->id);
            if (delay == 0) {
                enqueueJob(ctx,j,0);
            } else {
                updateJobRequeueTime(j,mstime()+delay*1000);
            }
        }
    }
}

/* Implements WORKINGcallback() and QUEUEDcallback().
 *
 * Those messages are sent by nodes that just queued a message or that
 * received a WORKING command from a client, in order to update the
 * re-queue time in the other nodes and avoid useless additional deliveries
 * of the same message. */
void receiveWorkingAndQueued(RedisModuleCtx *ctx, const char *sender_id, const unsigned char *payload, uint32_t len, int msgtype) {
    if (validateMessage(ctx,sender_id,payload,len,msgtype) == 0)
        return;

    clusterJobIDMessage *hdr = (clusterJobIDMessage*) payload;
    job *j = lookupJob(hdr->id);
    if (j && j->state <= JOB_STATE_QUEUED) {
        RedisModule_Log(ctx,"verbose",
            "UPDATING QTIME FOR JOB %.*s",JOB_ID_LEN,j->id);
        /* Move the time we'll re-queue this job in the future. Moreover
         * if the sender has a Node ID greater than our node ID, and we
         * have the message queued as well, dequeue it, to avoid an
         * useless multiple delivery.
         *
         * The message is always dequeued in case the message is of
         * type WORKING, since this is the explicit semantics of WORKING.
         *
         * If the message is WORKING always dequeue regardless of the
         * sender name, since there is a client claiming to work on the
         * message. */
        const char *myself = RedisModule_GetMyClusterID();
        if (j->state == JOB_STATE_QUEUED &&
            (msgtype == DISQUE_MSG_WORKING ||
             compareNodeIDsByJob(sender_id,myself,j) > 0))
        {
            dequeueJob(j);
        }

        /* Update the time at which this node will attempt to enqueue
         * the message again. */
        if (j->retry) {
            j->flags |= JOB_FLAG_BCAST_WILLQUEUE;
            updateJobRequeueTime(j,mstime()+
                                   j->retry*1000+
                                   randomTimeError(DISQUE_TIME_ERR));
        }

        /* Update multiple deliveries counters. */
        if (msgtype == DISQUE_MSG_QUEUED) {
            if (hdr->flags[0] & DISQUE_MSG_FLAG0_INCR_NACKS)
                j->num_nacks++;
            if (hdr->flags[0] & DISQUE_MSG_FLAG0_INCR_DELIV)
                j->num_deliv++;
        }
    } else if (j && j->state == JOB_STATE_ACKED) {
        /* Some other node queued a message that we have as
         * already acknowledged. Try to force it to drop it. */
        clusterSendSetAck(ctx,sender_id,j);
    }
}

/* See receiveWorkingAndQueued() for more info. */
void WORKINGcallback(RedisModuleCtx *ctx, const char *sender_id, uint8_t msgtype, const unsigned char *payload, uint32_t len) {
    receiveWorkingAndQueued(ctx,sender_id,payload,len,msgtype);
}

/* See receiveWorkingAndQueued() for more info. */
void QUEUEDcallback(RedisModuleCtx *ctx, const char *sender_id, uint8_t msgtype, const unsigned char *payload, uint32_t len) {
    receiveWorkingAndQueued(ctx,sender_id,payload,len,msgtype);
}

/* We receive WILLQUEUE messages when nodes are about to queue a given
 * message (a few milliseconds before). This way the receiver may react
 * accordingly: if the have the message already queued, we send a
 * QUEUED message, so that the sender avoids to queue the message another
 * time. If instead the message is already acknowledged as processed, we
 * inform the sender with a SETACK message. */
void WILLQUEUEcallback(RedisModuleCtx *ctx, const char *sender_id, uint8_t msgtype, const unsigned char *payload, uint32_t len) {
    if (validateMessage(ctx,sender_id,payload,len,msgtype) == 0)
        return;

    clusterJobIDMessage *hdr = (clusterJobIDMessage*) payload;
    job *j = lookupJob(hdr->id);
    if (j) {
        if (j->state == JOB_STATE_ACTIVE ||
            j->state == JOB_STATE_QUEUED)
        {
            raxInsert(j->nodes_delivered,
                (unsigned char*)sender_id,REDISMODULE_NODE_ID_LEN,
                NULL,NULL);
        }
        if (j->state == JOB_STATE_QUEUED)
            clusterBroadcastQueued(ctx,j,DISQUE_MSG_NOFLAGS);
        else if (j->state == JOB_STATE_ACKED)
            clusterSendSetAck(ctx,sender_id,j);
    }
}

/* Receive the NEEDJOBS and PAUSE messages, that are messages citing a
 * specific queue name, because other nodes either need us to transfer
 * jobs about this queue to them, or to pause/resume the queue. */
void receiveNeedjobsAndPause(RedisModuleCtx *ctx, const char *sender_id, const unsigned char *payload, uint32_t len, int msgtype) {
   if (validateMessage(ctx,sender_id,payload,len,msgtype) == 0)
        return;

    clusterQueueMessage *hdr = (clusterQueueMessage*) payload;
    uint32_t qnamelen = ntohl(hdr->qnamelen);
    uint32_t count = ntohl(hdr->aux);

    RedisModule_Log(ctx,"verbose","RECEIVED %s FOR QUEUE %.*s (aux=%d)",
        (msgtype == DISQUE_MSG_NEEDJOBS) ? "NEEDJOBS" : "PAUSE",
        (int)qnamelen,hdr->qname,count);
    if (msgtype == DISQUE_MSG_NEEDJOBS)
        receiveNeedJobs(ctx,sender_id,hdr->qname,qnamelen,count);
    else
        receivePauseQueue(ctx,hdr->qname,qnamelen,count);
}

void NEEDJOBScallback(RedisModuleCtx *ctx, const char *sender_id, uint8_t msgtype, const unsigned char *payload, uint32_t len) {
    receiveNeedjobsAndPause(ctx,sender_id,payload,len,msgtype);
}

void PAUSEcallback(RedisModuleCtx *ctx, const char *sender_id, uint8_t msgtype, const unsigned char *payload, uint32_t len) {
    receiveNeedjobsAndPause(ctx,sender_id,payload,len,msgtype);
}

/* -----------------------------------------------------------------------------
 * CLUSTER job related messages
 * -------------------------------------------------------------------------- */

/* This is a wrapper for RedisModule_SendClusterMessage() sending the message
 * to all the node IDs stored into the radix tree 'nodes'. */
void clusterBroadcastMessage(RedisModuleCtx *ctx, rax *nodes, int type, const unsigned char *msg, uint32_t msglen) {
    if (nodes == NULL) {
        RedisModule_SendClusterMessage(ctx,NULL,type,
            (unsigned char*)msg,msglen);
    } else {
        const char *myself = RedisModule_GetMyClusterID();
        raxIterator ri;
        raxStart(&ri,nodes);
        raxSeek(&ri,"^",NULL,0);
        while(raxNext(&ri)) {
            if (memcmp(myself,ri.key,ri.key_len) == 0) continue;
            RedisModule_SendClusterMessage(ctx,(char*)ri.key,type,
                (unsigned char*)msg,msglen);
        }
        raxStop(&ri);
    }
}

/* Broadcast a REPLJOB message to 'repl' nodes, and populates the list of jobs
 * that may have the job.
 *
 * If there are already nodes that received the message, additional
 * 'repl' nodes will be added to the list (if possible), and the message
 * broadcast again to all the nodes that may already have the message,
 * plus the new ones.
 *
 * However for nodes for which we already have the GOTJOB reply, we flag
 * the message with the NOREPLY flag. If the 'noreply' argument of the function
 * is true we also flag the message with NOREPLY regardless of the fact the
 * node we are sending REPLJOB to already replied or not.
 *
 * Nodes are selected from ClusterReachableNodes list.
 * The function returns the number of new (additional) nodes that MAY have
 * received the message. */
int clusterReplicateJob(RedisModuleCtx *ctx, job *j, int repl, int noreply) {
    int i, added = 0;

    if (repl <= 0) return 0;

    /* Even if we'll not be able to add new nodes, we set the last try
     * time here, so we'll not try again before some time in case the job
     * replication gets delayed. */
    j->added_node_time = mstime();

    /* Add the specified number of nodes to the list of receivers. */
    clusterShuffleReachableNodes();
    for (i = 0; i < ClusterReachableNodesCount; i++) {
        const char *nodeid = ClusterReachableNodes[i];

        if (raxInsert(j->nodes_delivered,(unsigned char*)nodeid,
            REDISMODULE_NODE_ID_LEN,NULL,NULL))
        {
            /* Only counts non-duplicated nodes. */
            added++;
            if (--repl == 0) break;
        }
    }

    /* Resend the message to all the past and new nodes. */
    unsigned char buf[sizeof(clusterSerializedJobMessage)], *payload;
    clusterSerializedJobMessage *hdr = (clusterSerializedJobMessage*) buf;
    uint32_t totlen;

    sds serialized = serializeJob(sdsempty(),j,SER_MESSAGE);

    memset(hdr,0,sizeof(*hdr));
    totlen = sizeof(clusterSerializedJobMessage)-sizeof(hdr->jobs_data);
    totlen += sdslen(serialized);

    hdr->numjobs = htonl(1);
    hdr->datasize = htonl(sdslen(serialized));

    if (totlen <= sizeof(buf)) {
        payload = buf;
    } else {
        payload = RedisModule_Alloc(totlen);
        memcpy(payload,buf,sizeof(buf));
        hdr = (clusterSerializedJobMessage*) payload;
    }
    memcpy(hdr->jobs_data,serialized,sdslen(serialized));
    sdsfree(serialized);

    /* Actual delivery of the message to the list of nodes. */
    raxIterator ri;
    raxStart(&ri,j->nodes_delivered);
    raxSeek(&ri,"^",NULL,0);

    const char *myself = RedisModule_GetMyClusterID();
    while(raxNext(&ri)) {
        char *receiver = (char*)ri.key;
        if (memcmp(myself,receiver,ri.key_len) == 0) continue;

        /* We ask for reply only if 'noreply' is false and the target node
         * did not already replied with GOTJOB. */
        int acked = j->nodes_confirmed &&
                    raxFind(j->nodes_confirmed,
                            (unsigned char*)receiver,ri.key_len)
                            != raxNotFound;
        if (noreply || acked) {
            hdr->flags[0] |= DISQUE_MSG_FLAG0_NOREPLY;
        } else {
            hdr->flags[0] &= ~DISQUE_MSG_FLAG0_NOREPLY;
        }

        /* If the target node acknowledged the message already, send it again
         * only if there are additional nodes. We want the target node to
         * refresh its list of receivers. */
        if (!acked || added != 0)
            RedisModule_SendClusterMessage(ctx,receiver,DISQUE_MSG_REPLJOB,
                                           payload,totlen);
    }
    raxStop(&ri);

    if (payload != buf) RedisModule_Free(payload);
    return added;
}

/* Helper function to send all the messages that have just a type,
 * a Job ID, and an optional 'aux' additional value. */
void clusterSendJobIDMessage(RedisModuleCtx *ctx, int type, const char *receiver, char *id, int aux) {
    clusterJobIDMessage msg;
    memset(&msg,0,sizeof(msg));
    memcpy(msg.id,id,JOB_ID_LEN);
    msg.aux = htonl(aux);
    RedisModule_SendClusterMessage(ctx,(char*)receiver,type,
        (unsigned char*)&msg,sizeof(msg));
}

/* Like clusterSendJobIDMessage(), but sends the message to the specified set
 * of nodes (excluding myself if included in the set of nodes).
 * If nodes is set to NULL sends the message to all the nodes. */
void clusterBroadcastJobIDMessage(RedisModuleCtx *ctx, rax *nodes, const char *id, int type, uint32_t aux, unsigned char flags) {
    clusterJobIDMessage msg;

    /* Build the message one time, send the same to everybody. */
    memset(&msg,0,sizeof(msg));
    memcpy(msg.id,id,JOB_ID_LEN);
    msg.aux = htonl(aux);
    msg.flags[0] = flags;

    if (nodes == NULL) {
        RedisModule_SendClusterMessage(ctx,NULL,type,
            (unsigned char*)&msg,sizeof(msg));
    } else {
        clusterBroadcastMessage(ctx,nodes,type,
            (unsigned char*)&msg,sizeof(msg));
    }
}

/* Send a GOTJOB message to the specified node, if connected.
 * GOTJOB messages only contain the ID of the job, and are acknowledges
 * that the job was replicated to a target node. The receiver of the message
 * will be able to reply to the client that the job was accepted by the
 * system when enough nodes have a copy of the job. */
void clusterSendGotJob(RedisModuleCtx *ctx, const char *receiver, job *j) {
    clusterSendJobIDMessage(ctx,DISQUE_MSG_GOTJOB,receiver,j->id,0);
}

/* Force the receiver to queue a job, if it has that job in an active
 * state. */
void clusterSendEnqueue(RedisModuleCtx *ctx, const char *receiver, job *j, uint32_t delay) {
    clusterSendJobIDMessage(ctx,DISQUE_MSG_ENQUEUE,receiver,j->id,delay);
}

/* Tell the receiver that the specified job was just re-queued by the
 * sender, so it should update its qtime to the future, and if the job
 * is also queued by the receiving node, to drop it from the queue if the
 * sender has a greater node ID (so that we try in a best-effort way to
 * avoid useless multi deliveries).
 *
 * This message is sent to all the nodes we believe may have a copy
 * of the message and are reachable. */
void clusterBroadcastQueued(RedisModuleCtx *ctx, job *j, unsigned char flags) {
    RedisModule_Log(ctx,"verbose","BCAST QUEUED: %.*s",JOB_ID_LEN,j->id);
    clusterBroadcastJobIDMessage(ctx,j->nodes_delivered,j->id,
                                 DISQUE_MSG_QUEUED,0,flags);
}

/* WORKING is like QUEUED, but will always force the receiver to dequeue. */
void clusterBroadcastWorking(RedisModuleCtx *ctx, job *j) {
    RedisModule_Log(ctx,"verbose","BCAST WORKING: %.*s",JOB_ID_LEN,j->id);
    clusterBroadcastJobIDMessage(ctx,j->nodes_delivered,j->id,
                                 DISQUE_MSG_WORKING,0,DISQUE_MSG_NOFLAGS);
}

/* Send a DELJOB message to all the nodes that may have a copy. */
void clusterBroadcastDelJob(RedisModuleCtx *ctx, job *j) {
    RedisModule_Log(ctx,"verbose","BCAST DELJOB: %.*s",JOB_ID_LEN,j->id);
    clusterBroadcastJobIDMessage(ctx,j->nodes_delivered,j->id,
                                 DISQUE_MSG_DELJOB,0,DISQUE_MSG_NOFLAGS);
}

/* Tell the receiver to reply with a QUEUED message if it has the job
 * already queued, to prevent us from queueing it in the next few
 * milliseconds. */
void clusterSendWillQueue(RedisModuleCtx *ctx, job *j) {
    RedisModule_Log(ctx,"verbose","BCAST WILLQUEUE: %.*s",JOB_ID_LEN,j->id);
    clusterBroadcastJobIDMessage(ctx,j->nodes_delivered,j->id,
                                 DISQUE_MSG_WILLQUEUE,0,DISQUE_MSG_NOFLAGS);
}

/* Force the receiver to acknowledge the job as delivered. */
void clusterSendSetAck(RedisModuleCtx *ctx, const char *receiver, job *j) {
    uint32_t maxowners = raxSize(j->nodes_delivered);
    clusterSendJobIDMessage(ctx,DISQUE_MSG_SETACK,receiver,j->id,maxowners);
}

/* Acknowledge a SETACK message. */
void clusterSendGotAck(RedisModuleCtx *ctx, const char *receiver, char *jobid, int known) {
    clusterSendJobIDMessage(ctx,DISQUE_MSG_GOTACK,receiver,jobid,known);
}

/* Send a NEEDJOBS message to the specified set of nodes. */
void clusterSendNeedJobs(RedisModuleCtx *ctx, sds qname, int numjobs, rax *nodes) {
    clusterQueueMessage *msg;
    uint32_t totlen;
    size_t qnamelen = sdslen(qname);

    RedisModule_Log(ctx,"verbose","Sending NEEDJOBS for %s %d, %d nodes",
        qname, (int)numjobs, nodes ? (int)raxSize(nodes) : -1);

    totlen = sizeof(*msg) - sizeof(msg->qname) + qnamelen;
    msg = RedisModule_Alloc(totlen);
    msg->aux = htonl(numjobs);
    msg->qnamelen = htonl(qnamelen);
    memcpy(msg->qname, qname, qnamelen);
    clusterBroadcastMessage(ctx,nodes,DISQUE_MSG_NEEDJOBS,
        (unsigned char*)msg,totlen);
    RedisModule_Free(msg);
}

/* Send a PAUSE message to the specified set of nodes. */
void clusterSendPause(RedisModuleCtx *ctx, const char *qname, size_t qnamelen, uint32_t flags, rax *nodes) {
    uint32_t totlen;
    clusterQueueMessage *hdr;

    RedisModule_Log(ctx,"verbose","Sending PAUSE for %.*s flags=%d",
        (int)qnamelen, (char*)qname, (int)flags);

    totlen = sizeof(clusterQueueMessage)-sizeof(hdr->qname)+qnamelen;
    hdr = RedisModule_Alloc(totlen);
    memset(hdr,0,totlen);

    hdr->aux = htonl(flags);
    hdr->qnamelen = htonl(qnamelen);
    memcpy(hdr->qname, qname, qnamelen);
    clusterBroadcastMessage(ctx,nodes,DISQUE_MSG_PAUSE,
        (unsigned char*)hdr,totlen);
    RedisModule_Free(hdr);
}

/* Same as clusterSendPause() but broadcasts the message to the
 * whole cluster. */
void clusterBroadcastPause(RedisModuleCtx *ctx, const char *qname, size_t qnamelen, uint32_t flags) {
    clusterSendPause(ctx,qname,qnamelen,flags,NULL);
}

/* Send a YOURJOBS message to the specified node, with a serialized copy of
 * the jobs referenced by the 'jobs' array and containing 'count' jobs. */
void clusterSendYourJobs(RedisModuleCtx *ctx, const char *node, job **jobs, uint32_t count) {
    unsigned char buf[sizeof(clusterSerializedJobMessage)], *payload;
    clusterSerializedJobMessage *hdr = (clusterSerializedJobMessage*) buf;
    uint32_t totlen, j;

    RedisModule_Log(ctx,"verbose",
        "Sending %d jobs to %.40s", (int)count,node);

    totlen = sizeof(clusterSerializedJobMessage)-sizeof(hdr->jobs_data);
    sds serialized = sdsempty();
    for (j = 0; j < count; j++)
        serialized = serializeJob(serialized,jobs[j],SER_MESSAGE);
    totlen += sdslen(serialized);

    memset(hdr,0,sizeof(buf));
    hdr->numjobs = htonl(count);
    hdr->datasize = htonl(sdslen(serialized));

    if (totlen <= sizeof(buf)) {
        payload = buf;
    } else {
        payload = RedisModule_Alloc(totlen);
        memcpy(payload,buf,sizeof(buf));
        hdr = (clusterSerializedJobMessage*) payload;
    }
    memcpy(hdr->jobs_data,serialized,sdslen(serialized));
    sdsfree(serialized);
    RedisModule_SendClusterMessage(ctx,(char*)node,DISQUE_MSG_YOURJOBS,
                                   payload,totlen);
    if (payload != buf) RedisModule_Free(payload);
}
