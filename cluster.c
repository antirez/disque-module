/* Disque Cluster implementation.
 *
 * Copyright (c) 2018, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 */

#include "ack.h"

/* When this function is called, there is a packet to process starting
 * at node->rcvbuf. Releasing the buffer is up to the caller, so this
 * function should just handle the higher level stuff of processing the
 * packet, modifying the cluster state if needed.
 *
 * The function returns 1 if the link is still valid after the packet
 * was processed, otherwise 0 if the link was freed since the packet
 * processing lead to some inconsistency error (for instance a PONG
 * received from the wrong sender ID). */
int clusterProcessPacket(clusterLink *link) {
    clusterMsg *hdr = (clusterMsg*) link->rcvbuf;
    uint32_t totlen = ntohl(hdr->totlen);
    uint16_t type = ntohs(hdr->type);
    clusterNode *sender;

    server.cluster->stats_bus_messages_received++;
    serverLog(LL_DEBUG,"--- Processing packet of type %d, %lu bytes",
        type, (unsigned long) totlen);

    /* Perform sanity checks */
    if (totlen < 16) return 1; /* At least signature, version, totlen, count. */
    if (ntohs(hdr->ver) != CLUSTER_PROTO_VER)
        return 1; /* Can't handle versions other than the current one.*/
    if (totlen > sdslen(link->rcvbuf)) return 1;
    if (type == CLUSTERMSG_TYPE_PING || type == CLUSTERMSG_TYPE_PONG ||
        type == CLUSTERMSG_TYPE_MEET)
    {
        uint16_t count = ntohs(hdr->count);
        uint32_t explen; /* expected length of this packet */

        explen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
        explen += (sizeof(clusterMsgDataGossip)*count);
        if (totlen != explen) return 1;
    } else if (type == CLUSTERMSG_TYPE_FAIL) {
        uint32_t explen = sizeof(clusterMsg)-sizeof(union clusterMsgData);

        explen += sizeof(clusterMsgDataFail);
        if (totlen != explen) return 1;
    } else if (type == CLUSTERMSG_TYPE_REPLJOB ||
               type == CLUSTERMSG_TYPE_YOURJOBS) {
        uint32_t explen = sizeof(clusterMsg)-sizeof(union clusterMsgData);

        explen += sizeof(clusterMsgDataJob) -
                  sizeof(hdr->data.jobs.serialized.jobs_data) +
                  ntohl(hdr->data.jobs.serialized.datasize);
        if (totlen != explen) return 1;
    } else if (type == CLUSTERMSG_TYPE_GOTJOB ||
               type == CLUSTERMSG_TYPE_ENQUEUE ||
               type == CLUSTERMSG_TYPE_QUEUED ||
               type == CLUSTERMSG_TYPE_WORKING ||
               type == CLUSTERMSG_TYPE_SETACK ||
               type == CLUSTERMSG_TYPE_GOTACK ||
               type == CLUSTERMSG_TYPE_DELJOB ||
               type == CLUSTERMSG_TYPE_WILLQUEUE)
    {
        uint32_t explen = sizeof(clusterMsg)-sizeof(union clusterMsgData);

        explen += sizeof(clusterMsgDataJobID);
        if (totlen != explen) return 1;
    } else if (type == CLUSTERMSG_TYPE_NEEDJOBS ||
               type == CLUSTERMSG_TYPE_PAUSE)
    {
        uint32_t explen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
        explen += sizeof(clusterMsgDataQueueOp) - 8;
        if (totlen < explen) return 1;
        explen += ntohl(hdr->data.queueop.about.qnamelen);
        if (totlen != explen) return 1;
    }

    /* Check if the sender is a known node. */
    sender = clusterLookupNode(hdr->sender);

    /* Initial processing of PING and MEET requests replying with a PONG. */
    if (type == CLUSTERMSG_TYPE_PING || type == CLUSTERMSG_TYPE_MEET) {
        serverLog(LL_DEBUG,"Ping packet received: %p", (void*)link->node);

        /* We use incoming MEET messages in order to set the address
         * for 'myself', since only other cluster nodes will send us
         * MEET messages on handshakes, when the cluster joins, or
         * later if we changed address, and those nodes will use our
         * official address to connect to us. So by obtaining this address
         * from the socket is a simple way to discover / update our own
         * address in the cluster without it being hardcoded in the config.
         *
         * However if we don't have an address at all, we update the address
         * even with a normal PING packet. If it's wrong it will be fixed
         * by MEET later. */
        if (type == CLUSTERMSG_TYPE_MEET || myself->ip[0] == '\0') {
            char ip[NET_IP_STR_LEN];

            if (anetSockName(link->fd,ip,sizeof(ip),NULL) != -1 &&
                strcmp(ip,myself->ip))
            {
                memcpy(myself->ip,ip,NET_IP_STR_LEN);
                serverLog(LL_WARNING,"IP address for this node updated to %s",
                    myself->ip);
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
            }
        }

        /* Add this node if it is new for us and the msg type is MEET.
         * In this stage we don't try to add the node with the right
         * flags, slaveof pointer, and so forth, as this details will be
         * resolved when we'll receive PONGs from the node. */
        if (!sender && type == CLUSTERMSG_TYPE_MEET) {
            clusterNode *node;

            node = createClusterNode(NULL,CLUSTER_NODE_HANDSHAKE);
            nodeIp2String(node->ip,link);
            node->port = ntohs(hdr->port);
            clusterAddNode(node);
            clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
        }

        /* If this is a MEET packet from an unknown node, we still process
         * the gossip section here since we have to trust the sender because
         * of the message type. */
        if (!sender && type == CLUSTERMSG_TYPE_MEET)
            clusterProcessGossipSection(hdr,link);

        /* Anyway reply with a PONG */
        clusterSendPing(link,CLUSTERMSG_TYPE_PONG);
    }

    /* PING, PONG, MEET: process config information. */
    if (type == CLUSTERMSG_TYPE_PING || type == CLUSTERMSG_TYPE_PONG ||
        type == CLUSTERMSG_TYPE_MEET)
    {
        serverLog(LL_DEBUG,"%s packet received: %p",
            type == CLUSTERMSG_TYPE_PING ? "ping" : "pong",
            (void*)link->node);
        if (link->node) {
            if (nodeInHandshake(link->node)) {
                /* If we already have this node, try to change the
                 * IP/port of the node with the new one. */
                if (sender) {
                    serverLog(LL_VERBOSE,
                        "Handshake: we already know node %.40s, "
                        "updating the address if needed.", sender->name);
                    if (nodeUpdateAddressIfNeeded(sender,link,ntohs(hdr->port)))
                    {
                        clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                                             CLUSTER_TODO_UPDATE_STATE);
                    }
                    /* Free this node as we already have it. This will
                     * cause the link to be freed as well. */
                    clusterDelNode(link->node);
                    return 0;
                }

                /* First thing to do is replacing the random name with the
                 * right node name if this was a handshake stage. */
                clusterRenameNode(link->node, hdr->sender);
                serverLog(LL_DEBUG,"Handshake with node %.40s completed.",
                    link->node->name);
                link->node->flags &= ~CLUSTER_NODE_HANDSHAKE;
                clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE|
                                     CLUSTER_TODO_SAVE_CONFIG);
            } else if (memcmp(link->node->name,hdr->sender,
                        CLUSTER_NAMELEN) != 0)
            {
                /* If the reply has a non matching node ID we
                 * disconnect this node and set it as not having an associated
                 * address. */
                serverLog(LL_DEBUG,"PONG contains mismatching sender ID");
                link->node->flags |= CLUSTER_NODE_NOADDR;
                link->node->ip[0] = '\0';
                link->node->port = 0;
                freeClusterLink(link);
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
                return 0;
            }
        }

        /* Update the node address if it changed. */
        if (sender && type == CLUSTERMSG_TYPE_PING &&
            !nodeInHandshake(sender) &&
            nodeUpdateAddressIfNeeded(sender,link,ntohs(hdr->port)))
        {
            clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                                 CLUSTER_TODO_UPDATE_STATE);
        }

        /* Update our info about the node */
        if (link->node && type == CLUSTERMSG_TYPE_PONG) {
            link->node->pong_received = mstime();
            link->node->ping_sent = 0;

            /* The PFAIL condition can be reversed without external
             * help if it is momentary (that is, if it does not
             * turn into a FAIL state).
             *
             * The FAIL condition is also reversible under specific
             * conditions detected by clearNodeFailureIfNeeded(). */
            if (nodeTimedOut(link->node)) {
                link->node->flags &= ~CLUSTER_NODE_PFAIL;
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                                     CLUSTER_TODO_UPDATE_STATE);
            } else if (nodeFailed(link->node)) {
                clearNodeFailureIfNeeded(link->node);
            }
        }

        /* Copy certain flags from what the node publishes. */
        if (sender) {
            int old_flags = sender->flags;
            int reported_flags = ntohs(hdr->flags);
            int flags_to_copy = CLUSTER_NODE_LEAVING;
            sender->flags &= ~flags_to_copy;
            sender->flags |= (reported_flags & flags_to_copy);

            /* Currently we just save the config without FSYNC nor
             * update of the cluster state, since the only flag we update
             * here is LEAVING which is non critical to persist. */
            if (sender->flags != old_flags)
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
        }

        /* Get info from the gossip section */
        if (sender) clusterProcessGossipSection(hdr,link);
    } else if (type == CLUSTERMSG_TYPE_FAIL) {
        clusterNode *failing;

        if (!sender) return 1;
        failing = clusterLookupNode(hdr->data.fail.about.nodename);
        if (failing &&
            !(failing->flags & (CLUSTER_NODE_FAIL|CLUSTER_NODE_MYSELF)))
        {
            serverLog(LL_VERBOSE,
                "FAIL message received from %.40s about %.40s",
                hdr->sender, hdr->data.fail.about.nodename);
            failing->flags |= CLUSTER_NODE_FAIL;
            failing->fail_time = mstime();
            failing->flags &= ~CLUSTER_NODE_PFAIL;
            clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                                 CLUSTER_TODO_UPDATE_STATE);
        }
    } else if (type == CLUSTERMSG_TYPE_REPLJOB) {
        uint32_t numjobs = ntohl(hdr->data.jobs.serialized.numjobs);
        uint32_t datasize = ntohl(hdr->data.jobs.serialized.datasize);
        job *j;

        /* Only replicate jobs by known nodes. */
        if (!sender || numjobs != 1) return 1;

        /* Don't replicate jobs if we got already memory issues or if we
         * are leaving the cluster. */
        if (getMemoryWarningLevel() > 0 || myselfLeaving()) return 1;

        j = deserializeJob(hdr->data.jobs.serialized.jobs_data,datasize,NULL,SER_MESSAGE);
        if (j == NULL) {
            serverLog(LL_WARNING,
                "Received corrupted job description from node %.40s",
                hdr->sender);
        } else {
            /* Don't replicate jobs about queues paused in input. */
            queue *q = lookupQueue(j->queue);
            if (q && q->flags & QUEUE_FLAG_PAUSED_IN) {
                freeJob(j);
                return 1;
            }
            j->flags |= JOB_FLAG_BCAST_QUEUED;
            j->state = JOB_STATE_ACTIVE;
            int retval = registerJob(j);
            if (retval == C_ERR) {
                /* The job already exists. Just update the list of nodes
                 * that may have a copy. */
                updateJobNodes(j);
            } else {
                AOFLoadJob(j);
            }
            /* Reply with a GOTJOB message, even if we already did in the past.
             * The node receiving ADDJOB may try multiple times, and our
             * GOTJOB messages may get lost. */
            if (!(hdr->mflags[0] & CLUSTERMSG_FLAG0_NOREPLY))
                clusterSendGotJob(sender,j);
            /* Release the job if we already had it. */
            if (retval == C_ERR) freeJob(j);
        }
    } else if (type == CLUSTERMSG_TYPE_YOURJOBS) {
        uint32_t numjobs = ntohl(hdr->data.jobs.serialized.numjobs);
        uint32_t datasize = ntohl(hdr->data.jobs.serialized.datasize);

        if (!sender || numjobs == 0) return 1;
        receiveYourJobs(sender,numjobs,hdr->data.jobs.serialized.jobs_data,datasize);
    } else if (type == CLUSTERMSG_TYPE_GOTJOB) {
        if (!sender) return 1;

        job *j = lookupJob(hdr->data.jobid.job.id);
        if (j && j->state == JOB_STATE_WAIT_REPL) {
            dictAdd(j->nodes_confirmed,sender->name,sender);
            if (dictSize(j->nodes_confirmed) == j->repl)
                jobReplicationAchieved(j);
        }
    } else if (type == CLUSTERMSG_TYPE_SETACK) {
        if (!sender) return 1;
        uint32_t mayhave = ntohl(hdr->data.jobid.job.aux);

        serverLog(LL_VERBOSE,"RECEIVED SETACK(%d) FROM %.40s FOR JOB %.*s",
            (int) mayhave,
            sender->name, JOB_ID_LEN, hdr->data.jobid.job.id);

        job *j = lookupJob(hdr->data.jobid.job.id);

        /* If we have the job, change the state to acknowledged. */
        if (j) {
            if (j->state == JOB_STATE_WAIT_REPL) {
                /* The job was acknowledged before ADDJOB achieved the
                 * replication level requested! Unblock the client and
                 * change the job state to active. */
                if (jobReplicationAchieved(j) == C_ERR) {
                    /* The job was externally replicated and deleted from
                     * this node. Nothing to do... */
                    return 1;
                }
            }
            /* ACK it if not already acked. */
            acknowledgeJob(j);
        }

        /* Reply according to the job exact state. */
        if (j == NULL || dictSize(j->nodes_delivered) <= mayhave) {
            /* If we don't know the job or our set of nodes that may have
             * the job is not larger than the sender, reply with GOTACK. */
            int known = j ? 1 : 0;
            clusterSendGotAck(sender,hdr->data.jobid.job.id,known);
        } else {
            /* We have the job but we know more nodes that may have it
             * than the sender, if we are here. Don't reply with GOTACK unless
             * mayhave is 0 (the sender just received an ACK from client about
             * a job it does not know), in order to let the sender delete it. */
            if (mayhave == 0)
                clusterSendGotAck(sender,hdr->data.jobid.job.id,1);
            /* Anyway, start a GC about this job. Because one of the two is
             * true:
             * 1) mayhave in the sender is 0, so it's up to us to start a GC
             *    because the sender has just a dummy ACK.
             * 2) Or, we prevented the sender from finishing the GC since
             *    we are not replying with GOTACK, and we need to take
             *    responsability to evict the job in the cluster. */
            tryJobGC(j);
        }
    } else if (type == CLUSTERMSG_TYPE_GOTACK) {
        if (!sender) return 1;
        uint32_t known = ntohl(hdr->data.jobid.job.aux);

        job *j = lookupJob(hdr->data.jobid.job.id);
        if (j) gotAckReceived(sender,j,known);
    } else if (type == CLUSTERMSG_TYPE_DELJOB) {
        if (!sender) return 1;
        job *j = lookupJob(hdr->data.jobid.job.id);
        if (j) {
            serverLog(LL_VERBOSE,"RECEIVED DELJOB FOR JOB %.*s",JOB_ID_LEN,j->id);
            unregisterJob(j);
            freeJob(j);
        }
    } else if (type == CLUSTERMSG_TYPE_ENQUEUE) {
        if (!sender) return 1;
        uint32_t delay = ntohl(hdr->data.jobid.job.aux);

        job *j = lookupJob(hdr->data.jobid.job.id);
        if (j && j->state < JOB_STATE_QUEUED) {
            /* Discard this message if the queue is paused in input. */
            queue *q = lookupQueue(j->queue);
            if (q == NULL || !(q->flags & QUEUE_FLAG_PAUSED_IN)) {
                /* We received an ENQUEUE message: consider this node as the
                 * first to queue the job, so no need to broadcast a QUEUED
                 * message the first time we queue it. */
                j->flags &= ~JOB_FLAG_BCAST_QUEUED;
                serverLog(LL_VERBOSE,"RECEIVED ENQUEUE FOR JOB %.*s",
                          JOB_ID_LEN,j->id);
                if (delay == 0) {
                    enqueueJob(j,0);
                } else {
                    updateJobRequeueTime(j,server.mstime+delay*1000);
                }
            }
        }
    } else if (type == CLUSTERMSG_TYPE_QUEUED ||
               type == CLUSTERMSG_TYPE_WORKING) {
        if (!sender) return 1;

        job *j = lookupJob(hdr->data.jobid.job.id);
        if (j && j->state <= JOB_STATE_QUEUED) {
            serverLog(LL_VERBOSE,"UPDATING QTIME FOR JOB %.*s",JOB_ID_LEN,j->id);
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
            if (j->state == JOB_STATE_QUEUED &&
                (type == CLUSTERMSG_TYPE_WORKING ||
                 compareNodeIDsByJob(sender,myself,j) > 0))
            {
                dequeueJob(j);
            }

            /* Update the time at which this node will attempt to enqueue
             * the message again. */
            if (j->retry) {
                j->flags |= JOB_FLAG_BCAST_WILLQUEUE;
                updateJobRequeueTime(j,server.mstime+
                                       j->retry*1000+
                                       randomTimeError(DISQUE_TIME_ERR));
            }

            /* Update multiple deliveries counters. */
            if (type == CLUSTERMSG_TYPE_QUEUED) {
                if (hdr->mflags[0] & CLUSTERMSG_FLAG0_INCR_NACKS)
                    j->num_nacks++;
                if (hdr->mflags[0] & CLUSTERMSG_FLAG0_INCR_DELIV)
                    j->num_deliv++;
            }
        } else if (j && j->state == JOB_STATE_ACKED) {
            /* Some other node queued a message that we have as
             * already acknowledged. Try to force it to drop it. */
            clusterSendSetAck(sender,j);
        }
    } else if (type == CLUSTERMSG_TYPE_WILLQUEUE) {
        if (!sender) return 1;

        job *j = lookupJob(hdr->data.jobid.job.id);
        if (j) {
            if (j->state == JOB_STATE_ACTIVE ||
                j->state == JOB_STATE_QUEUED)
            {
                dictAdd(j->nodes_delivered,sender->name,sender);
            }
            if (j->state == JOB_STATE_QUEUED)
                clusterBroadcastQueued(j,CLUSTERMSG_NOFLAGS);
            else if (j->state == JOB_STATE_ACKED) clusterSendSetAck(sender,j);
        }
    } else if (type == CLUSTERMSG_TYPE_NEEDJOBS ||
               type == CLUSTERMSG_TYPE_PAUSE)
    {
        if (!sender) return 1;
        uint32_t qnamelen = ntohl(hdr->data.queueop.about.qnamelen);
        uint32_t count = ntohl(hdr->data.queueop.about.aux);
        robj *qname = createStringObject(hdr->data.queueop.about.qname,
                                         qnamelen);
        serverLog(LL_VERBOSE,"RECEIVED %s FOR QUEUE %s (%d)",
            (type == CLUSTERMSG_TYPE_NEEDJOBS) ? "NEEDJOBS" : "PAUSE",
            (char*)qname->ptr,count);
        if (type == CLUSTERMSG_TYPE_NEEDJOBS)
            receiveNeedJobs(sender,qname,count);
        else
            receivePauseQueue(qname,count);
        decrRefCount(qname);
    } else {
        serverLog(LL_WARNING,"Received unknown packet type: %d", type);
    }
    return 1;
}

/* -----------------------------------------------------------------------------
 * CLUSTER job related messages
 * -------------------------------------------------------------------------- */

/* Broadcast a REPLJOB message to 'repl' nodes, and populates the list of jobs
 * that may have the job.
 *
 * If there are already nodes that received the message, additional
 * 'repl' nodes will be added to the list (if possible), and the message
 * broadcasted again to all the nodes that may already have the message,
 * plus the new ones.
 *
 * However for nodes for which we already have the GOTJOB reply, we flag
 * the message with the NOREPLY flag. If the 'noreply' argument of the function
 * is true we also flag the message with NOREPLY regardless of the fact the
 * node we are sending REPLJOB to already replied or not.
 *
 * Nodes are selected from server.cluster->reachable_nodes list.
 * The function returns the number of new (additional) nodes that MAY have
 * received the message. */
int clusterReplicateJob(job *j, int repl, int noreply) {
    int i, added = 0;

    if (repl <= 0) return 0;

    /* Add the specified number of nodes to the list of receivers. */
    clusterShuffleReachableNodes();
    for (i = 0; i < server.cluster->reachable_nodes_count; i++) {
        clusterNode *node = server.cluster->reachable_nodes[i];

        if (node->link == NULL) continue; /* No link, no party... */
        if (dictAdd(j->nodes_delivered,node->name,node) == DICT_OK) {
            /* Only counts non-duplicated nodes. */
            added++;
            if (--repl == 0) break;
        }
    }

    /* Resend the message to all the past and new nodes. */
    unsigned char buf[sizeof(clusterMsg)], *payload;
    clusterMsg *hdr = (clusterMsg*) buf;
    uint32_t totlen;

    sds serialized = serializeJob(sdsempty(),j,SER_MESSAGE);

    totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
    totlen += sizeof(clusterMsgDataJob) -
              sizeof(hdr->data.jobs.serialized.jobs_data) +
              sdslen(serialized);

    clusterBuildMessageHdr(hdr,CLUSTERMSG_TYPE_REPLJOB);
    hdr->data.jobs.serialized.numjobs = htonl(1);
    hdr->data.jobs.serialized.datasize = htonl(sdslen(serialized));
    hdr->totlen = htonl(totlen);

    if (totlen < sizeof(buf)) {
        payload = buf;
    } else {
        payload = zmalloc(totlen);
        memcpy(payload,buf,sizeof(clusterMsg));
        hdr = (clusterMsg*) payload;
    }
    memcpy(hdr->data.jobs.serialized.jobs_data,serialized,sdslen(serialized));
    sdsfree(serialized);

    /* Actual delivery of the message to the list of nodes. */
    dictIterator *di = dictGetIterator(j->nodes_delivered);
    dictEntry *de;

    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);
        if (node == myself) continue;

        /* We ask for reply only if 'noreply' is false and the target node
         * did not already replied with GOTJOB. */
        int acked = j->nodes_confirmed && dictFind(j->nodes_confirmed,node);
        if (noreply || acked) {
            hdr->mflags[0] |= CLUSTERMSG_FLAG0_NOREPLY;
        } else {
            hdr->mflags[0] &= ~CLUSTERMSG_FLAG0_NOREPLY;
        }

        /* If the target node acknowledged the message already, send it again
         * only if there are additional nodes. We want the target node to refresh
         * its list of receivers. */
        if (node->link && !(acked && added == 0))
            clusterSendMessage(node->link,payload,totlen);
    }
    dictReleaseIterator(di);

    if (payload != buf) zfree(payload);
    return added;
}

/* Helper function to send all the messages that have just a type,
 * a Job ID, and an optional 'aux' additional value. */
void clusterSendJobIDMessage(int type, clusterNode *node, char *id, int aux) {
    unsigned char buf[sizeof(clusterMsg)];
    clusterMsg *hdr = (clusterMsg*) buf;

    if (node->link == NULL) return; /* This is a best effort message. */
    clusterBuildMessageHdr(hdr,type);
    memcpy(hdr->data.jobid.job.id,id,JOB_ID_LEN);
    hdr->data.jobid.job.aux = htonl(aux);
    clusterSendMessage(node->link,buf,ntohl(hdr->totlen));
}

/* Like clusterSendJobIDMessage(), but sends the message to the specified set
 * of nodes (excluding myself if included in the set of nodes). */
void clusterBroadcastJobIDMessage(dict *nodes, char *id, int type, uint32_t aux, unsigned char flags) {
    dictIterator *di = dictGetIterator(nodes);
    dictEntry *de;
    unsigned char buf[sizeof(clusterMsg)];
    clusterMsg *hdr = (clusterMsg*) buf;

    /* Build the message one time, send the same to everybody. */
    clusterBuildMessageHdr(hdr,type);
    memcpy(hdr->data.jobid.job.id,id,JOB_ID_LEN);
    hdr->data.jobid.job.aux = htonl(aux);
    hdr->mflags[0] = flags;

    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);
        if (node == myself) continue;
        if (node->link)
            clusterSendMessage(node->link,buf,ntohl(hdr->totlen));
    }
    dictReleaseIterator(di);
}

/* Send a GOTJOB message to the specified node, if connected.
 * GOTJOB messages only contain the ID of the job, and are acknowledges
 * that the job was replicated to a target node. The receiver of the message
 * will be able to reply to the client that the job was accepted by the
 * system when enough nodes have a copy of the job. */
void clusterSendGotJob(clusterNode *node, job *j) {
    clusterSendJobIDMessage(CLUSTERMSG_TYPE_GOTJOB,node,j->id,0);
}

/* Force the receiver to queue a job, if it has that job in an active
 * state. */
void clusterSendEnqueue(clusterNode *node, job *j, uint32_t delay) {
    clusterSendJobIDMessage(CLUSTERMSG_TYPE_ENQUEUE,node,j->id,delay);
}

/* Tell the receiver that the specified job was just re-queued by the
 * sender, so it should update its qtime to the future, and if the job
 * is also queued by the receiving node, to drop it from the queue if the
 * sender has a greater node ID (so that we try in a best-effort way to
 * avoid useless multi deliveries).
 *
 * This message is sent to all the nodes we believe may have a copy
 * of the message and are reachable. */
void clusterBroadcastQueued(job *j, unsigned char flags) {
    serverLog(LL_VERBOSE,"BCAST QUEUED: %.*s",JOB_ID_LEN,j->id);
    clusterBroadcastJobIDMessage(j->nodes_delivered,j->id,
                                 CLUSTERMSG_TYPE_QUEUED,0,flags);
}

/* WORKING is like QUEUED, but will always force the receiver to dequeue. */
void clusterBroadcastWorking(job *j) {
    serverLog(LL_VERBOSE,"BCAST WORKING: %.*s",JOB_ID_LEN,j->id);
    clusterBroadcastJobIDMessage(j->nodes_delivered,j->id,
                                 CLUSTERMSG_TYPE_WORKING,0,CLUSTERMSG_NOFLAGS);
}

/* Send a DELJOB message to all the nodes that may have a copy. */
void clusterBroadcastDelJob(job *j) {
    serverLog(LL_VERBOSE,"BCAST DELJOB: %.*s",JOB_ID_LEN,j->id);
    clusterBroadcastJobIDMessage(j->nodes_delivered,j->id,
                                 CLUSTERMSG_TYPE_DELJOB,0,CLUSTERMSG_NOFLAGS);
}

/* Tell the receiver to reply with a QUEUED message if it has the job
 * already queued, to prevent us from queueing it in the next few
 * milliseconds. */
void clusterSendWillQueue(job *j) {
    serverLog(LL_VERBOSE,"BCAST WILLQUEUE: %.*s",JOB_ID_LEN,j->id);
    clusterBroadcastJobIDMessage(j->nodes_delivered,j->id,
                                 CLUSTERMSG_TYPE_WILLQUEUE,0,CLUSTERMSG_NOFLAGS);
}

/* Force the receiver to acknowledge the job as delivered. */
void clusterSendSetAck(clusterNode *node, job *j) {
    uint32_t maxowners = dictSize(j->nodes_delivered);
    clusterSendJobIDMessage(CLUSTERMSG_TYPE_SETACK,node,j->id,maxowners);
}

/* Acknowledge a SETACK message. */
void clusterSendGotAck(clusterNode *node, char *jobid, int known) {
    clusterSendJobIDMessage(CLUSTERMSG_TYPE_GOTACK,node,jobid,known);
}

/* Send a NEEDJOBS message to the specified set of nodes. */
void clusterSendNeedJobs(robj *qname, int numjobs, dict *nodes) {
    uint32_t totlen, qnamelen = sdslen(qname->ptr);
    uint32_t alloclen;
    clusterMsg *hdr;

    serverLog(LL_VERBOSE,"Sending NEEDJOBS for %s %d, %d nodes",
        (char*)qname->ptr, (int)numjobs, (int)dictSize(nodes));

    totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
    totlen += sizeof(clusterMsgDataQueueOp) - 8 + qnamelen;
    alloclen = totlen;
    if (alloclen < (int)sizeof(clusterMsg)) alloclen = sizeof(clusterMsg);
    hdr = zmalloc(alloclen);

    clusterBuildMessageHdr(hdr,CLUSTERMSG_TYPE_NEEDJOBS);
    hdr->data.queueop.about.aux = htonl(numjobs);
    hdr->data.queueop.about.qnamelen = htonl(qnamelen);
    memcpy(hdr->data.queueop.about.qname, qname->ptr, qnamelen);
    hdr->totlen = htonl(totlen);
    clusterBroadcastMessage(nodes,hdr,totlen);
    zfree(hdr);
}

/* Send a PAUSE message to the specified set of nodes. */
void clusterSendPause(robj *qname, uint32_t flags, dict *nodes) {
    uint32_t totlen, qnamelen = sdslen(qname->ptr);
    uint32_t alloclen;
    clusterMsg *hdr;

    serverLog(LL_VERBOSE,"Sending PAUSE for %s flags=%d, %d nodes",
        (char*)qname->ptr, (int)flags, (int)dictSize(nodes));

    totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
    totlen += sizeof(clusterMsgDataQueueOp) - 8 + qnamelen;
    alloclen = totlen;
    if (alloclen < (int)sizeof(clusterMsg)) alloclen = sizeof(clusterMsg);
    hdr = zmalloc(alloclen);

    clusterBuildMessageHdr(hdr,CLUSTERMSG_TYPE_PAUSE);
    hdr->data.queueop.about.aux = htonl(flags);
    hdr->data.queueop.about.qnamelen = htonl(qnamelen);
    memcpy(hdr->data.queueop.about.qname, qname->ptr, qnamelen);
    hdr->totlen = htonl(totlen);
    clusterBroadcastMessage(nodes,hdr,totlen);
    zfree(hdr);
}

/* Same as clusterSendPause() but broadcasts the message to the
 * whole cluster. */
void clusterBroadcastPause(robj *qname, uint32_t flags) {
    clusterSendPause(qname,flags,server.cluster->nodes);
}

/* Send a YOURJOBS message to the specified node, with a serialized copy of
 * the jobs referneced by the 'jobs' array and containing 'count' jobs. */
void clusterSendYourJobs(clusterNode *node, job **jobs, uint32_t count) {
    unsigned char buf[sizeof(clusterMsg)], *payload;
    clusterMsg *hdr = (clusterMsg*) buf;
    uint32_t totlen, j;

    if (!node->link) return;

    serverLog(LL_VERBOSE,"Sending %d jobs to %.40s", (int)count,node->name);

    totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
    totlen += sizeof(clusterMsgDataJob) -
              sizeof(hdr->data.jobs.serialized.jobs_data);

    sds serialized = sdsempty();
    for (j = 0; j < count; j++)
        serialized = serializeJob(serialized,jobs[j],SER_MESSAGE);
    totlen += sdslen(serialized);

    clusterBuildMessageHdr(hdr,CLUSTERMSG_TYPE_YOURJOBS);
    hdr->data.jobs.serialized.numjobs = htonl(count);
    hdr->data.jobs.serialized.datasize = htonl(sdslen(serialized));
    hdr->totlen = htonl(totlen);

    if (totlen < sizeof(buf)) {
        payload = buf;
    } else {
        payload = zmalloc(totlen);
        memcpy(payload,buf,sizeof(clusterMsg));
        hdr = (clusterMsg*) payload;
    }
    memcpy(hdr->data.jobs.serialized.jobs_data,serialized,sdslen(serialized));
    sdsfree(serialized);
    clusterSendMessage(node->link,payload,totlen);
    if (payload != buf) zfree(payload);
}
