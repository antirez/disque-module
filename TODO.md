Technical debts in Disque as a module
=====================================

(this list is in no way complete, because this version of Disque as a
 module is an alpha at best)

* When deserializing jobs, is it correct even in the new module
  setup that we want to remove the node from the list of nodes that
  may have a copy? In the original Disque this was mandatory because we
  referenced the node object itself, but now we just have the ID, so it
  may make more sense to remember the ID. But then during garbage
  collection, remove the nodes that we don't know anything about.

* Implement "leaving":
    - Set the "leaving" flag if the global vars leaving is true.
    - Process the leaving flag when a message is received: if set make sure to
      populate the radix tree with the nodes that are leaving.
    - When a node gets the "leaving" command, broadcast this information
      periodically to all the nodes in the cron timer callback.
    - From time to time remove the "leaving" nodes from the radix tree if the
      last leaving report is older than a few minutes.

* When loading jobs, by default re-queue them if they are in queued state, and
  don't do it only if a specific "safety" option is enabled to ensure
  semantics of at-most-once jobs even with weak AOF fsync policies.
  Now we generate DEQUEUE commands in the AOF for at-most-once jobs that
  are removed from the queue.

* Have an option to generate DEQUEUE in the AOF for every job, not just
  jobs with retry set to 0. More AOF traffic but less chances of re-delivery
  on restarts.

* Fix blocking in same queue mulitple times. Grep for blockForJobs().

* Implement AOF and AOF rewriting, or an alternative file-based and threaded
  persistence layer in a spool directory.
    - Handle the persistence in DISQUE FLUSHALL.
    - Fail executing ADDJOB, if the RDB preamble is not
      active in the AOF configuration. Check periodically and update
      a global state.

* Implement the Disque original test in the module version: work in progress.

* Implement `DISQUE HELLO` and `DISQUE INFO`.

* Implement QSCAN and JSCAN, but using the radix tree and thus returning no cursor: we'll just use the last returned item as cursor.
