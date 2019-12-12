Technical debts in Disque as a module
=====================================

(this list is in no way complete, because this version of Disque as a
 module is an alpha at best)

* Stop the module immediately when loaded in non-cluster mode.

* Implement "leaving":
    - Set the "leaving" flag if the global vars leaving is true.
    - Process the leaving flag when a message is received: if set make sure to
      populate the radix tree with the nodes that are leaving.
    - When a node gets the "leaving" command, broadcast this information
      periodically to all the nodes in the cron timer callback.
    - From time to time remove the "leaving" nodes from the radix tree if the
      last leaving report is older than a few minutes.

* Fix blocking in same queue mulitple times. Grep for blockForJobs().

* Implement AOF and AOF rewriting, or an alternative file-based and threaded
  persistence layer in a spool directory.
    - Handle the persistence in DISQUE FLUSHALL.

* Implement the Disque original test in the module version: work in progress.

* Implement `DISQUE HELLO` and `DISQUE INFO`.

* Implement QSCAN and JSCAN, but using the radix tree and thus returning no cursor: we'll just use the last returned item as cursor.
