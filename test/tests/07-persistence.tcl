source "../tests/includes/init-tests.tcl"
source "../tests/includes/job-utils.tcl"

for {set i 0} {$i < 5} {incr i} {
    set qname [randomQueue]
    set repl_level 3
    set body "xxxyyy$j"
    set target_id [randomInt $::instances_count]
    test "Job replicated to N nodes is delivered after mass restart #$i" {
        set id [R $target_id addjob $qname $body 5000 replicate $repl_level retry 1]
        set job [R $target_id show $id]
        assert {$id ne {}}

        # Kill all nodes.
        foreach_redis_id j {
            kill_instance redis $j
        }

        # Restart them all.
        foreach_redis_id j {
            restart_instance redis $j
        }

        # Wait for the job to be re-queued after restart.
        wait_for_condition 1000 50 {
            [count_job_copies $job queued] > 0
        } else {
            fail "Job not requeued after some time"
        }

        # Verify it's actually our dear job
        set queueing_id [lindex [get_job_instances $job queued] 0]
        assert {$queueing_id ne {}}
        set myjob [lindex [R $queueing_id getjob from $qname] 0]
        assert {[lindex $myjob 0] eq $qname}
        assert {[lindex $myjob 1] eq $id}
        assert {[lindex $myjob 2] eq $body}
    }
}
