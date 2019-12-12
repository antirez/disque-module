# Disque specific Tcl procedures.

# Return the number of instances having the specified flag set from the
# POV of the instance 'id'.
proc count_cluster_nodes_with_flag {id flag} {
    set count 0
    foreach node [get_cluster_nodes $id] {
        if {[has_flag $node $flag]} {incr count}
    }
    return $count
}

proc randomQueue {} {
    return "queue-[randstring 40 40 alpha]"
}
