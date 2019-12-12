#!/bin/bash

# Clone Redis if it's not there
if [ ! -d "redis" ]; then
    git clone https://github.com/antirez/redis.git
fi

# Remove the Redis Cluster tests
rm -rf redis/tests/cluster/tests/*

# Replace them with the Disque tests
cp run.tcl redis/tests/cluster/
cp tests/*.tcl redis/tests/cluster/tests/
cp -a includes redis/tests/cluster/tests/

# Run the test
(cd redis; make; ./runtest-cluster $*)
