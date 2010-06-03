#!/bin/bash

# Example script to run a workload.

YCSB_HOME=.
if [ $# -lt 3 ] ; then
	SCRIPT_NAME=`basename $0`
    echo "USAGE:  $SCRIPT_NAME {load | t} {number of threads} {workload file in $YCSB_HOME/workloads}"
    exit
fi

java \
-cp ${YCSB_HOME}/build/ycsb.jar:${YCSB_HOME}/db/mongodb/lib/mongo-2.0-rc4.jar \
com.yahoo.ycsb.Client \
-${1} \
-threads ${2} \
-db com.yahoo.ycsb.db.MongoDbClient \
-p measurementtype=histogram \
-p mongodb.url=mongodb://localhost:27017 \
-p mongodb.database=ycsb \
-P ${YCSB_HOME}/workloads/${3}


