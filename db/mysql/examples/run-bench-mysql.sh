#!/bin/bash

# Example script to run a workload.

YCSB_HOME=.
if [ $# -lt 3 ] ; then
	SCRIPT_NAME=`basename $0`
    echo "USAGE:  $SCRIPT_NAME {load | t} {number of threads} {workload file in $YCSB_HOME/workloads}"
    exit
fi

java \
-cp ${YCSB_HOME}/build/ycsb.jar:${YCSB_HOME}/db/mysql/lib/c3p0-0.9.1.2.jar:${YCSB_HOME}/db/mysql/lib/mysql-connector-java-5.1.12.jar \
com.yahoo.ycsb.Client \
-${1} \
-threads ${2} \
-db com.yahoo.ycsb.db.MysqlClient \
-p mysql.driver=com.mysql.jdbc.Driver \
-p mysql.url=jdbc:mysql://localhost:3306/ycsb \
-p mysql.user=root \
-p mysql.password=quaker17 \
-p mysql.pool.min=10 \
-p mysql.pool.max=20 \
-p mysql.pool.acquireIncrement=5 \
-P ${YCSB_HOME}/workloads/${3}



