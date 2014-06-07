#!/bin/sh

# load global configurations

CUR_DIR=`dirname "$0"`
BASE_DIR=$CUR_DIR/..
. $BASE_DIR/conf/config.sh
. $BASE_DIR/bin/functions.sh

# topology configuration

TOPOLOGY_CLASS=SOL
TOPOLOGY_NAME=SOL

TOPOLOGY_LEVEL=3
MESSAGE_SIZE=100

WORKERS=4
ACKERS=$WORKERS
PENDING=200

COMPONENT=topology.component
SPOUT_NUM=4
BOLT_NUM=4

TOPOLOGY_CONF=$TOPOLOGY_CONF,topology.name=$TOPOLOGY_NAME,topology.workers=$WORKERS,topology.acker.executors=$ACKERS,topology.max.spout.pending=$PENDING,topology.level=$TOPOLOGY_LEVEL,$COMPONENT.spout_num=$SPOUT_NUM,$COMPONENT.bolt_num=$BOLT_NUM

echo "========== running SOL =========="
run_benchmark
