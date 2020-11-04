#!/bin/bash

usage () {
    echo "Usage: $0 number-of-node"
}

if [ $# -lt 1 ]; then
    usage
    exit
fi

NODES_COUNT=$1
NODE_IDS=`seq 0 $((${NODES_COUNT} - 1))`

for id in ${NODE_IDS} ; do
  if ssh "node${id}" hostname; then
    echo "$TARGET alive"
    ssh "node${id}" "tail /tmp/node-setup.log.* --lines=1";
  else
    echo "$TARGET offline"
  fi
done