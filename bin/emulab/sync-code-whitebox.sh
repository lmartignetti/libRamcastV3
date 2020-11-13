#!/usr/bin/env bash

CONTROL_IP=10.10.1.1

usage () {
    echo "Usage: $0 [folder-to-sync] number-of-node"
}

if [ $# -gt 2 ] || [ $# -lt 1 ]; then
    usage
    exit
fi

if [ $# = "1" ]; then
    DIR="/users/lel/apps/libramcast/"
    NODE_COUNT=$1
else
    DIR=$1
    NODE_COUNT=$2
fi

for nid in `seq 0 $NODE_COUNT`; do

    if [ $nid = $CONTROL_IP ]; then
        echo "Ignoring control nid: $nid"
    else
#        echo "Syncing to: node$nid"
        rsync -e "ssh -o StrictHostKeyChecking=no" -rav --exclude-from=/users/lel/apps/libramcast/libRamcastV2/bin/emulab/.deployIgnore $DIR/* node$nid:$DIR/ &
    fi
#    echo "========================================"
done
