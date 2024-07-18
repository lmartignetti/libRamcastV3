#!/usr/bin/python

import getpass

import sys

import common

NODES = common.RDMA_NODES
ZKNODES = common.ZK_NODES
user = getpass.getuser()

if len(sys.argv) > 1:
    print("Killing runAllOnce.py")
    common.localcmd("pkill -9 runAllOnce.py")

# common.localcmd('rm -rf "/home/long/apps/ScalableSMR/zookeeper/data/1/version-2"  "/home/long/apps/ScalableSMR/zookeeper/data/2/version-2" "/home/long/apps/ScalableSMR/zookeeper/data/3/version-2"')

# cleaning remote nodes
for node in NODES:# + ZKNODES:
    # common.sshcmdbg(node, "pkill java &> /dev/null")
    # common.sshcmdbg(node,"ps ax | grep 'java' | awk -F ' ' '{print $1}' | xargs sudo kill -9")
    # common.sshcmdbg(node,"ps ax | grep 'proposer-acceptor' | awk -F ' ' '{print $1}' | xargs kill -9")
    common.sshcmdbg(node, "killall -9 -u " + user + " &> /dev/null")
