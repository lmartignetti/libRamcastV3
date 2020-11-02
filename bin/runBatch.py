#!/usr/bin/python
import common
import sys

if len(sys.argv) not in [5]:
    NUM_GROUPS = 1
    NUM_PROCESSES = [3]
    NUM_CLIENTS = [1]
    NUM_RUNS = 1
else:
    NUM_GROUPS = common.iarg(1)
    NUM_PROCESSES = [common.iarg(2)]
    NUM_CLIENTS = [common.iarg(3)]
    NUM_RUNS = common.iarg(4)

NUM_DEST = 1
DURATION = 60
WARMUP = 20

PROFILING = True
PROFILING = False
DEBUG = False
DEBUG = False

if PROFILING: DURATION = 9999

if NUM_DEST < NUM_GROUPS: NUM_DEST = NUM_GROUPS


def run():
    for p in NUM_PROCESSES:
        for c in NUM_CLIENTS:
            experimentCmd = ' '.join([str(val) for val in
                                      ['python ./runAllOnce.py', NUM_GROUPS, p, c, NUM_DEST,
                                       DURATION, WARMUP,
                                       DEBUG, PROFILING]])
            print experimentCmd
            common.localcmd(experimentCmd)


for i in range(0, NUM_RUNS):
    run()
