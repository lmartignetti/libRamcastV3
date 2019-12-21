#!/usr/bin/python
import common

NUM_GROUPS = 1
NUM_PROCESSES = [3]
NUM_CLIENTS = [3]

NUM_DEST = 1
DURATION = 60
WARMUP = 20

PROFILING = True
PROFILING = False
DEBUG = True
DEBUG = False

if PROFILING: DURATION = 9999

if NUM_DEST < NUM_GROUPS: NUM_DEST = NUM_GROUPS

def run():
    for p in NUM_PROCESSES:
        # for c in range(1, p * NUM_GROUPS + 1):
        for c in NUM_CLIENTS:
            experimentCmd = ' '.join([str(val) for val in
                                      ['python ./runAllOnce.py', NUM_GROUPS, p, c, NUM_DEST,
                                       DURATION, WARMUP,
                                       DEBUG, PROFILING]])
            print experimentCmd
            common.localcmd(experimentCmd)


run()
