#!/usr/bin/python
import common

# Latency when one client multicasts to all groups versus number of groups in configuration
# NUM_GROUPS = NUM_DEST
# NUM_CLIENT_PER_DESTINATION=1

# Latency when one client multicasts to k groups in a system with 8 groups.
# NUM_GROUPS 8
# Inc NUM_DEST
# NUM_CLIENT_PER_DESTINATION=1

# if len(sys.argv) not in [6]:
NUM_RUNS = 1
NUM_GROUPS = [1, 2]
NUM_GROUPS = [6]
NUM_GROUPS = [4]
NUM_PROCESSES = 3
NUM_DEST = [2, 4, 6]
NUM_DEST = [1]
NUM_CLIENT_PER_DESTINATION = [5]
NUM_CLIENT_PER_DESTINATION = [1, 2, 3, 4, 5, 6, 7, 8, 9]
# else:
#     NUM_RUNS = common.iarg(1)
#     NUM_GROUPS = common.iarg(2)
#     NUM_PROCESSES = common.iarg(3)
#     NUM_DEST = common.iarg(4)
#     NUM_CLIENT_PER_DESTINATION = common.iarg(5)

DURATION = 60
WARMUP = 20

PROFILING = True
PROFILING = False
DEBUG = False
DEBUG = True

# NUM_CLIENTS = NUM_DEST * NUM_CLIENT_PER_DESTINATION

if PROFILING: DURATION = 9999


# if NUM_DEST < NUM_GROUPS: NUM_DEST = NUM_GROUPS


def run():
    for g in NUM_GROUPS:
        for d in NUM_DEST:
            for c in NUM_CLIENT_PER_DESTINATION:
                experimentCmd = ' '.join([str(val) for val in
                                          ['python ./runAllOnce.py', g, NUM_PROCESSES, d, c, DURATION, WARMUP, DEBUG,
                                           PROFILING]])
                print experimentCmd
                common.localcmd(experimentCmd)


for i in range(0, NUM_RUNS):
    run()
