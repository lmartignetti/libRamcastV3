#!/usr/bin/python
import datetime
import time
from datetime import datetime

import common

PROTOCOLS = ['RDMA-SEND-RECEIVE', 'RDMA-READ', 'TCP']
PROTOCOLS = ['RDMA-SEND-RECEIVE', 'RDMA-READ']
PROTOCOLS = ['TCP']

NUM_RUNS = 1
PACKAGE_SIZE = [64, 512, 1024, 16384, 32768, 65536]
PACKAGE_SIZE = [64, 512, 1024, 16384, 32768]
PACKAGE_SIZE = [64]
PACKAGE_SIZE = [64, 512, 1024, 2048, 4096, 8192, 16384, 32768]

DURATION = 60
WARMUP = 20

PROFILING = False
DEBUG = True
DELAY = False
DEBUG = False

BIND_PORT = 5000


def run():
    for protocol in PROTOCOLS:
        print "running benchmark for", protocol

        if protocol == 'RDMA-SEND-RECEIVE':
            CLASS_BENCH_SERVER = common.CLASS_RDMA_SEND_RECEIVE_BENCH_SERVER
            CLASS_BENCH_CLIENT = common.CLASS_RDMA_SEND_RECEIVE_BENCH_CLIENT
            protocol_log_dir = 'rdma-send-receive-bench'
        elif protocol == 'RDMA-READ':
            CLASS_BENCH_SERVER = common.CLASS_RDMA_READ_BENCH_SERVER
            CLASS_BENCH_CLIENT = common.CLASS_RDMA_READ_BENCH_CLIENT
            protocol_log_dir = 'rdma-read'
        elif protocol == 'TCP':
            CLASS_BENCH_SERVER = common.CLASS_TCP_BENCH_SERVER
            CLASS_BENCH_CLIENT = common.CLASS_TCP_BENCH_CLIENT
            protocol_log_dir = 'tcp-bench'
        else:
            print "Protocol is not defined"
            exit(-1)

        for size in PACKAGE_SIZE:
            orchestra(size, CLASS_BENCH_SERVER, CLASS_BENCH_CLIENT, protocol_log_dir)


# =======================================================================================================================

java_cmd = "java -XX:+UseConcMarkSweepGC -XX:SurvivorRatio=15 -XX:+UseParNewGC"
if common.ENV_CLUSTER or common.ENV_LOCALHOST:
    java_cmd = java_cmd + " -Xms3g -Xmx3g"
if common.ENV_EMULAB:
    java_cmd = java_cmd + " -Xms12g -Xmx12g"

if PROFILING:
    java_cmd = java_cmd + " -agentpath:" + common.PATH_PROFILING
if DEBUG:
    log4j_conf = common.PATH_LIBRAMCAST_HOME + '/bin/logback-debug.xml'
    if common.ENV_EMULAB is not True: java_cmd = java_cmd + " -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
else:
    log4j_conf = common.PATH_LIBRAMCAST_HOME + '/bin/logback.xml'

java_cmd = java_cmd + " -Dlogback.configurationFile=" + log4j_conf
java_cmd = java_cmd + common.JAVA_CLASSPATH


def orchestra(package_size, CLASS_BENCH_SERVER, CLASS_BENCH_CLIENT, protocol_log_dir):
    client_node = common.RDMA_NODES[1]
    server_node = common.RDMA_NODES[2]

    debug_log_dir = '{}/bin'.format(common.PATH_LIBRAMCAST_HOME)
    log_dir = '{}/logs/{}/{}b-{}'.format(common.PATH_LIBRAMCAST_HOME, protocol_log_dir, package_size,
                                         datetime.now().strftime('%Y-%m-%d-%H-%M-%S'))

    server_cmd = [java_cmd, '-DLOG_DIR=' + debug_log_dir, CLASS_BENCH_SERVER,
                  "-s", package_size, "-sa", server_node, "-sp", BIND_PORT]
    cmdString = ' '.join([str(val) for val in server_cmd])
    print cmdString
    common.sshcmdbg(server_node, cmdString)
    time.sleep(3)

    client_cmd = [java_cmd, '-DLOG_DIR=' + debug_log_dir, CLASS_BENCH_CLIENT,
                  "-s", package_size, "-sa", server_node, "-sp", BIND_PORT,
                  "-d", DURATION, "-gh", common.GATHERER_HOST, "-gp", common.GATHERER_PORT, "-gd", log_dir, "-gw",
                  WARMUP * 1000]
    cmdString = ' '.join([str(val) for val in client_cmd])
    common.sshcmdbg(client_node, cmdString)

    # start gatherer
    cmd = [java_cmd, common.CLASS_GATHERER, WARMUP * 1000, common.GATHERER_PORT, log_dir,
           "throughput", "client_overall", 1,
           "latency", "client_overall", 1,
           "latencydistribution", "client_overall", 1,
           ]

    cmdString = ' '.join([str(val) for val in cmd])
    common.localcmd(cmdString)

    common.localcmd(common.APP_CLEANER)
    time.sleep(1)
    print "===================================\n          Throughput              \n==================================="
    common.localcmd("cat " + log_dir + "/throughput_client_overall_aggregate.log")
    print "===================================\n          Latency                 \n==================================="
    common.localcmd("cat " + log_dir + "/latency_client_overall_average.log")
    print "==================================="


for i in range(0, NUM_RUNS):
    run()
