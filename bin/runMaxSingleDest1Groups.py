#!/usr/bin/python
import datetime
import json
import os
import time
from datetime import datetime

import common

NUM_RUNS = 1
NUM_GROUPS = [5]  # 1 bench group and 3 clients groups
NUM_PROCESSES = 3
NUM_DEST = [1]
NUM_CLIENT_PER_DESTINATION = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
NUM_CLIENT_PER_DESTINATION = [13]

PACKAGE_SIZE = [5120, 6144, 7168]
PACKAGE_SIZE = [1024]
PACKAGE_SIZE = [98, 512, 1024, 2048, 4096, 5120, 6144, 7168, 8192, 16384, 32768, 65536]

DURATION = 60
WARMUP = 20

PROFILING = False
DEBUG = False
DELAY = False

# RDMA config
CONF_QUEUE_LENGTH = 8
CONF_NUM_PROCESSES = NUM_PROCESSES
CONF_SERVICE_TIMEOUT = 1
CONF_POLLING = True
CONF_MAX_INLINE = 64
CONF_PORT = 9000
CONF_SIGNAL_INTERVAL = 4

if PROFILING: DURATION = 9999


def run():
    for size in PACKAGE_SIZE:
        for g in NUM_GROUPS:
            for d in NUM_DEST:
                for c in NUM_CLIENT_PER_DESTINATION:
                    orchestra(g, d, c, NUM_PROCESSES, size)


# =======================================================================================================================

java_cmd = "java -XX:+UseConcMarkSweepGC -XX:SurvivorRatio=15 -XX:+UseParNewGC -Xms3g -Xmx3g"
if PROFILING:
    java_cmd = java_cmd + " -agentpath:" + common.PATH_PROFILING
if DEBUG:
    log4j_conf = common.PATH_LIBRAMCAST_HOME + '/bin/logback-debug.xml'
    java_cmd = java_cmd + " -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
else:
    log4j_conf = common.PATH_LIBRAMCAST_HOME + '/bin/logback.xml'

java_cmd = java_cmd + " -Dlogback.configurationFile=" + log4j_conf
java_cmd = java_cmd + common.JAVA_CLASSPATH

available_nodes = common.RDMA_NODES


def gen_config(num_process_per_group, node_used, config_file):
    config = dict()
    config["queueLength"] = CONF_QUEUE_LENGTH
    config["zkHost"] = common.ZK_HOST
    config["nodePerGroup"] = CONF_NUM_PROCESSES
    config["servicetimeout"] = CONF_SERVICE_TIMEOUT
    config["polling"] = CONF_POLLING
    config["maxinline"] = CONF_MAX_INLINE
    config["signalInterval"] = CONF_SIGNAL_INTERVAL
    config["debug"] = DEBUG
    config["delay"] = DELAY

    config["group_members"] = []
    i = 0
    p = 0
    g = 0

    while i < node_used:

        config["group_members"].append({
            "gid": g,  # if role == ROLE_BOTH or role == ROLE_SERVER else -1,
            "nid": p,  # if role == ROLE_BOTH or role == ROLE_SERVER else -1,
            "port": CONF_PORT,
            "host": available_nodes[i],
            # "role": role
        })
        i += 1
        p += 1
        if p == num_process_per_group:
            p = 0
            g += 1
    systemConfigurationFile = open(config_file, 'w')
    json.dump(config, systemConfigurationFile, sort_keys=False, indent=4, ensure_ascii=False)
    systemConfigurationFile.flush()
    systemConfigurationFile.close()


def orchestra(num_groups, num_destinations, num_clients, num_process_per_group, package_size):
    debug_log_dir = '{}/bin'.format(common.PATH_LIBRAMCAST_HOME)
    log_dir = '{}/logs/max-single-dest-1-group/{}d-{}g-{}c-{}p-{}b-{}'.format(common.PATH_LIBRAMCAST_HOME,
                                                                              num_destinations,
                                                                              num_groups, num_clients,
                                                                              num_process_per_group,
                                                                              package_size,
                                                                              datetime.now().strftime(
                                                                                  '%Y-%m-%d-%H-%M-%S'))
    config_file = '{}/bin/systemConfigs/generatedSystemConfig{}g{}p.json'.format(common.PATH_LIBRAMCAST_HOME,
                                                                                 num_groups, num_process_per_group)

    num_servers = num_process_per_group * num_groups
    node_used = max(num_servers, num_clients)

    gen_config(num_process_per_group, node_used, config_file)
    time.sleep(1)

    if common.ENV_EMULAB:
        # need to sync this sysConfig with other instances
        sync_script = '/users/lel/apps/libramcast/libRamcastV3/bin/emulab/sync-code.sh'
        config_dir = '/users/lel/apps/libramcast/libRamcastV3/bin/systemConfigs'
        os.system("{}  {} {}".format(sync_script, config_dir, str(node_used + len(common.EMULAB_DEAD_NODES))))
        time.sleep(3)

    i = 0
    p = 0
    g = 0
    cmds = []
    while i < node_used:
        cmd = [java_cmd, '-DLOG_DIR=' + debug_log_dir, '-DGROUPID=' + str(g), '-DNODEID=' + str(p), common.CLASS_BENCH,
               "-c", config_file, "-s", package_size,
               "-gid", g, "-nid", p, "-cid", i,
               "-d", DURATION, "-gh", common.GATHERER_HOST, "-gp", common.GATHERER_PORT, "-gd", log_dir, "-gw",
               WARMUP * 1000]
        if num_process_per_group - 1 <= i < num_process_per_group - 1 + num_clients:
            # -1 so that one process in the bench group will be client to measure latency
            cmd += ["-df", "0", "-dc 1", "-isClient", "1"]
        cmdString = ' '.join([str(val) for val in cmd])
        cmds.append([available_nodes[i], cmdString])
        i += 1
        p += 1
        if p == NUM_PROCESSES:
            p = 0
            g += 1

    for cmd in cmds:
        print cmd[0], cmd[1]
        common.sshcmdbg(cmd[0], cmd[1])

    # start gatherer
    cmd = [java_cmd, common.CLASS_GATHERER, WARMUP * 1000, common.GATHERER_PORT, log_dir,
           "throughput", "client_overall", num_clients,
           "latency", "client_overall", 1,
           "latencydistribution", "client_overall", 1,
           ]

    cmdString = ' '.join([str(val) for val in cmd])
    print cmdString
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
