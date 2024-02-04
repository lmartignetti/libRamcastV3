#!/usr/bin/python
import datetime
import json
import math
import os
import time
from datetime import datetime

import common

NUM_RUNS = 1
# NUM_GROUPS = [3, 6, 11, 22]  # 1 bench group and 3 clients groups
# NUM_GROUPS = [2]  # 1 bench group and 3 clients groups
NUM_PROCESSES = 3
NUM_DEST = [1]
NUM_CLIENT_PER_DESTINATION = [1]

PACKAGE_SIZE = [98, 512, 1024, 16384, 32768, 65536]
PACKAGE_SIZE = [98]

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
CONF_MAX_INLINE = 250
CONF_PORT = 9000
CONF_SIGNAL_INTERVAL = 8

if PROFILING: DURATION = 9999


def run():
    for size in PACKAGE_SIZE:
        # for g in NUM_GROUPS:
        for d in NUM_DEST:
            for c in NUM_CLIENT_PER_DESTINATION:
                orchestra(d, c, NUM_PROCESSES, size)


# =======================================================================================================================

java_cmd = "java -XX:SurvivorRatio=15 -Xms3g -Xmx3g"
if PROFILING:
    java_cmd = java_cmd + " -agentpath:" + common.PATH_PROFILING
if DEBUG:
    log4j_conf = common.PATH_LIBRAMCAST_HOME + '/bin/logback-debug.xml'
    java_cmd = java_cmd + " -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
else:
    log4j_conf = common.PATH_LIBRAMCAST_HOME + '/bin/logback.xml'

java_cmd = java_cmd + " -Dlogback.configurationFile=" + log4j_conf
java_cmd = java_cmd + common.JAVA_CLASSPATH


def gen_config(num_process_per_group, num_dest, num_client_per_dest, config_file):
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
    available_nodes = common.RDMA_NODES

    config["group_members"] = []
    i = 0
    p = 0
    g = 0

    last_node = None
    last_client_per_node_used = 0

    # first set up nodes in bench groups
    while i < num_dest * num_process_per_group:
        config["group_members"].append({
            "gid": g,
            "nid": p,
            "port": CONF_PORT,
            "host": available_nodes[i],
        })
        i += 1
        p += 1
        if p == num_process_per_group:
            p = 0
            g += 1

    client_nodes = available_nodes[i:]

    remaining_clients = num_client_per_dest * num_dest - num_dest
    clients_per_node = int(math.ceil(remaining_clients * 1.0 / len(client_nodes)))

    # then fill up clients to remaining nodes
    j = 0
    i = 0
    client_per_node_used = 0
    last_node = None

    while j < remaining_clients:
        config["group_members"].append({
            "gid": g,
            "nid": p,
            "port": CONF_PORT + client_per_node_used,
            "host": client_nodes[i],
        })
        last_node = client_nodes[i]
        last_client_per_node_used = client_per_node_used
        p += 1
        j += 1
        client_per_node_used += 1
        if p == num_process_per_group:
            p = 0
            g += 1
        if client_per_node_used == clients_per_node:
            i += 1
            client_per_node_used = 0

    # fill up the last group. these process is just for filling group, not participate in anything
    if last_node is None: last_node = client_nodes[i]
    while p < num_process_per_group:
        last_client_per_node_used += 1
        config["group_members"].append({
            "gid": g,
            "nid": p,
            "port": CONF_PORT + last_client_per_node_used,
            "host": last_node
        })
        p += 1

    systemConfigurationFile = open(config_file, 'w')
    json.dump(config, systemConfigurationFile, sort_keys=False, indent=4, ensure_ascii=False)
    systemConfigurationFile.flush()
    systemConfigurationFile.close()
    return config["group_members"]


def orchestra(num_destinations, num_clients, num_process_per_group, package_size):
    debug_log_dir = '{}/bin'.format(common.PATH_LIBRAMCAST_HOME)
    log_dir = '{}/logs/max-single-dest-1248-group/{}d-{}c-{}p-{}b-{}'.format(common.PATH_LIBRAMCAST_HOME,
                                                                             num_destinations,
                                                                             num_clients,
                                                                             num_process_per_group,
                                                                             package_size,
                                                                             datetime.now().strftime(
                                                                                 '%Y-%m-%d-%H-%M-%S'))
    config_file = '{}/bin/systemConfigs/generatedSystemConfig{}g{}p.json'.format(common.PATH_LIBRAMCAST_HOME,
                                                                                 num_destinations,
                                                                                 num_process_per_group)

    available_nodes = gen_config(num_process_per_group, num_destinations, num_clients, config_file)
    time.sleep(1)

    if common.ENV_EMULAB:
        # need to sync this sysConfig with other instances
        sync_script = '/users/lel/apps/libramcast/libRamcastV3/bin/emulab/sync-code.sh'
        config_dir = '/users/lel/apps/libramcast/libRamcastV3/bin/systemConfigs'
        os.system("{}  {} {}".format(sync_script, config_dir, str(len(common.RDMA_NODES))))
        time.sleep(3)

    print(available_nodes)

    i = 0
    p = 0
    g = 0
    cmds = []
    clients_used = 0
    clients_per_group_used = 1
    while i < len(available_nodes):
        cmd = [java_cmd, '-DLOG_DIR=' + debug_log_dir, '-DGROUPID=' + str(g), '-DNODEID=' + str(p), common.CLASS_BENCH,
               "-c", config_file, "-s", package_size,
               "-gid", g, "-nid", p, "-cid", i,
               "-d", DURATION, "-gh", common.GATHERER_HOST, "-gp", common.GATHERER_PORT, "-gd", log_dir, "-gw",
               WARMUP * 1000]
        if p == num_process_per_group - 1 and g < num_destinations:
            # the last process of the group will be client of that group for measuring latency
            cmd += ["-df", str(g), "-dc 1", "-isClient", "1"]
            clients_used += 1
        cmdString = ' '.join([str(val) for val in cmd])
        cmds.append([available_nodes[i]["host"], cmdString])
        i += 1
        p += 1
        if p == NUM_PROCESSES:
            p = 0
            g += 1

    i = num_process_per_group * num_destinations
    dest_from = 0
    for j in range(0, num_destinations):
        for k in range(1, num_clients):  # already provide 1 client in the group
            cmds[i][1] = cmds[i][1] + ' ' + ' '.join(["-df", str(dest_from), "-dc", "1", "-isClient", "1"])
            i += 1
        dest_from += 1

    for cmd in cmds:
        print(cmd[0], cmd[1])
        common.sshcmdbg(cmd[0], cmd[1])

    # start gatherer
    cmd = [java_cmd, common.CLASS_GATHERER, WARMUP * 1000, common.GATHERER_PORT, log_dir,
           "throughput", "client_overall", num_clients,
           "latency", "client_overall", num_destinations,
           "latencydistribution", "client_overall", num_destinations,
           ]

    cmdString = ' '.join([str(val) for val in cmd])
    common.localcmd(cmdString)

    common.localcmd(common.APP_CLEANER)
    time.sleep(1)
    print("===================================\n          Throughput              \n===================================")
    common.localcmd("cat " + log_dir + "/throughput_client_overall_aggregate.log")
    print("===================================\n          Latency                 \n===================================")
    common.localcmd("cat " + log_dir + "/latency_client_overall_average.log")
    print("===================================")


for i in range(0, NUM_RUNS):
    run()
