#!/usr/bin/python

import inspect
import os
import sys

HEAD_NODE = 187
TAIL_NODE = 20


def script_dir():
    return os.path.dirname(os.path.abspath(inspect.getsourcefile(lambda _: None)))


def usage():
    print "usage: " + sys.argv[0] + " control-node nodes-count"
    sys.exit(1)


if len(sys.argv) not in [1, 3]:
    usage()

if len(sys.argv) != 1:
    HEAD_NODE = sys.argv[1]
    TAIL_NODE = sys.argv[2]

GLOBAL_HOME = os.path.normpath(script_dir() + '/../../../')
WHITECAST_HOME = "/Users/longle/Documents/Workspace/PhD/RDMA/samples/atomic-multicast"
TARGET_WHITECAST_HOME = '/users/lel/apps/atomic-multicast'

if "apt" in HEAD_NODE:
    TARGET_NODE = "{}.apt.emulab.net".format(HEAD_NODE)
else:
    TARGET_NODE = "{}.utah.cloudlab.us".format(HEAD_NODE)
TARGET_HOME = '/users/lel/apps/libramcast/'

CMD_CREATE_DIR = ["ssh", TARGET_NODE, "'mkdir -p ", TARGET_HOME, "'"]
CMD_CREATE_DIR = ' '.join([str(val) for val in CMD_CREATE_DIR])
os.system(CMD_CREATE_DIR)

IGNORE_FILE = script_dir() + '/.deployIgnore'

CMD_COPY_BUILD = ["rsync", "-rav", "--exclude-from='" + IGNORE_FILE + "'",
                  GLOBAL_HOME + "/*", TARGET_NODE + ":" + TARGET_HOME]

CMD_COPY_BUILD = ' '.join([str(val) for val in CMD_COPY_BUILD])
print CMD_COPY_BUILD
os.system(CMD_COPY_BUILD)
#
# CMD_COPY_BUILD = ["rsync", "-rav", "--exclude-from='" + IGNORE_FILE + "'",
#                   WHITECAST_HOME + "/bench", TARGET_NODE + ":" + TARGET_WHITECAST_HOME]
#
# CMD_COPY_BUILD = ' '.join([str(val) for val in CMD_COPY_BUILD])
# print CMD_COPY_BUILD
# os.system(CMD_COPY_BUILD)

CMD_SYNC = ["ssh -o StrictHostKeyChecking=no", TARGET_NODE, "/users/lel/apps/libramcast/libRamcastV3/bin/emulab/sync-code.sh", TAIL_NODE]
CMD_SYNC = ' '.join([str(val) for val in CMD_SYNC])

os.system(CMD_SYNC)
