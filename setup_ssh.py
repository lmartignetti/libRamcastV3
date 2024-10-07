import os
import json
import subprocess

CONFIG_FILE = "config.json"
LOG_FOLDER = "logs_setup_ssh"
TEMP_DIR = "temp"
KEY_FILE = os.path.join(TEMP_DIR, "id_ed25519")
PUB_KEY_FILE = os.path.join(TEMP_DIR, "id_ed25519.pub")

if os.path.exists(LOG_FOLDER):
    subprocess.run(["rm", "-rf", LOG_FOLDER])
os.mkdir(LOG_FOLDER)

with open(CONFIG_FILE) as f:
    config = json.load(f)

local = config["local"]
remotes = config["remotes"]

if os.path.exists(TEMP_DIR):
    subprocess.run(["rm", "-rf", TEMP_DIR])
os.mkdir(TEMP_DIR)

subprocess.run(["ssh-keygen", "-f", KEY_FILE, "-N", ""])

for node in [local] + remotes:
    remote_hostname = node["hostname"]
    print(f"Setup of host {remote_hostname}")
    log_file = os.path.join(LOG_FOLDER, f"log_setup_ssh_{remote_hostname}.log")

    with open(log_file, "w") as log:
        subprocess.run(
            [
                "rsync",
                "-avh",
                "-e",
                "ssh -o StrictHostKeyChecking=no",
                KEY_FILE,
                f"{remote_hostname}:~/.ssh/",
            ],
            stdout=log,
            stderr=log,
        )
        subprocess.run(
            [
                "rsync",
                "-avh",
                "-e",
                "ssh -o StrictHostKeyChecking=no",
                PUB_KEY_FILE,
                f"{remote_hostname}:~/.ssh/",
            ],
            stdout=log,
            stderr=log,
        )

        subprocess.run(
            [
                "ssh",
                "-o",
                "StrictHostKeyChecking=no",
                remote_hostname,
                "cd ~/.ssh; cat id_ed25519.pub >> authorized_keys",
            ],
            stdout=log,
            stderr=log,
        )

subprocess.run(["rm", "-rf", TEMP_DIR])
