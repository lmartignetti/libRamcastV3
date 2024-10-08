#!/bin/bash

PROJECT_HOME=~/libRamcastV3
BRANCH=emulab-cluster-tpcc

cd $PROJECT_HOME

config=$(jq '.' config.json)
remote_ids=($(echo $config | jq -r '.remotes[] | "\(.id)"'))
remote_hostnames=($(echo $config | jq -r '.remotes[] | "\(.hostname)"'))

# Upload changes
for ((i = 0; i < ${#remote_hostnames[@]}; i++)); do
  id=${remote_ids[i]}
  host=${remote_hostnames[i]}
  echo "Cleaning source code of node$id ($host)..."
  rsync -a --delete --exclude='target' --exclude='logs' -e "ssh -o StrictHostKeyChecking=no" "$PROJECT_HOME" "$host:~"
  echo "Cleaning source code of node$id done"
done

# Install RamCast on all nodes
echo "Building local node..."
mvn clean install -DskipTests >/dev/null
echo "Building local node done"

pids=()
for ((i = 0; i < ${#remote_hostnames[@]}; i++)); do
  id=${remote_ids[i]}
  host=${remote_hostnames[i]}
  echo "Building node$id ($host)..."
  ssh -o StrictHostKeyChecking=no $host "cd $PROJECT_HOME; mvn clean install -DskipTests" >/dev/null &
  pids+=("$!")
done

# Wait for all remotes to finish
for ((i = 0; i < ${#pids[@]}; i++)); do
  id=${remote_ids[i]}
  pid=${pids[i]}

  wait $pid
  echo "Building node$id done"
done
