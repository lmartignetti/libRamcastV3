#!/bin/bash

PROJECT_HOME=~/libRamcastV3
BRANCH=emulab-cluster-tpcc

cd $PROJECT_HOME

config=$(jq '.' config.json)
remote_ids=($(echo $config | jq -r '.remotes[] | "\(.id)"'))
remote_hostnames=($(echo $config | jq -r '.remotes[] | "\(.hostname)"'))

# Gather the list of files changed locally and upload changes
paths_modified=($(git status --porcelain | awk '/M|\?\? /{print $2}'))
paths_deleted=($(git status --porcelain | awk '/D/ {print $2}'))

for ((i = 0; i < ${#remote_hostnames[@]}; i++)); do
  id=${remote_ids[i]}
  host=${remote_hostnames[i]}
  echo "Cleaning source code of node$id ($host)..."

  # Clean remote source code
  ssh -o StrictHostKeyChecking=no $host "cd $PROJECT_HOME; git clean -f -d; git reset --hard; git pull; git checkout $BRANCH" >/dev/null

  for m in "${paths_modified[@]}"; do
    scp -o StrictHostKeyChecking=no "$m" "$host:$PROJECT_HOME/$m" >/dev/null
  done
  for d in "${paths_deleted[@]}"; do
    ssh -o StrictHostKeyChecking=no "$host" "cd $PROJECT_HOME; rm $d" >/dev/null
  done
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
