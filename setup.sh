#!/bin/bash

sudo apt-get update > /dev/null
sudo apt-get install jq -y > /dev/null

config=$(jq '.' config.json)
local_id=$(echo $config | jq -r '.local | "\(.id)"')
local_hostname=$(echo $config | jq -r '.local | "\(.hostname)"')
remote_ids=($(echo $config | jq -r '.remotes[] | "\(.id)"'))
remote_hostnames=($(echo $config | jq -r '.remotes[] | "\(.hostname)"'))
node_ids=("$local_id" "${remote_ids[@]}")
node_hostnames=("$local_hostname" "${remote_hostnames[@]}")

# Pull the last version before setup
git pull

rm -rf logs_setup
mkdir logs_setup

pids=()
for ((i = 0; i < ${#node_hostnames[@]}; i++)); do
  id=${node_ids[i]}
  host=${node_hostnames[i]}

  echo "Setup of node$id..."
  ssh -o StrictHostKeyChecking=no "$host" 'bash -s' <./setup_script.sh &> "./logs_setup/node$id" &
  pids+=("$!")
done

for ((i = 0; i < ${#pids[@]}; i++)); do
  id=${node_ids[i]}
  wait ${pids[i]}
  echo "Setup of node$id done"
done
