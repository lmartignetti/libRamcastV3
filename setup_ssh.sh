#!/bin/bash

sudo apt update
sudo apt install jq -y

config=$(jq '.' config.json)
local_id=$(echo $config | jq -r '.local | "\(.id)"')
local_hostname=$(echo $config | jq -r '.local | "\(.hostname)"')
remote_ids=($(echo $config | jq -r '.remotes[] | "\(.id)"'))
remote_hostnames=($(echo $config | jq -r '.remotes[] | "\(.hostname)"'))
node_ids=("$local_id" "${remote_ids[@]}")
node_hostnames=("$local_hostname" "${remote_hostnames[@]}")

rm -rf temp
mkdir temp
cd temp
ssh-keygen -f id_ed25519 -N ""
cd ..

pids=()
for ((i = 0; i < ${#node_hostnames[@]}; i++)); do
  id=${node_ids[i]}
  host=${node_hostnames[i]}

  scp -o StrictHostKeyChecking=no temp/id_ed25519 "$host:~/.ssh" >/dev/null
  scp -o StrictHostKeyChecking=no temp/id_ed25519.pub "$host:~/.ssh" >/dev/null

  echo "Setup of node$id..."
  ssh -o StrictHostKeyChecking=no "$host" "cd ~/.ssh; cat id_ed25519.pub >>authorized_keys" >/dev/null &
  pids+=("$!")
done

for ((i = 0; i < ${#pids[@]}; i++)); do
  id=${node_ids[i]}
  wait ${pids[i]}
  echo "Setup of node$id done"
done

rm -rf temp
