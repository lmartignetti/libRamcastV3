#!/bin/bash

sudo apt-get update
sudo apt-get install libibverbs-dev librdmacm-dev ibverbs-utils rdma-core -y
sudo apt-get install jq -y
sudo apt-get install openjdk-8-jdk maven -y
sudo apt-get install autoconf automake libtool -y

cd ~

sudo rm -rf libRamcastV3
sudo rm -rf disni
sudo rm -rf sense
sudo rm -rf netwrapper

if [ ! -d libRamcastV3 ]; then
  git clone https://github.com/martilo-usi/libRamcastV3.git
fi

if [ ! -d disni ]; then
  git clone https://github.com/zrlio/disni.git
fi

if [ ! -d sense ]; then
  git clone https://bitbucket.org/kdubezerra/sense.git
fi

if [ ! -d netwrapper ]; then
  git clone https://bitbucket.org/kdubezerra/netwrapper.git
fi

# Install disni from the java source and then install libdisni
cd disni
export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
mvn clean install -DskipTests

cd libdisni
./autoprepare.sh
./configure --with-jdk=$JAVA_HOME
sudo make install
cd ../..

# Install netwrapper
cd netwrapper
mvn clean install
cd ..

# Install sense
cd sense
mvn clean install
cd ..
