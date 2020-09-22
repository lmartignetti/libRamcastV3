/*
 * Ramcast: Data Center Remote Procedure Call
 *
 * Author: Patrick Stuedi <stu@zurich.ibm.com>
 *
 * Copyright (C) 2016-2018, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package ch.usi.dslab.lel.ramcast.benchmark;

import ch.usi.dslab.bezerra.sense.DataGatherer;
import ch.usi.dslab.bezerra.sense.monitors.LatencyPassiveMonitor;
import ch.usi.dslab.bezerra.sense.monitors.ThroughputPassiveMonitor;
import ch.usi.dslab.lel.ramcast.MessageDeliveredCallback;
import ch.usi.dslab.lel.ramcast.RamcastAgent;
import ch.usi.dslab.lel.ramcast.RamcastConfig;
import ch.usi.dslab.lel.ramcast.models.RamcastGroup;
import ch.usi.dslab.lel.ramcast.models.RamcastMessage;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Semaphore;

public class BenchAgent {
  private static final Logger logger = LoggerFactory.getLogger(BenchAgent.class);
  Semaphore sendPermits;
  private ThroughputPassiveMonitor tpMonitor;
  private LatencyPassiveMonitor latMonitor;

  private RamcastConfig config;
  private RamcastAgent agent;
  private int clientId;
  private ByteBuffer _buffer;
  private List<RamcastGroup> _dests;
  private long startTime;
  private ByteBuffer responseBuffer;
  private ByteBuffer sampleBuffer;
  private int msgCount;

  private MessageDeliveredCallback onDeliverAmcast =
          new MessageDeliveredCallback() {
            @Override
            public void call(Object data) {
              //          System.out.println(
              //              ">>>>>> "
              //                  //                  + agent.getNode()
              //                  + " == "
              //                  + ((RamcastMessage) data).getId()
              //                  + " == "
              //                  + (System.nanoTime() - startTime)
              //              //                                + "\n"
              //              //                                + (RamcastMessage) data
              //              //                                + "\n"
              //              //                                +
              // agent.getEndpointGroup().getTimestampBlock()
              //              );

              if (agent.hasClientRole()) {
                if (((RamcastMessage) data).getMessage().getInt(0) == clientId) {
                  releasePermit();
                  System.out.println(
                          ">>>>>> "
                                  + agent.getNode()
                                  + " == "
                                  + ((RamcastMessage) data).getId()
                                  + " == "
                                  + (System.nanoTime() - startTime)
                          //                                                  + "\n"
                          //                                                  + (RamcastMessage) data
                          //                                                  + "\n"
                          //                                                  +
                          // agent.getEndpointGroup().getTimestampBlock()
                  );
                  latMonitor.logLatency(startTime, System.nanoTime());
                  tpMonitor.incrementCount();
                }
              }
            }
          };

  public static void main(String[] args) throws Exception {
    //        Thread.sleep(5000);
    BenchAgent benchAgent = new BenchAgent();
    benchAgent.launch(args);
  }

  void getPermit() {
    try {
      sendPermits.acquire();
    } catch (InterruptedException e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  public void releasePermit() {
    sendPermits.release();
  }

  public void launch(String[] args) throws Exception {
    Option nIdOption = Option.builder("nid").desc("node id").hasArg().build();
    Option gIdOption = Option.builder("gid").desc("group id").hasArg().build();
    Option cIdOption = Option.builder("cid").desc("client id").hasArg().build();
    Option configOption = Option.builder("c").required().desc("config file").hasArg().build();
    Option packageSizeOption =
            Option.builder("s").required().desc("sample package size").hasArg().build();
    Option gathererHostOption =
            Option.builder("gh").required().desc("gatherer host").hasArg().build();
    Option gathererPortOption =
            Option.builder("gp").required().desc("gatherer port").hasArg().build();
    Option gathererDirectoryOption =
            Option.builder("gd").required().desc("gatherer directory").hasArg().build();
    Option warmUpTimeOption =
            Option.builder("gw").required().desc("gatherer warmup time").hasArg().build();
    Option durationOption =
            Option.builder("d").required().desc("benchmark duration").hasArg().build();
    Option destinationCountOption =
            Option.builder("dc").required().desc("destination count").hasArg().build();

    Options options = new Options();
    options.addOption(nIdOption);
    options.addOption(gIdOption);
    options.addOption(cIdOption);
    options.addOption(configOption);
    options.addOption(packageSizeOption);
    options.addOption(gathererHostOption);
    options.addOption(gathererPortOption);
    options.addOption(warmUpTimeOption);
    options.addOption(durationOption);
    options.addOption(destinationCountOption);
    options.addOption(gathererDirectoryOption);

    CommandLineParser parser = new DefaultParser();
    CommandLine line = parser.parse(options, args);

    int nodeId = Integer.parseInt(line.getOptionValue(nIdOption.getOpt()));
    int groupId = Integer.parseInt(line.getOptionValue(gIdOption.getOpt()));
    clientId = Integer.parseInt(line.getOptionValue(cIdOption.getOpt()));
    String configFile = line.getOptionValue(configOption.getOpt());
    int payloadSize = Integer.parseInt(line.getOptionValue(packageSizeOption.getOpt()));
    String gathererHost = line.getOptionValue(gathererHostOption.getOpt());
    int gathererPort = Integer.parseInt(line.getOptionValue(gathererPortOption.getOpt()));
    String fileDirectory = line.getOptionValue(gathererDirectoryOption.getOpt());
    int experimentDuration = Integer.parseInt(line.getOptionValue(durationOption.getOpt()));
    int warmUpTime = Integer.parseInt(line.getOptionValue(warmUpTimeOption.getOpt()));
    int destinationCount = Integer.parseInt(line.getOptionValue(destinationCountOption.getOpt()));

    config = RamcastConfig.getInstance();
    config.loadConfig(configFile);
    config.setPayloadSize(payloadSize);

    this.agent = new RamcastAgent(groupId, nodeId, onDeliverAmcast);

    if (this.agent.hasClientRole()) {
      DataGatherer.configure(
              experimentDuration, fileDirectory, gathererHost, gathererPort, warmUpTime);
      this.tpMonitor = new ThroughputPassiveMonitor(this.clientId, "client_overall", true);
      this.latMonitor = new LatencyPassiveMonitor(this.clientId, "client_overall", true);
    }

    this.agent.bind();
    Thread.sleep(1000);
    this.agent.establishConnections();
    logger.info("NODE READY");
    Thread.sleep(3000);

    this.startBenchmark();
  }

  private void startBenchmark() throws IOException, InterruptedException {
    System.out.println("Node " + this.agent.getNode() + " start benchmarking");
    sendPermits = new Semaphore(1);

    ByteBuffer buffer = ByteBuffer.allocateDirect(12);
    buffer.putInt(clientId);
    buffer.putInt(11);
    buffer.putInt(12);

    List<RamcastGroup> dest = new ArrayList<>();
    for (int i = 0; i < RamcastConfig.getInstance().getGroupCount(); i++) {
      dest.add(RamcastGroup.getGroup(i));
    }
    RamcastMessage sampleMessage;
    ByteBuffer sampleBuffer;
    int i = 0;
    int lastMsgId = -1;
    //    sampleBuffer = sampleMessage.toBuffer();
    if (agent.hasClientRole()) {
      while (true) {
        getPermit();
//        try {
//          Thread.sleep(1000);
//        } catch (InterruptedException e) {
//          e.printStackTrace();
//        }
        int id = Objects.hash(i, this.clientId);
        //        id = i;
        sampleMessage = this.agent.createMessage(id, buffer, dest);
        //        sampleBuffer.putInt(0, i);
        startTime = System.nanoTime();
        if (RamcastConfig.LOG_ENABLED)
          logger.debug("Client {} start new request {} msgId {}", clientId, i, id);
        //        agent.multicast(sampleBuffer, dest); // for bemchmark only
        while (!agent.isAllDestReady(dest, lastMsgId)) {
          Thread.yield();
        }
        agent.multicast(sampleMessage, dest);
        lastMsgId = id;
        i++;
      }
    }
  }
}
