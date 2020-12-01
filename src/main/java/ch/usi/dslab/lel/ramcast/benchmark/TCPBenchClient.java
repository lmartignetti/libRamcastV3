package ch.usi.dslab.lel.ramcast.benchmark;

import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.bezerra.netwrapper.tcp.TCPConnection;
import ch.usi.dslab.bezerra.sense.DataGatherer;
import ch.usi.dslab.bezerra.sense.monitors.LatencyDistributionPassiveMonitor;
import ch.usi.dslab.bezerra.sense.monitors.LatencyPassiveMonitor;
import ch.usi.dslab.bezerra.sense.monitors.ThroughputPassiveMonitor;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TCPBenchClient {

  private static final int SERIALIZE_OVERHEAD = 64;
  private ThroughputPassiveMonitor tpMonitor;
  private LatencyPassiveMonitor latMonitor;
  private LatencyDistributionPassiveMonitor cdfMonitor;

  public static void main(String[] args) throws IOException, ParseException, InterruptedException {

    TCPBenchClient app = new TCPBenchClient();
    app.bench(args);
  }

  public void bench(String[] args) throws IOException, ParseException, InterruptedException {
    Option serverAddressOption = Option.builder("sa").required().desc("server address size").hasArg().build();
    Option serverPortOption = Option.builder("sp").required().desc("server port size").hasArg().build();
    Option packageSizeOption = Option.builder("s").required().desc("sample package size").hasArg().build();
    Option gathererHostOption = Option.builder("gh").required().desc("gatherer host").hasArg().build();
    Option gathererPortOption = Option.builder("gp").required().desc("gatherer port").hasArg().build();
    Option gathererDirectoryOption = Option.builder("gd").required().desc("gatherer directory").hasArg().build();
    Option warmUpTimeOption = Option.builder("gw").required().desc("gatherer warmup time").hasArg().build();
    Option durationOption = Option.builder("d").required().desc("benchmark duration").hasArg().build();

    Options options = new Options();
    options.addOption(serverAddressOption);
    options.addOption(serverPortOption);
    options.addOption(packageSizeOption);
    options.addOption(gathererHostOption);
    options.addOption(gathererPortOption);
    options.addOption(warmUpTimeOption);
    options.addOption(durationOption);
    options.addOption(gathererDirectoryOption);

    CommandLineParser parser = new DefaultParser();
    CommandLine line = parser.parse(options, args);

    String serverAddress = line.getOptionValue(serverAddressOption.getOpt());
    int serverPort = Integer.parseInt(line.getOptionValue(serverPortOption.getOpt()));
    int payloadSize = Integer.parseInt(line.getOptionValue(packageSizeOption.getOpt()));
    String gathererHost = line.getOptionValue(gathererHostOption.getOpt());
    int gathererPort = Integer.parseInt(line.getOptionValue(gathererPortOption.getOpt()));
    String fileDirectory = line.getOptionValue(gathererDirectoryOption.getOpt());
    int experimentDuration = Integer.parseInt(line.getOptionValue(durationOption.getOpt()));
    int warmUpTime = Integer.parseInt(line.getOptionValue(warmUpTimeOption.getOpt()));

    DataGatherer.configure(experimentDuration, fileDirectory, gathererHost, gathererPort, warmUpTime);
    ByteBuffer buffer = ByteBuffer.allocateDirect(payloadSize);
    buffer.putLong(System.nanoTime());

    byte[] payload = new byte[payloadSize - SERIALIZE_OVERHEAD];
    int i = 0;
    while (i < payload.length) payload[i++] = 1;

    Message message = new Message(System.nanoTime(), payload);
    System.out.println("Message size: " + message.getByteBufferWithLengthHeader().capacity());
    TCPConnection serverConnection = new TCPConnection(serverAddress, serverPort);

    long now;
    tpMonitor = new ThroughputPassiveMonitor(1, "client_overall", true);
    latMonitor = new LatencyPassiveMonitor(1, "client_overall", true);
    cdfMonitor = new LatencyDistributionPassiveMonitor(1, "client_overall", true);
    while (!Thread.interrupted()) {
      message.setItem(0, System.nanoTime());
      serverConnection.sendBusyWait(message);
      Message rawReply = serverConnection.receiveBusyWait();
      now = System.nanoTime();
      tpMonitor.incrementCount();
      latMonitor.logLatency((Long) rawReply.getItem(0), now);
      cdfMonitor.logLatencyForDistribution((Long) rawReply.getItem(0), now);
//      System.out.println("Reply received: " + rawReply.getByteBufferWithLengthHeader().capacity() + " time " + (System.nanoTime() - (Long) rawReply.getItem(0)));
//      Thread.sleep(100);
    }
  }
}
