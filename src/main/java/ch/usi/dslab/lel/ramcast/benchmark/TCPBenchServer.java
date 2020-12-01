package ch.usi.dslab.lel.ramcast.benchmark;

import ch.usi.dslab.bezerra.netwrapper.tcp.TCPMessage;
import ch.usi.dslab.bezerra.netwrapper.tcp.TCPReceiver;
import ch.usi.dslab.bezerra.netwrapper.tcp.TCPSender;
import org.apache.commons.cli.*;

public class TCPBenchServer {
  public static void main(String[] args) throws ParseException {
    Option serverPortOption = Option.builder("sp").required().desc("server bind port").hasArg().build();
    Options options = new Options();
    options.addOption(serverPortOption);
    CommandLineParser parser = new DefaultParser();
    CommandLine line = parser.parse(options, args);

    int serverPort = Integer.parseInt(line.getOptionValue(serverPortOption.getOpt()));

    options.addOption(serverPortOption);
    TCPReceiver receiver = new TCPReceiver(serverPort);
    TCPSender sender = new TCPSender();

    while (!Thread.interrupted()) {
      TCPMessage msg = receiver.receive();
//      String recvdMessage = (String) msg.getContents().getItem(0);
//      System.out.println("Received message: " + count++);
      sender.send(msg.getContents(), msg.getConnection());
    }
  }
}
