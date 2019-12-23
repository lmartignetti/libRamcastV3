package ch.usi.dslab.lel.ramcast.endpoint;

public interface CustomHandler {
  default void handleReceive(Object data) {}

  default void handleSendComplete(Object data) {}

  default void handleReceiveMessage(Object data) {}
}
