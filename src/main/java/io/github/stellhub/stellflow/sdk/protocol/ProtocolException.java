package io.github.stellhub.stellflow.sdk.protocol;

/** 协议基础异常。 */
public class ProtocolException extends RuntimeException {

  public ProtocolException(String message) {
    super(message);
  }

  public ProtocolException(String message, Throwable cause) {
    super(message, cause);
  }
}
