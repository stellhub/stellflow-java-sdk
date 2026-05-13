package io.github.stellhub.stellflow.sdk.client;

import io.github.stellhub.stellflow.sdk.protocol.ErrorCode;

/** SDK 请求异常。 */
public class StellflowClientException extends RuntimeException {

  private final ErrorCode errorCode;

  public StellflowClientException(String message, ErrorCode errorCode) {
    super(message);
    this.errorCode = errorCode;
  }

  public ErrorCode errorCode() {
    return errorCode;
  }
}
