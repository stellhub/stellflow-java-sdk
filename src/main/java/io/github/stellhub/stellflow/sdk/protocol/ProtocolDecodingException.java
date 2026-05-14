package io.github.stellhub.stellflow.sdk.protocol;

/** 协议解码异常。 */
public class ProtocolDecodingException extends ProtocolException {

    public ProtocolDecodingException(String message) {
        super(message);
    }

    public ProtocolDecodingException(String message, Throwable cause) {
        super(message, cause);
    }
}
