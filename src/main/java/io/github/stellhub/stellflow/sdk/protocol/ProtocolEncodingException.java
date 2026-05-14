package io.github.stellhub.stellflow.sdk.protocol;

/** 协议编码异常。 */
public class ProtocolEncodingException extends ProtocolException {

    public ProtocolEncodingException(String message) {
        super(message);
    }

    public ProtocolEncodingException(String message, Throwable cause) {
        super(message, cause);
    }
}
