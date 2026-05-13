package io.github.stellhub.stellflow.sdk.protocol.message;

/** API 版本范围。 */
public record ApiVersionRange(short apiKey, short minVersion, short maxVersion) {}
