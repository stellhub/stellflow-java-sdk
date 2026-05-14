package io.github.stellhub.stellflow.sdk.network;

import io.github.stellhub.stellflow.sdk.protocol.ApiKey;
import io.github.stellhub.stellflow.sdk.protocol.RequestMessage;
import io.github.stellhub.stellflow.sdk.protocol.ResponseMessage;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** 管理一个连接上的 in-flight 请求。 */
public class InFlightRequests {

    private final ConcurrentMap<Integer, PendingRequest> requests = new ConcurrentHashMap<>();

    /** 注册待响应请求。 */
    public PendingRequest register(RequestMessage request, Duration timeout) {
        int correlationId = request.header().correlationId();
        PendingRequest pendingRequest =
                new PendingRequest(
                        correlationId,
                        request.header().apiKey(),
                        request.header().apiVersion(),
                        System.nanoTime(),
                        timeout,
                        new CompletableFuture<>());
        PendingRequest previous = requests.putIfAbsent(correlationId, pendingRequest);
        if (previous != null) {
            throw new IllegalStateException("duplicate in-flight correlationId: " + correlationId);
        }
        return pendingRequest;
    }

    /** 查找待响应请求。 */
    public Optional<PendingRequest> get(int correlationId) {
        return Optional.ofNullable(requests.get(correlationId));
    }

    /** 完成请求。 */
    public boolean complete(ResponseMessage response) {
        PendingRequest pendingRequest = requests.remove(response.header().correlationId());
        if (pendingRequest == null) {
            return false;
        }
        pendingRequest.future().complete(response);
        return true;
    }

    /** 将请求标记为失败。 */
    public boolean fail(int correlationId, Throwable throwable) {
        PendingRequest pendingRequest = requests.remove(correlationId);
        if (pendingRequest == null) {
            return false;
        }
        pendingRequest.future().completeExceptionally(throwable);
        return true;
    }

    /** 失败所有未完成请求。 */
    public void failAll(Throwable throwable) {
        for (Integer correlationId : requests.keySet()) {
            fail(correlationId, throwable);
        }
    }

    /** 返回当前 in-flight 数量。 */
    public int size() {
        return requests.size();
    }

    /** 待响应请求上下文。 */
    public record PendingRequest(
            int correlationId,
            ApiKey apiKey,
            short apiVersion,
            long createdAtNanos,
            Duration timeout,
            CompletableFuture<ResponseMessage> future) {}
}
