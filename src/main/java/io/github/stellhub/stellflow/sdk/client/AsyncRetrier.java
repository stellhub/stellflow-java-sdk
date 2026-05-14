package io.github.stellhub.stellflow.sdk.client;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

/** 异步重试工具。 */
public final class AsyncRetrier {

    private AsyncRetrier() {}

    /** 按策略执行异步重试。 */
    public static <T> CompletableFuture<T> execute(
            RetryPolicy retryPolicy,
            Supplier<CompletableFuture<T>> operation,
            Predicate<Throwable> retryable) {
        Objects.requireNonNull(retryPolicy, "retryPolicy must not be null");
        Objects.requireNonNull(operation, "operation must not be null");
        Objects.requireNonNull(retryable, "retryable must not be null");
        return attempt(retryPolicy, operation, retryable, 1);
    }

    private static <T> CompletableFuture<T> attempt(
            RetryPolicy retryPolicy,
            Supplier<CompletableFuture<T>> operation,
            Predicate<Throwable> retryable,
            int attempt) {
        try {
            return operation
                    .get()
                    .handle(
                            (value, throwable) -> {
                                if (throwable == null) {
                                    return CompletableFuture.completedFuture(value);
                                }
                                Throwable cause = unwrap(throwable);
                                if (attempt >= retryPolicy.maxAttempts() || !retryable.test(cause)) {
                                    return CompletableFuture.<T>failedFuture(cause);
                                }
                                return delay(retryPolicy)
                                        .thenCompose(
                                                ignored -> attempt(retryPolicy, operation, retryable, attempt + 1));
                            })
                    .thenCompose(value -> value);
        } catch (Throwable throwable) {
            Throwable cause = unwrap(throwable);
            if (attempt >= retryPolicy.maxAttempts() || !retryable.test(cause)) {
                return CompletableFuture.failedFuture(cause);
            }
            return delay(retryPolicy)
                    .thenCompose(ignored -> attempt(retryPolicy, operation, retryable, attempt + 1));
        }
    }

    private static CompletableFuture<Void> delay(RetryPolicy retryPolicy) {
        if (retryPolicy.backoff().isZero()) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.runAsync(
                () -> {},
                CompletableFuture.delayedExecutor(retryPolicy.backoff().toMillis(), TimeUnit.MILLISECONDS));
    }

    private static Throwable unwrap(Throwable throwable) {
        if (throwable instanceof CompletionException && throwable.getCause() != null) {
            return throwable.getCause();
        }
        return throwable;
    }
}
