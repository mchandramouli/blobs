package com.expedia.blobs.core.io;

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ManagedAsyncOperation implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedAsyncOperation.class);
    private final ExecutorService threadPool;

    ManagedAsyncOperation(int threadPoolSize) {
        Validate.isTrue(threadPoolSize > 0);
        this.threadPool = Executors.unconfigurableExecutorService(Executors.newFixedThreadPool(threadPoolSize));
    }

    ManagedAsyncOperation(ExecutorService threadPool) {
        Validate.notNull(threadPool);
        this.threadPool = threadPool;
    }

    void execute(Runnable operation, BiConsumer<Void, Throwable> handler) {
        CompletableFuture.runAsync(operation, threadPool).whenComplete(handler);
    }

    <T> void execute(Supplier<T> supplier, BiConsumer<T, Throwable> callback) {
        CompletableFuture.supplyAsync(supplier, threadPool).whenComplete(callback);
    }

    <T> Optional<T> execute(Supplier<T> supplier, long timeout, TimeUnit timeUnit) {
        try {
            final T result = CompletableFuture.supplyAsync(supplier, threadPool).get(timeout, timeUnit);
            return Optional.ofNullable(result);
        }
        catch (InterruptedException|ExecutionException|TimeoutException e) {
            return Optional.empty();
        }
    }

    @Override
    public void close() {
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(60L, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
                if (!threadPool.awaitTermination(60L, TimeUnit.SECONDS)) {
                    LOGGER.error("AsyncStore thread pool failed to terminate");
                }
            }
        }
        catch (InterruptedException ie) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
