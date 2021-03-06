/*
 *
 *     Copyright 2018 Expedia, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 *
 */
package com.expedia.blobs.core.io;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

class ManagedAsyncOperation implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedAsyncOperation.class);
    private final ExecutorService threadPool;
    private int shutdownWaitInSeconds;

    ManagedAsyncOperation(int threadPoolSize, int shutdownWaitInSeconds) {
        Validate.isTrue(threadPoolSize > 0, "threadPoolSize cannot be 0");
        Validate.isTrue(shutdownWaitInSeconds > 0);
        this.shutdownWaitInSeconds = shutdownWaitInSeconds;
        this.threadPool = Executors.unconfigurableExecutorService(Executors.newFixedThreadPool(threadPoolSize));
    }

    void execute(Runnable operation, BiConsumer<Void, Throwable> handler) {
        CompletableFuture.runAsync(operation, threadPool).whenComplete(handler);
    }

    <T> void execute(Supplier<T> supplier, BiConsumer<T, Throwable> callback) {
        CompletableFuture.supplyAsync(supplier, threadPool).whenComplete(callback);
    }

    <T> T execute(Supplier<T> supplier, T defaultValue, long timeout, TimeUnit timeUnit) {
        try {
            return CompletableFuture.supplyAsync(supplier, threadPool).get(timeout, timeUnit);
        }
        catch (InterruptedException|ExecutionException|TimeoutException e) {
            return defaultValue;
        }
    }

    @Override
    public void close() {
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(this.shutdownWaitInSeconds, TimeUnit.SECONDS)) {
                LOGGER.error(String.format("AsyncStore thread pool failed to terminate in %s seconds. Forcing shutdown", this.shutdownWaitInSeconds));
                threadPool.shutdownNow();
            }
        }
        catch (InterruptedException ie) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
