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

import com.expedia.blobs.core.Blob;
import com.expedia.blobs.core.BlobStore;
import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AsyncStore implements BlobStore, Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncStore.class);
    private final ManagedAsyncOperation managedAsyncOperation;

    /**
     * Initializes {@link AsyncStore} with threads equal to the number of processors available
     * See Runtime#availableProcessors()
     */
    public AsyncStore() {
        this(Runtime.getRuntime().availableProcessors());
    }

    /**
     * Initializes {@link AsyncStore} instance with a threadpool if the number of
     * threads given are greater than zero
     * @param threadPoolSize 0 or more
     */
    public AsyncStore(int threadPoolSize) {
        this(threadPoolSize > 0 ? new ManagedAsyncOperation(threadPoolSize) : null);
    }

    private AsyncStore(ManagedAsyncOperation managedAsyncOperation) {
        this.managedAsyncOperation = managedAsyncOperation;
    }

    @Override
    public void store(Blob blob) {
        if (this.managedAsyncOperation == null) {
            storeInternal(blob);
        }
        else {
            this.managedAsyncOperation.execute(() -> storeInternal(blob), (v, t) -> {
                LOGGER.error(this.getClass().getSimpleName() + " failed to store blob " + blob.getKey(), t);
            });
        }
    }

    @Override
    public Optional<Blob> read(String key) {
        return readInternal(key);
    }

    @Override
    public void read(String key, BiConsumer<Optional<Blob>, Throwable> callback) {
        if (this.managedAsyncOperation == null) {
            throw new UnsupportedOperationException(this.getClass() + ": async operations not enabled");
        }

        this.managedAsyncOperation.execute(() -> readInternal(key), callback);
    }

    @Override
    public Optional<Blob> read(String key, long timeout, TimeUnit timeUnit) {
        if (this.managedAsyncOperation == null) {
            throw new UnsupportedOperationException(this.getClass() + ": async operations not enabled");
        }

        return this.managedAsyncOperation.execute(() -> readInternal(key), Optional.empty(), timeout, timeUnit);
    }

    @Override
    public void close() throws IOException {
        if (this.managedAsyncOperation != null) {
            this.managedAsyncOperation.close();
        }
    }

    protected abstract void storeInternal(Blob blob);

    protected abstract Optional<Blob> readInternal(String fileKey);
}
