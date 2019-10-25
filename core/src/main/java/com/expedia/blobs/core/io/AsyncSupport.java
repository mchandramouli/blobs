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

import com.expedia.blobs.core.BlobStore;
import com.expedia.blobs.core.BlobWriterImpl;
import com.expedia.blobs.model.Blob;
import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AsyncSupport implements BlobStore, Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncSupport.class);
    private final ManagedAsyncOperation managedAsyncOperation;

    /**
     * Initializes {@link AsyncSupport} instance with a threadpool
     * and the time it waits before it is forcefully shut down(in seconds) if the number of
     * threads given are greater than zero
     * @param threadPoolSize 1 or more
     * @param shutdownWaitInSeconds Waiting time before the threadpool is forcefully shutdown
     */

    public AsyncSupport(int threadPoolSize, int shutdownWaitInSeconds){
        this.managedAsyncOperation = new ManagedAsyncOperation(threadPoolSize, shutdownWaitInSeconds);
    }

    @Override
    public void store(BlobWriterImpl.BlobBuilder blobBuilder) {
        store (blobBuilder, (v, t) -> {
            if (t != null) {
                LOGGER.error(this.getClass().getSimpleName() + " failed to store blob ", t);
            }
        });
    }

    @Override
    public Optional<Blob> read(String key) {
        try {
            return readInternal(key);
        }
        catch (Exception e) {
            LOGGER.error(this.getClass().getSimpleName() + " failed to read blob " + key, e);
            return Optional.empty();
        }
    }

    @Override
    public void read(String key, BiConsumer<Optional<Blob>, Throwable> callback) {
        this.managedAsyncOperation.execute(() -> readInternal(key), callback);
    }

    @Override
    public Optional<Blob> read(String key, long timeout, TimeUnit timeUnit) {
        return this.managedAsyncOperation.execute(() -> readInternal(key), Optional.empty(), timeout, timeUnit);
    }

    @Override
    public void close() {
        if (this.managedAsyncOperation != null) {
            this.managedAsyncOperation.close();
        }
    }

    protected final void store(BlobWriterImpl.BlobBuilder blobBuilder, BiConsumer<Void, Throwable> errorHandler) {
        this.managedAsyncOperation.execute(() -> storeInternal(blobBuilder), errorHandler);
    }

    protected abstract void storeInternal(BlobWriterImpl.BlobBuilder blobBuilder);

    protected abstract Optional<Blob> readInternal(String key);
}
