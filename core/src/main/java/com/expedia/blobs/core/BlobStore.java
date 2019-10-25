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
package com.expedia.blobs.core;

import com.expedia.blobs.model.Blob;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Interface for storage implementations that can save and retrieve {@link Blob} instances
 */
public interface BlobStore {
    /**
     * Store a {@link Blob} instance
     *
     * @param blobBuilder Non-null instance of {@link BlobWriterImpl.BlobBuilder}
     */
    void store(BlobWriterImpl.BlobBuilder blobBuilder);

    /**
     * Reads a {@link Blob} instance associated with the given key
     *
     * @param key non-null key
     * @return valid Optional instance
     */
    Optional<Blob> read(String key);

    /**
     * Performs the read operation asynchronously and invokes the callback
     * when the blob is read. If there are no blobs associated with the key then
     * `callback` will be invoked with an empty Optional instance. If there is an error reading
     * the blob then the callback will be invoked with the exception
     *
     * @param key      non-null key
     * @param callback non-null consumer to be invoked
     */
    void read(String key, BiConsumer<Optional<Blob>, Throwable> callback);

    /**
     * Performs the read operation asynchronously and returns the blob when it is read
     * or an empty Optional instance if the given timeout period is elapsed
     *
     * @param key      non-null key
     * @param timeout  time to wait for the operation to complete
     * @param timeUnit time unit associated with the time to wait
     * @return valid Optional instance
     */
    Optional<Blob> read(String key, long timeout, TimeUnit timeUnit);
}
