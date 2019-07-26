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

import org.apache.commons.lang.Validate;

import java.util.function.Predicate;

/**
 * Starting point to obtain an instance of {@link BlobWriter} to write
 * {@link com.expedia.www.blobs.model.Blob} instances to be saved using a given {@link BlobStore}
 * @param <T> Actual type of {@link BlobContext} associated with the {@link BlobWriter} instance
 */
public class BlobsFactory<T extends BlobContext> {
    private final BlobStore store;
    private final Predicate<T> predicate;

    /**
     * Constructor
     * @param store valid instance of {@link BlobStore}
     */
    public BlobsFactory(BlobStore store) {
        this(store, t -> true);
    }

    /**
     * Constructor
     * @param store valid instance of {@link BlobStore}
     * @param predicate predicate instance to test if Blobs be written for a given {@link BlobContext}
     */
    public BlobsFactory(BlobStore store,
                        Predicate<T> predicate) {
        Validate.notNull(store);
        Validate.notNull(predicate);
        this.store = store;
        this.predicate = predicate;
    }

    /**
     * Creates a new instance of {@link BlobWriter} that can be used to write one or more {@link com.expedia.www.blobs.model.Blob} instances
     * associated with the same {@link BlobContext}.
     *
     * This method will return a no-op Blobs instance if the given `context` does not qualify
     * to be written out. i.e., predicate provided in the constructor can be used as a sampler or rate-limiter.
     * See {@link com.expedia.blobs.core.predicates.BlobsRateLimiter}
     *
     * @param context {@link BlobContext} associated with the Blobs
     * @return valid {@link BlobWriter} instance
     */
    public BlobWriter create(final T context) {
        Validate.notNull(context);
        return predicate.test(context) ? new BlobWriterImpl(context, store) : new NoOpBlobWriterImpl();
    }
}

