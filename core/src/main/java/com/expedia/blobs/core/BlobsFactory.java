package com.expedia.blobs.core;

import java.util.function.Predicate;
import org.apache.commons.lang3.Validate;

/**
 * Starting point to obtain an instance of {@link Blobs} to write
 * {@link Blob} instances to be saved using a given {@link BlobStore}
 * @param <T> Actual type of {@link BlobContext} associated with the {@link Blobs} instance
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
     * Creates a new instance of {@link Blobs} that can be used to write one or more {@link Blob} instances
     * associated with the same {@link BlobContext}.
     *
     * This method will return a no-op Blobs instance if the given `context` does not qualify
     * to be written out. i.e., predicate provided in the constructor can be used as a sampler or rate-limiter.
     * See {@link com.expedia.blobs.core.predicates.BlobsRateLimiter}
     *
     * @param context {@link BlobContext} associated with the Blobs
     * @return valid {@link Blobs} instance
     */
    public Blobs create(final T context) {
        Validate.notNull(context);
        return predicate.test(context) ? new WritableBlobs(context, store) : new NoOpBlobs();
    }
}

