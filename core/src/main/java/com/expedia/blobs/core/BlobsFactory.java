package com.expedia.blobs.core;

import java.util.function.Predicate;
import org.apache.commons.lang3.Validate;

public class BlobsFactory<T extends BlobContext> {
    private final BlobStore store;
    private final Predicate<T> predicate;

    public BlobsFactory(BlobStore store) {
        this(store, t -> true);
    }

    public BlobsFactory(BlobStore store,
                        Predicate<T> predicate) {
        Validate.notNull(store);
        Validate.notNull(predicate);
        this.store = store;
        this.predicate = predicate;
    }

    public Blobs create(final T context) {
        return predicate.test(context) ? new WritableBlobs(context, store) : new NoOpBlobs();
    }
}

