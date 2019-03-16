package com.expedia.blobs.core;

import java.util.function.Predicate;

public class BlobsFactory<T extends BlobContext> {
    private final BlobStore store;
    private final Predicate<T> predicate;

    public BlobsFactory(BlobStore store,
                        Predicate<T> predicate) {
        this.store = store;
        this.predicate = predicate;
    }

    public Blobs create(final T context) {
        return predicate.test(context) ? new StorableBlobs(context, store) : new NoOpBlobs();
    }
}

