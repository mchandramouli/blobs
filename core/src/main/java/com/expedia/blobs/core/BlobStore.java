package com.expedia.blobs.core;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public interface BlobStore {
    void store(Blob blob);

    Blob read(String fileKey);

    void read(String fileKey, BiConsumer<Blob, Throwable> callback);

    Optional<Blob> read(String fileKey, long timeout, TimeUnit timeUnit);
}
