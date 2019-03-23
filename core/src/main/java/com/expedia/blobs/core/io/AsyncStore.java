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

    public AsyncStore() {
        this(Runtime.getRuntime().availableProcessors());
    }

    public AsyncStore(int threadPoolSize) {
        this(threadPoolSize > 0 ? new ManagedAsyncOperation(threadPoolSize) : null);
    }

    AsyncStore(ManagedAsyncOperation managedAsyncOperation) {
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
    public Blob read(String fileKey) {
        return readInternal(fileKey);
    }

    @Override
    public void read(String fileKey, BiConsumer<Blob, Throwable> callback) {
        if (this.managedAsyncOperation == null) {
            throw new UnsupportedOperationException(this.getClass() + ": async operations not enabled");
        }

        this.managedAsyncOperation.execute(() -> readInternal(fileKey), callback);
    }

    @Override
    public Optional<Blob> read(String fileKey, long timeout, TimeUnit timeUnit) {
        if (this.managedAsyncOperation == null) {
            throw new UnsupportedOperationException(this.getClass() + ": async operations not enabled");
        }

        return this.managedAsyncOperation.execute(() -> readInternal(fileKey), timeout, timeUnit);
    }

    @Override
    public void close() throws IOException {
        if (this.managedAsyncOperation != null) {
            this.managedAsyncOperation.close();
        }
    }

    protected abstract void storeInternal(Blob blob);

    protected abstract Blob readInternal(String fileKey);
}
