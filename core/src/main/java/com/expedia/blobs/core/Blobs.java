package com.expedia.blobs.core;

import java.io.OutputStream;
import java.util.function.Consumer;

/**
 * Class representing the instance returned by {@link BlobsFactory} to
 * write blobs to be saved
 */
public interface Blobs {
    void write(BlobType blobType, ContentType contentType,
          Consumer<OutputStream> callback, Consumer<Metadata> metadata);
}
