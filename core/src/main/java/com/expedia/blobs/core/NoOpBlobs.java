package com.expedia.blobs.core;

import java.io.OutputStream;
import java.util.function.Consumer;

/**
 * NoOp implementation of {@link Blobs}
 */
class NoOpBlobs implements Blobs {
    public void write(BlobType blobType, ContentType contentType,
                      Consumer<OutputStream> callback, Consumer<Metadata> metadata) {
        //nothing
    }
}
