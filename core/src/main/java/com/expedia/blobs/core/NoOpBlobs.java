package com.expedia.blobs.core;

import java.io.OutputStream;
import java.util.function.Consumer;

public class NoOpBlobs implements Blobs {
    public void write(BlobType blobType, ContentType contentType,
                      Consumer<OutputStream> callback, Consumer<Metadata> metadata) {
        //nothing
    }
}
