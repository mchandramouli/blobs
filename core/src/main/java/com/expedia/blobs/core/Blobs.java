package com.expedia.blobs.core;

import java.io.OutputStream;
import java.util.function.Consumer;

public interface Blobs {
    void write(BlobType blobType, ContentType contentType,
          Consumer<OutputStream> callback, Consumer<Metadata> metadata);
}
