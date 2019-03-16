package com.expedia.blobs.core;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

class StorableBlobs implements Blobs {
    private final BlobContext context;
    private final BlobStore store;

    StorableBlobs(BlobContext context, BlobStore store) {
        this.context = context;
        this.store = store;
    }

    public void write(BlobType blobType, ContentType contentType,
                 Consumer<OutputStream> callback, Consumer<Metadata> metadataCallback) {
        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream()) {

            callback.accept(bos);

            if (bos.size() > 0) {
                String fileKey = context.makeKey(blobType);

                final Map<String, String> metadata = new HashMap<>(2);
                metadataCallback.accept(metadata::put);
                metadata.put("blob-type", blobType.toString());
                metadata.put("content-type", contentType.toString());

                write(fileKey, metadata, bos.toByteArray());
            }
        }
        catch (IOException e) {
            throw new BlobReadWriteException(e);
        }
    }

    private void write(String fileKey,
                    Map<String, String> metadata, byte[] data) {
        Blob b = new Blob(fileKey, metadata, data);
        store.store(b);
    }
}
