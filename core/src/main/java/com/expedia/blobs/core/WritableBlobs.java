package com.expedia.blobs.core;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

final class WritableBlobs implements Blobs {
    private final BlobContext context;
    private final BlobStore store;

    WritableBlobs(BlobContext context, BlobStore store) {
        this.context = context;
        this.store = store;
    }

    public void write(BlobType blobType, ContentType contentType,
                 Consumer<OutputStream> dataCallback, Consumer<Metadata> metadataCallback) {
        String fileKey = context.makeKey(blobType);

        final Supplier<Map<String, String>> metadataSupplier = () -> {
            final Map<String, String> metadata = new HashMap<>(2);
            metadataCallback.accept(metadata::put);
            metadata.put("blob-type", blobType.toString());
            metadata.put("content-type", contentType.toString());
            return metadata;
        };

        final Supplier<byte[]> dataSupplier = () -> {
            try (final ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
                dataCallback.accept(bos);
                return bos.toByteArray();
            }
            catch (IOException e) {
                throw new BlobReadWriteException(e);
            }
        };

        write(fileKey, metadataSupplier, dataSupplier);
    }

    private void write(String fileKey,
                       Supplier<Map<String, String>> metadataSupplier, Supplier<byte[]> dataSupplier) {
        final WritableBlob b = new WritableBlob(fileKey, metadataSupplier, dataSupplier);
        store.store(b);
    }

    public class WritableBlob implements Blob {
        private final String key;
        private final Supplier<Map<String, String>>  metadataSupplier;
        private final Supplier<byte[]> dataSupplier;
        private Map<String, String> metadata;
        private byte[] data;

        WritableBlob(String fileKey, Supplier<Map<String, String>>  metadataSupplier,
                            Supplier<byte[]> dataSupplier) {
            this.key = fileKey;
            this.metadataSupplier = metadataSupplier;
            this.dataSupplier = dataSupplier;
        }

        public String getKey() {
            return key;
        }

        public Map<String, String> getMetadata() {
            ensureMetadata();
            return metadata;
        }

        public byte[] getData() {
            ensureData();
            return data;
        }

        public int getSize() {
            ensureData();
            return data.length;
        }

        private void ensureMetadata() {
            if (metadata == null) {
                metadata = metadataSupplier.get();
            }
        }

        private void ensureData() {
            if (data == null) {
                data = dataSupplier.get();
            }
        }
    }
}
