/*
 *
 *     Copyright 2018 Expedia, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 *
 */
package com.expedia.blobs.core;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.commons.lang3.Validate;

final class WritableBlobs implements Blobs {
    private final BlobContext context;
    private final BlobStore store;

    WritableBlobs(BlobContext context, BlobStore store) {
        Validate.notNull(context);
        Validate.notNull(store);
        this.context = context;
        this.store = store;
    }

    public void write(BlobType blobType, ContentType contentType,
                 Consumer<OutputStream> dataCallback, Consumer<Metadata> metadataCallback) {
        Validate.notNull(blobType);
        Validate.notNull(contentType);
        Validate.notNull(dataCallback);
        Validate.notNull(metadataCallback);

        String blobKey = context.makeKey(blobType);

        final Supplier<Map<String, String>> metadataSupplier = () -> {
            final Map<String, String> metadata = new HashMap<>(2);
            metadataCallback.accept(metadata::put);
            metadata.put("blob-type", blobType.getType());
            metadata.put("content-type", contentType.getType());
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

        write(blobKey, metadataSupplier, dataSupplier);
    }

    private void write(String blobKey,
                       Supplier<Map<String, String>> metadataSupplier, Supplier<byte[]> dataSupplier) {
        final WritableBlob b = new WritableBlob(blobKey, metadataSupplier, dataSupplier);
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
