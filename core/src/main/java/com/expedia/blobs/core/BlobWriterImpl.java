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

import com.expedia.www.haystack.agent.blobs.grpc.Blob;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import org.apache.commons.lang.Validate;

public final class BlobWriterImpl implements BlobWriter {
    private final BlobContext context;
    private final BlobStore store;

    BlobWriterImpl(BlobContext context, BlobStore store) {
        Validate.notNull(context);
        Validate.notNull(store);
        this.context = context;
        this.store = store;
    }

    public void write(BlobType blobType, ContentType contentType,
                      Consumer<OutputStream> dataCallback,
                      Consumer<Metadata> metadataCallback) {
        Validate.notNull(blobType);
        Validate.notNull(contentType);
        Validate.notNull(dataCallback);
        Validate.notNull(metadataCallback);

        final String blobKey = context.makeKey(blobType);
        context.onBlobKeyCreate(blobKey, blobType);

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
            } catch (IOException e) {
                throw new BlobReadWriteException(e);
            }
        };

        write(blobKey, metadataSupplier, dataSupplier);
    }

    private void write(String blobKey, Supplier<Map<String, String>> metadataSupplier, Supplier<byte[]> dataSupplier) {
        store.store(new BlobBuilder(blobKey, metadataSupplier, dataSupplier, context));
    }

    public static class BlobBuilder {
        private Blob blob;
        private String blobKey;
        private Supplier<Map<String, String>> metadataSupplier;
        private Supplier<byte[]> dataSupplier;
        private BlobContext context;


        BlobBuilder(String blobKey, Supplier<Map<String, String>> metadataSupplier, Supplier<byte[]> dataSupplier, BlobContext context) {
            this.blobKey = blobKey;
            this.metadataSupplier = metadataSupplier;
            this.dataSupplier = dataSupplier;
            this.context = context;
        }

        @VisibleForTesting
        BlobBuilder(Blob blob){
            this.blob = blob;
        }

        public Blob build() {
            if (blob == null) {
                final Map<String, String> metadata = metadataSupplier.get();
                final byte[] data = dataSupplier.get();

                final Blob.BlobType blobType = BlobType.from(metadata.get("blob-type")) == BlobType.REQUEST ? Blob.BlobType.REQUEST : Blob.BlobType.RESPONSE;

                blob = Blob.newBuilder()
                        .setKey(blobKey)
                        .setServiceName(context.getServiceName())
                        .setOperationName(context.getOperationName())
                        .setBlobType(blobType)
                        .setContentType(metadata.get("content-type"))
                        .putAllMetadata(metadata)
                        .setContent(ByteString.copyFrom(data))
                        .build();
            }
            return blob;
        }
    }
}
