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
package com.expedia.blobs.stores.io;

import com.expedia.blobs.core.BlobReadWriteException;
import com.expedia.blobs.core.BlobWriterImpl;
import com.expedia.blobs.core.io.AsyncSupport;
import com.expedia.www.haystack.agent.blobs.grpc.Blob;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.ByteString;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Optional;

public class FileStore extends AsyncSupport {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileStore.class);
    private final File directory;
    private final Gson gson = new GsonBuilder().create();
    private final Type mapType = new TypeToken<Map<String, String>>() {
    }.getType();
    private final Charset Utf8 = Charset.forName("UTF-8");
    private Thread shutdownHook = new Thread(() -> this.close());

    @VisibleForTesting
    Boolean shutdownHookAdded = false;

    FileStore(FileStore.Builder builder) {
        super(builder.threadPoolSize, builder.shutdownWaitInSeconds);

        this.directory = builder.directory;

        if (builder.closeOnShutdown) {
            shutdownHookAdded = builder.closeOnShutdown;
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        } else {
            LOGGER.info("No shutdown hook registered: Please call close() manually on application shutdown.");
        }
    }

    @Override
    protected void storeInternal(BlobWriterImpl.BlobBuilder blobSupplier) {
        try {

            final Blob blob = blobSupplier.build();

            if (blob.getContent().size() > 0) {
                final String blobPath = FilenameUtils.concat(directory.getAbsolutePath(), blob.getKey());
                final byte[] data = blob.getContent().toByteArray();
                final Map<String, String> metadata = blob.getMetadataMap();
                FileUtils.writeByteArrayToFile(new File(blobPath), data);
                FileUtils.write(new File(blobPath + ".meta.json"), gson.toJson(metadata), Utf8);
            }
        } catch (IOException e) {
            throw new BlobReadWriteException(e);
        }
    }

    @Override
    protected Optional<Blob> readInternal(String key) {
        try {
            final String blobPath = FilenameUtils.concat(directory.getAbsolutePath(), key);
            final String meta = FileUtils.readFileToString(new File(blobPath + ".meta.json"), Utf8);
            final Map<String, String> metadata = gson.fromJson(meta, mapType);
            final byte[] data = FileUtils.readFileToByteArray(new File(blobPath));

            Blob blob = Blob.newBuilder()
                    .setKey(key)
                    .putAllMetadata(metadata)
                    .setContent(ByteString.copyFrom(data))
                    .build();

            return Optional.of(blob);
        } catch (IOException e) {
            throw new BlobReadWriteException(e);
        }
    }

    @Override
    public void close() {
        super.close();
    }

    /**
     * Builds the {@link FileStore} with options
     */

    public static class Builder {
        private final File directory;
        private int threadPoolSize;
        private int shutdownWaitInSeconds;
        private boolean closeOnShutdown;

        public Builder(String directory) {
            this(new File(directory));
        }

        public Builder(File directory) {
            this.directory = directory;
            this.threadPoolSize = Runtime.getRuntime().availableProcessors();
            this.shutdownWaitInSeconds = 60;
            this.closeOnShutdown = true;
        }

        public Builder withThreadPoolSize(int threadPoolSize) {
            this.threadPoolSize = threadPoolSize;
            return this;
        }

        public Builder withShutdownWaitInSeconds(int shutdownWaitInSeconds) {
            this.shutdownWaitInSeconds = shutdownWaitInSeconds;
            return this;
        }

        public Builder disableAutoShutdown() {
            this.closeOnShutdown = false;
            return this;
        }

        public FileStore build() {

            Validate.isTrue(directory.exists() &&
                    directory.isDirectory() &&
                    directory.canWrite(), "Argument must be an existing directory that is writable");

            return new FileStore(this);
        }
    }
}
