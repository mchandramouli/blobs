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
package com.expedia.blobs.core.io;

import com.expedia.blobs.core.Blob;
import com.expedia.blobs.core.BlobReadWriteException;
import com.expedia.blobs.core.SimpleBlob;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileStore extends AsyncSupport {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileStore.class);
    private final File directory;
    private final Gson gson = new GsonBuilder().create();
    private final Type mapType = new TypeToken<Map<String, String>>() {
    }.getType();
    private final Charset Utf8 = Charset.forName("UTF-8");
    ShutdownHook shutdownHook;

    protected FileStore(FileStore.Builder builder) {
        super(builder.threadPoolSize, builder.shutdownWaitInSeconds);

        Validate.isTrue(builder.directory.exists() &&
                builder.directory.isDirectory() &&
                builder.directory.canWrite(), "Argument must be an existing directory that is writable");

        this.directory = builder.directory;

        if (builder.manualShutdown) {
            LOGGER.info("No shutdown hook registered: Please call close() manually on application shutdown.");
        } else {
            shutdownHook = new ShutdownHook();
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        }
    }

    @Override
    protected void storeInternal(Blob blob) {
        try {
            if (blob.getSize() > 0) {
                final String blobPath = FilenameUtils.concat(directory.getAbsolutePath(), blob.getKey());
                final byte[] data = blob.getData();
                final Map<String, String> metadata = blob.getMetadata();
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
            return Optional.of(new SimpleBlob(key, metadata, data));
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
        private boolean manualShutdown;

        public Builder(String directory) {
            this(new File(directory));
        }

        public Builder(File directory) {
            this.directory = directory;
            this.threadPoolSize = Runtime.getRuntime().availableProcessors();
            this.shutdownWaitInSeconds = 60;
            this.manualShutdown = false;
        }

        public Builder withThreadPoolSize(int threadPoolSize) {
            this.threadPoolSize = threadPoolSize;
            return this;
        }

        public Builder withShutdownWaitInSeconds(int shutdownWaitInSeconds) {
            this.shutdownWaitInSeconds = shutdownWaitInSeconds;
            return this;
        }

        public Builder withManualShutdown() {
            this.manualShutdown = true;
            return this;
        }

        public FileStore build() {
            return new FileStore(this);
        }
    }

    private class ShutdownHook extends Thread {
        @Override
        public void run() {
            FileStore.this.close();
        }
    }
}
