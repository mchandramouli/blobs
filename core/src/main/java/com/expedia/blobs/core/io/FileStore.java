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
import org.apache.commons.lang3.Validate;

public class FileStore extends AsyncStore {
    private final File directory;
    private final Gson gson = new GsonBuilder().create();
    private final Type mapType = new TypeToken<Map<String, String>>() {
    }.getType();
    private final Charset Utf8 = Charset.forName("UTF-8");

    public FileStore(String directory) {
        this(new File(directory));
    }

    public FileStore(File directory) {
        this(directory, Runtime.getRuntime().availableProcessors());
    }

    public FileStore(File directory, int threadPoolSize) {
        super(threadPoolSize);

        Validate.isTrue(directory.exists() &&
                                directory.isDirectory() &&
                                directory.canWrite(), "Argument must be an existing directory that is writable");

        this.directory = directory;
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
        }
        catch (IOException e) {
            throw new BlobReadWriteException(e);
        }
    }

    @Override
    protected Optional<Blob> readInternal(String fileKey) {
        try {
            final String blobPath = FilenameUtils.concat(directory.getAbsolutePath(), fileKey);
            final String meta = FileUtils.readFileToString(new File(blobPath + ".meta.json"), Utf8);
            final Map<String, String> metadata = gson.fromJson(meta, mapType);
            final byte[] data = FileUtils.readFileToByteArray(new File(blobPath));
            return Optional.of(new SimpleBlob(fileKey, metadata, data));
        }
        catch (IOException e) {
            throw new BlobReadWriteException(e);
        }
    }
}
