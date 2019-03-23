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
    protected Blob readInternal(String fileKey) {
        try {
            final String blobPath = FilenameUtils.concat(directory.getAbsolutePath(), fileKey);
            final String meta = FileUtils.readFileToString(new File(blobPath + ".meta.json"), Utf8);
            final Map<String, String> metadata = gson.fromJson(meta, mapType);
            final byte[] data = FileUtils.readFileToByteArray(new File(blobPath));
            return new SimpleBlob(fileKey, metadata, data);
        }
        catch (IOException e) {
            throw new BlobReadWriteException(e);
        }
    }
}
