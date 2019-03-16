package com.expedia.blobs.core.io;

import com.expedia.blobs.core.Blob;
import com.expedia.blobs.core.BlobReadWriteException;
import com.expedia.blobs.core.BlobStore;
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

public class FileStore implements BlobStore {
    private final File directory;
    private final Gson gson = new GsonBuilder().create();
    private final Type mapType = new TypeToken<Map<String, String>>(){}.getType();
    private final Charset Utf8 = Charset.forName("UTF-8");

    public FileStore(String directory) {
        this(new File(directory));
    }

    public FileStore(File directory) {
        Validate.isTrue(directory.exists() &&
                                directory.isDirectory() &&
                                directory.canWrite(), "Argument must be an existing directory that is writable");

        this.directory = directory;
    }

    @Override
    public void store(Blob blob) {
        try {
            final String blobPath = FilenameUtils.concat(directory.getAbsolutePath(), blob.getKey());
            FileUtils.writeByteArrayToFile(new File(blobPath), blob.getData());
            FileUtils.write(new File(blobPath + ".meta.json"),
                            gson.toJson(blob.getMetadata()), Utf8);
        }
        catch (IOException e) {
            throw new BlobReadWriteException(e);
        }
    }

    @Override
    public Blob read(String fileKey) {
        try {
            final String blobPath = FilenameUtils.concat(directory.getAbsolutePath(), fileKey);
            final byte[] data = FileUtils.readFileToByteArray(new File(blobPath));
            final String meta = FileUtils.readFileToString(new File(blobPath + ".meta.json"), Utf8);
            final Map<String, String> metadata = gson.fromJson(meta, mapType);
            return new Blob(fileKey, metadata, data);
        }
        catch (IOException e) {
            throw new BlobReadWriteException(e);
        }
    }
}
