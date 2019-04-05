package com.expedia.blobs.core;

import java.util.Map;
import org.apache.commons.lang3.Validate;

/**
 * Simple implementation of {@link Blob}
 */
public final class SimpleBlob implements Blob {
    private final String key;
    private final Map<String, String> metadata;
    private final byte[] data;

    /**
     * Constructor
     * @param key non-empty key
     * @param metadata non-null map
     * @param data non-null data
     */
    public SimpleBlob(String key, Map<String, String> metadata, byte[] data) {
        Validate.notEmpty(key);
        Validate.notNull(metadata);
        Validate.notNull(data);
        this.key = key;
        this.metadata = metadata;
        this.data = data;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public Map<String, String> getMetadata() {
        return metadata;
    }

    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public int getSize() {
        return data.length;
    }
}
