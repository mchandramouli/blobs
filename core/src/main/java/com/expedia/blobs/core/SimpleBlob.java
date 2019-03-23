package com.expedia.blobs.core;

import java.util.Map;

public final class SimpleBlob implements Blob {
    private final String key;
    private final Map<String, String> metadata;
    private final byte[] data;

    public SimpleBlob(String key, Map<String, String> metadata, byte[] data) {
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
