package com.expedia.blobs.core;

import java.util.Map;

public class Blob {
    private final String key;
    private final Map<String, String> metadata;
    private final byte[] data;

    public Blob(String fileKey, Map<String, String> metadata, byte[] data) {
        this.key = fileKey;
        this.metadata = metadata;
        this.data = data;
    }

    public String getKey() {
        return key;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public byte[] getData() {
        return data;
    }
}
