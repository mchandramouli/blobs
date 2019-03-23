package com.expedia.blobs.core;

import java.util.Map;

public interface Blob {
    String getKey();

    Map<String, String> getMetadata();

    byte[] getData();

    int getSize();
}
