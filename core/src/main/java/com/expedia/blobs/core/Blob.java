package com.expedia.blobs.core;

import java.util.Map;

/**
 * Container class that represents an instance of a binary large object along with
 * its metadata and key
 */
public interface Blob {
    /**
     * unique key of the blob instance
     * @return string
     */
    String getKey();

    /**
     * Map of metadata
     * @return a valid map object
     */
    Map<String, String> getMetadata();

    /**
     * Array of bytes representing the object
     * @return valid array
     */
    byte[] getData();

    /**
     * size of the object
     * @return integer
     */
    int getSize();
}
