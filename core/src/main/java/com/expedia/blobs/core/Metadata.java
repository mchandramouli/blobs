package com.expedia.blobs.core;

/**
 * interface of a class representing metadata associated with a {@link Blob} object
 */
public interface Metadata {
    void add(String key, String value);
}
