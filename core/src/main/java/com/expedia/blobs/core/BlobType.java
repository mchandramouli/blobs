package com.expedia.blobs.core;

import org.apache.commons.lang3.Validate;

/**
 * Class representing the type of {@link Blob} object being saved or retrieved
 */
public class BlobType {
    private final String type;
    public static BlobType REQUEST = BlobType.from("request");
    public static BlobType RESPONSE = BlobType.from("response");

    private BlobType(String type) {
        Validate.notEmpty(type);
        this.type = type;
    }

    public String getType() {
        return this.type;
    }

    /**
     * factory method to create a {@link BlobType} instance
     * @param type non-empty string
     * @return valid BlobType instance
     */
    public static BlobType from(String type) {
        return new BlobType(type);
    }
}
