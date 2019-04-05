package com.expedia.blobs.core;

import org.apache.commons.lang3.Validate;

/**
 * Class representing the content type of {@link Blob} object being saved or retrieved
 */
public class ContentType {
    private final String type;
    public static ContentType JSON = ContentType.from("application/json");
    public static ContentType XML = ContentType.from("application/xml");

    private ContentType(String type) {
        Validate.notEmpty(type);
        this.type = type;
    }

    public String getType() {
        return this.type;
    }

    /**
     * factory method to create a {@link ContentType} instance
     * @param type non-empty mime-type string
     * @return valid BlobType instance
     */
    public static ContentType from(String type) {
        return new ContentType(type);
    }
}
