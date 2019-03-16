package com.expedia.blobs.core;

public class BlobType {
    private final String type;
    public static BlobType REQUEST = BlobType.from("request");
    public static BlobType RESPONSE = BlobType.from("response");

    private BlobType(String type) {
        this.type = type;
    }

    public String getType() {
        return this.type;
    }

    public static BlobType from(String type) {
        return new BlobType(type);
    }
}
