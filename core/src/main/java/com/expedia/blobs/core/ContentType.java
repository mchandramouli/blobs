package com.expedia.blobs.core;

public class ContentType {
    private final String type;
    public static ContentType JSON = ContentType.from("application/json");
    public static ContentType XML = ContentType.from("application/xml");

    private ContentType(String type) {
        this.type = type;
    }

    public String getType() {
        return this.type;
    }

    public static ContentType from(String type) {
        return new ContentType(type);
    }
}
