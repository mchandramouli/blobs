package com.expedia.blobs.core.io;

import java.io.InputStream;

public class BlobInputStream {
    private final InputStream stream;
    private final long length;

    public BlobInputStream(final InputStream stream, long length) {
        this.stream = stream;
        this.length = length;
    }


    public InputStream getStream() {
        return stream;
    }

    public long getLength() {
        return length;
    }
}
