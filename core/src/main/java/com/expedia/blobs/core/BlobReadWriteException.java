package com.expedia.blobs.core;

import java.io.IOException;

public class BlobReadWriteException extends RuntimeException {
    public BlobReadWriteException(IOException cause) {
        super(cause);
    }
}
