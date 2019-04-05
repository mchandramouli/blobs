package com.expedia.blobs.core;

/**
 * Runtime exception for any IO operation
 */
public class BlobReadWriteException extends RuntimeException {
    public BlobReadWriteException(String message, Throwable cause) {
        super(message, cause);
    }

    public BlobReadWriteException(Throwable cause) {
        super(cause);
    }
}
