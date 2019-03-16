package com.expedia.blobs.core;

public interface BlobStore {
    void store(Blob blob);

    Blob read(String fileKey);
}
