package com.expedia.blobs.stores.aws;

import com.expedia.blobs.core.Blob;
import com.expedia.blobs.core.BlobStore;

public class S3BlobStore implements BlobStore {
    @Override
    public void store(Blob blob) {

    }

    @Override
    public Blob read(String fileKey) {
        return null;
    }
}
