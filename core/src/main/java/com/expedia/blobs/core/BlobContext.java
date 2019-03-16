package com.expedia.blobs.core;

import java.util.UUID;

public interface BlobContext {
    default String getOperationId() {
        return UUID.randomUUID().toString();
    }

    String getOperationName();


    String getServiceName();

    default String makeKey(BlobType type) {
        return String.format("%s/%s/%s/%s-%s",
                             getServiceName(),
                             getOperationName(),
                             getOperationId(),
                             type, UUID.randomUUID().toString());
    }
}
