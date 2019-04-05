package com.expedia.blobs.core;

import java.util.UUID;

/**
 * Operation context related to the blob
 */
public interface BlobContext {
    /**
     * unique id of the operation
     * @return string
     */
    default String getOperationId() {
        return UUID.randomUUID().toString();
    }

    /**
     * name of the operation
     * @return string
     */
    String getOperationName();

    /**
     * name of the service
     * @return string
     */
    String getServiceName();

    /**
     * create a unique key for the given {@link BlobType}
     * @param type type of the blob being saved
     * @return string
     */
    default String makeKey(BlobType type) {
        return String.format("%s/%s/%s/%s-%s",
                             getServiceName(),
                             getOperationName(),
                             getOperationId(),
                             type, UUID.randomUUID().toString());
    }
}
