package com.expedia.blobs.core;

import org.apache.commons.lang3.Validate;

/**
 * Simple class representing a {@link BlobContext} associated with {@link Blobs}
 */
public class SimpleBlobContext implements BlobContext {
    private final String serviceName;
    private final String operationName;

    /**
     * constructor
     * @param serviceName non-empty string
     * @param operationName non-empty string
     */
    public SimpleBlobContext(String serviceName, String operationName) {
        Validate.notEmpty(serviceName);
        Validate.notEmpty(operationName);
        this.serviceName = serviceName;
        this.operationName = operationName;
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    @Override
    public String getOperationName() {
        return operationName;
    }
}
