package com.expedia.blobs.core;

public class SimpleBlobContext implements BlobContext {
    private final String serviceName;
    private final String operationName;

    public SimpleBlobContext(String serviceName, String operationName) {
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
