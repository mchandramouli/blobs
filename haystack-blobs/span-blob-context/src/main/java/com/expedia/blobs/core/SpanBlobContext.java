package com.expedia.blobs.core;

import io.opentracing.Span;
import org.apache.commons.lang3.Validate;

/**
 * Class representing a {@link BlobContext} associated with {@link BlobWriter} and uses {@link Span}
 * to save blob key produced to be used again for reading
 */

public class SpanBlobContext implements BlobContext {

    private final Span span;
    private final String serviceName;
    private final static String PARTIAL_BLOB_KEY = "-blob";
    private final String operationName;

    /**
     * constructor
     * @param span span object
     * @param serviceName for a specific service name, can't be null or empty
     * @param operationName for a specific operation name
     */
    public SpanBlobContext(Span span, String serviceName, String operationName) {
        Validate.notNull(span, "span cannot be null in context");
        Validate.notEmpty(serviceName, "service name cannot be null in context");

        this.span = span;
        this.serviceName = serviceName;
        this.operationName = operationName;
    }

    @Override
    public String getOperationName() {
        return operationName;
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    /**
     * This will be used to add the key produced inside the span
     * for it to be used during the time of reading the blob through the span
     * @param blobKey created from {@link SpanBlobContext#makeKey(BlobType)}
     * @param blobType value of {@link BlobType}
     */
    @Override
    public void onBlobKeyCreate(String blobKey, BlobType blobType) {
        span.setTag(String.format("%s%s", blobType.getType(), PARTIAL_BLOB_KEY), blobKey);
    }
}
