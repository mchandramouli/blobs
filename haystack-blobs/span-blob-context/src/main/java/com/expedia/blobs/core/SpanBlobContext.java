package com.expedia.blobs.core;

import com.expedia.www.haystack.client.Span;
import org.apache.commons.lang3.Validate;

/**
 * Class representing a {@link BlobContext} associated with {@link BlobWriter} and uses {@link Span}
 * to save blob key produced to be used again for reading
 */

public class SpanBlobContext implements BlobContext {
    private Span span;
    private final static String PARTIAL_BLOB_KEY = "-blob";

    /**
     * constructor
     * @param span for a specific service operation
     */

    public SpanBlobContext(Span span) {
        Validate.notNull(span, "Span cannot be null in context");
        this.span = span;
    }

    @Override
    public String getOperationName() {
        return span.getOperationName();
    }

    @Override
    public String getServiceName() {
        return span.getServiceName();
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
