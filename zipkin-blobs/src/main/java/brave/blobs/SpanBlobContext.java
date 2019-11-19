package brave.blobs;

import brave.Span;
import com.expedia.blobs.core.BlobContext;
import com.expedia.blobs.core.BlobType;
import com.expedia.blobs.core.BlobWriter;
import org.apache.commons.lang3.Validate;

/**
 * Class representing a {@link BlobContext} associated with {@link BlobWriter} and uses {@link Span}
 * to save blob key produced to be used again for reading
 */

public class SpanBlobContext implements BlobContext {

    private final Span span;
    private final String remoteServiceName;
    private final static String PARTIAL_BLOB_KEY = "-blob";
    private final String name;

    /**
     * constructor
     * @param span span object
     * @param remoteServiceName for a specific service name, can't be null or empty
     * @param name for a specific operation name
     */
    public SpanBlobContext(Span span, String remoteServiceName, String name) {
        Validate.notNull(span, "span cannot be null in context");
        Validate.notEmpty(remoteServiceName, "remoteServiceName name cannot be null or empty in context");
        Validate.notEmpty(name, "name cannot be null or empty in context");

        this.span = span;
        this.remoteServiceName = remoteServiceName;
        this.name = name;
    }

    @Override
    public String getOperationName() {
        return this.name;
    }

    @Override
    public String getServiceName() {
        return this.remoteServiceName;
    }

    @Override
    public String getOperationId() {
        return String.valueOf(this.span.context().spanId());
    }

    /**
     * This will be used to add the key produced inside the span
     * for it to be used during the time of reading the blob through the span
     * @param blobKey created from {@link SpanBlobContext#makeKey(BlobType)}
     * @param blobType value of {@link BlobType}
     */
    @Override
    public void onBlobKeyCreate(String blobKey, BlobType blobType) {
        span.tag(String.format("%s%s", blobType.getType(), PARTIAL_BLOB_KEY), blobKey);
    }
}
