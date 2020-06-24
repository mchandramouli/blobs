package io.opentracing.blobs;

import com.expedia.blobs.core.BlobType;
import io.opentracing.Span;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

import static org.mockito.Mockito.mock;

public class SpanBlobContextTest {

    @Test
    public void verifyConstructorThrowsExceptionWithoutSpan() {
        try {
            new SpanBlobContext(null, "remote", "operation");
        } catch (Exception e) {
            Assert.assertEquals("span cannot be null in context", e.getMessage());
        }
    }

    @Test
    public void verifyConstructorThrowsExceptionWithoutServiceName() {
        try {
            new SpanBlobContext(mock(Span.class), "", "operation");
        } catch (Exception e) {
            Assert.assertEquals("remoteServiceName name cannot be null or empty in context", e.getMessage());
        }
    }

    @Test
    public void verifyConstructorThrowsExceptionWithoutOperationName() {
        try {
            new SpanBlobContext(mock(Span.class), "remote", "");
        } catch (Exception e) {
            Assert.assertEquals("name cannot be null or empty in context", e.getMessage());
        }
    }

    @Test
    public void verifyTagIsAddedOnBlobCreate() {
        MockTracer tracer = new MockTracer();
        MockSpan span = tracer.buildSpan("span-name").start();
        SpanBlobContext spanBlobContext = new SpanBlobContext(span, "remote-service", "span-name");

        spanBlobContext.onBlobKeyCreate(spanBlobContext.makeKey(BlobType.REQUEST), BlobType.REQUEST);
        Assert.assertEquals(1, span.tags().size());
        Object blobKey = span.tags().get("request-blob");
        Assert.assertEquals("remote-service_span-name_2_request", blobKey);
    }
}
