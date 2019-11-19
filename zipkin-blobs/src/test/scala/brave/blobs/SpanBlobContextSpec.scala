package brave.blobs

import brave.Span
import brave.propagation.TraceContext
import com.expedia.blobs.core.BlobType
import org.easymock.EasyMock.{replay, verify}
import org.scalatest.easymock.EasyMockSugar
import org.scalatest.{FunSpec, Matchers}

class SpanBlobContextSpec extends FunSpec with Matchers with EasyMockSugar {

  describe("brave.blobs.SpanBlobContext") {

    it("should throw an error if span is not present") {
      val catchExpection = intercept[Exception] {
        val _ = new SpanBlobContext(null, "", "")
      }

      catchExpection.getMessage shouldEqual "span cannot be null in context"
    }

    it("should throw an error if serviceName is not present") {
      val catchExpection = intercept[Exception] {
        val _ = new SpanBlobContext(mock[Span], "", "")
      }

      catchExpection.getMessage shouldEqual "remoteServiceName name cannot be null or empty in context"
    }

    it("should throw an error if name is not present") {
      val catchExpection = intercept[Exception] {
        val _ = new SpanBlobContext(mock[Span], "remote-service", "")
      }

      catchExpection.getMessage shouldEqual "name cannot be null or empty in context"
    }

    it("should add a tag to the Span when onBlobKeyCreate is invoked") {
      val span = mock[Span]
      val traceContext = TraceContext.newBuilder().traceId(2345678901L).spanId(1234567890L).build()
      val spanBlobContext = new SpanBlobContext(span, "remote-service", "span-name")

      expecting {
        span.tag("request-blob", "remote-service_span-name_1234567890_request").andReturn(span).once()
        span.context().andReturn(traceContext).once()
      }
      replay(span)


      spanBlobContext.onBlobKeyCreate(spanBlobContext.makeKey(BlobType.REQUEST), BlobType.REQUEST)

      verify(span)
    }
  }
}
