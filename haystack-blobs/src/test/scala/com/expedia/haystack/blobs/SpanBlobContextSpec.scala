package com.expedia.haystack.blobs

import java.util

import com.expedia.blobs.core.BlobType
import com.expedia.www.haystack.client._
import org.easymock.EasyMock
import org.scalatest.easymock.EasyMockSugar
import org.scalatest.{FunSpec, Matchers}

class SpanBlobContextSpec extends FunSpec with Matchers with EasyMockSugar {

  describe("com.expedia.haystack.blobs.SpanBlobContext") {

    it("should throw an error if span is not present") {
      val catchExpection = intercept[Exception] {
        val _ = new SpanBlobContext(null)
      }

      catchExpection.getMessage shouldEqual "span cannot be null in context"
    }

    it("should add a tag to the Span when onBlobKeyCreate is invoked") {
      val spanContext = mock[SpanContext]
      val tracer = mock[Tracer]
      val tags =  new util.HashMap[String, Object]
      val span = MockSpanBuilder.mockSpan(tracer, mock[Clock], "span-name", spanContext,
        System.currentTimeMillis(), tags, new util.ArrayList[Reference])
      val spanBlobContext = new SpanBlobContext(span)

      expecting {
        tracer.getServiceName.andReturn("remote-service").once()
        spanContext.getSpanId.andReturn("1234567890").once()
      }
      EasyMock.replay(tracer, spanContext)


      spanBlobContext.onBlobKeyCreate(spanBlobContext.makeKey(BlobType.REQUEST), BlobType.REQUEST)

      EasyMock.verify(tracer, spanContext)
      tags.get("request-blob") should equal ("remote-service_span-name_1234567890_request")
    }
  }
}
