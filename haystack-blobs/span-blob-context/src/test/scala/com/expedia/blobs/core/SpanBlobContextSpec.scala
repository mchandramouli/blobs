package com.expedia.blobs.core

import com.expedia.www.haystack.client.Tracer
import com.expedia.www.haystack.client.dispatchers.NoopDispatcher
import com.expedia.www.haystack.client.metrics.NoopMetricsRegistry
import org.junit.Assert
import org.scalatest.easymock.EasyMockSugar
import org.scalatest.{FunSpec, Matchers}

class SpanBlobContextSpec extends FunSpec with Matchers with EasyMockSugar {

  private val tracer = new Tracer.Builder(new NoopMetricsRegistry, "TestService", new NoopDispatcher).build()
  private val span = tracer.buildSpan("TestOperation").start

  describe("com.expedia.blobs.core.SpanBlobContext") {

    it("should throw an error if span is not present") {
      val catchExpection = intercept[Exception] {
        val spanBlobContext: SpanBlobContext = new SpanBlobContext(null)
      }

      catchExpection.getMessage shouldEqual "Span cannot be null in context"
    }

    it("should return the correct operation name") {
      val operationName = new SpanBlobContext(span).getOperationName
      Assert.assertEquals("TestOperation", operationName)
    }

    it("should return the correct service name") {
      val serviceName = new SpanBlobContext(span).getServiceName
      Assert.assertEquals("TestService", serviceName)
    }
  }
}
