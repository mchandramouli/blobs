package com.expedia.blobs.core

import org.scalatest.easymock.EasyMockSugar
import org.scalatest.{FunSpec, Matchers}

class SpanBlobContextSpec extends FunSpec with Matchers with EasyMockSugar {

  describe("com.expedia.blobs.core.SpanBlobContext") {

    it("should throw an error if span is not present") {
      val catchExpection = intercept[Exception] {
        val _ = new SpanBlobContext(null, "", "")
      }

      catchExpection.getMessage shouldEqual "span cannot be null in context"
    }
  }
}
