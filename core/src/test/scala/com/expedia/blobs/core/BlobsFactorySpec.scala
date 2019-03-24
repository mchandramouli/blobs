package com.expedia.blobs.core

import org.scalatest.easymock.EasyMockSugar._
import org.scalatest.{FunSpec, GivenWhenThen, Matchers, BeforeAndAfter}

class BlobsFactorySpec extends FunSpec with GivenWhenThen with BeforeAndAfter with Matchers {
  describe("blobs creation") {
    it("should default blobs filter to true if one is not specified") {
      Given("a blobs factory")
      val store = mock[BlobStore]
      val factory = new BlobsFactory[SimpleBlobContext](store)
      When("a new blobs instance is requested")
      val blobs = factory.create(new SimpleBlobContext("service", "operation"))
      Then("is should be a WritableBlobs")
      blobs should not be null
      blobs shouldBe a [WritableBlobs]
    }
    it("should return a no-op blobs if filter test fails") {
      Given("a blobs factory with a predicate")
      val store = mock[BlobStore]
      val factory = new BlobsFactory[SimpleBlobContext](store, (c :BlobContext) => false)
      When("a new blobs instance is requested")
      val blobs = factory.create(new SimpleBlobContext("service", "operation"))
      Then("is should be a NoOpBlobs")
      blobs should not be null
      blobs shouldBe a [NoOpBlobs]
    }
    it("should return a valid blobs if filter test succeeds") {
      Given("a blobs factory with a predicate")
      val store = mock[BlobStore]
      val factory = new BlobsFactory[SimpleBlobContext](store, (c :BlobContext) => true)
      When("a new blobs instance is requested")
      val blobs = factory.create(new SimpleBlobContext("service", "operation"))
      Then("is should be a WritableBlobs")
      blobs should not be null
      blobs shouldBe a [WritableBlobs]
    }
  }
}
