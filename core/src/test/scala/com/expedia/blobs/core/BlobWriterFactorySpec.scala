/*
 *
 *     Copyright 2018 Expedia, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 *
 */
package com.expedia.blobs.core

import org.scalatest.easymock.EasyMockSugar._
import org.scalatest.{BeforeAndAfter, FunSpec, GivenWhenThen, Matchers}

class BlobWriterFactorySpec extends FunSpec with GivenWhenThen with BeforeAndAfter with Matchers {
  describe("blobs creation") {
    it("should default blobs filter to true if one is not specified") {
      Given("a blobs factory")
      val store = mock[BlobStore]
      val factory = new BlobsFactory[SimpleBlobContext](store)
      When("a new BlobWriter instance is requested")
      val blobWriter = factory.create(new SimpleBlobContext("service", "operation"))
      Then("is should be a BlobWriterImpl")
      blobWriter should not be null
      blobWriter shouldBe a [BlobWriterImpl]
    }
    it("should return a no-op blobs if filter test fails") {
      Given("a blobs factory with a predicate that returns false")
      val store = mock[BlobStore]
      val factory = new BlobsFactory[SimpleBlobContext](store, (c :BlobContext) => false)
      When("a new blobWriter instance is requested")
      val blobWriter = factory.create(new SimpleBlobContext("service", "operation"))
      Then("is should be a valid BlobWriter")
      blobWriter should not be null
      And("an operation through blobWriter should not result in anything")
      blobWriter.write(BlobType.REQUEST, ContentType.JSON, o => { fail("should not be called") },
        m => { fail("should not be called") })
      Thread.sleep(100)
      blobWriter shouldBe a [NoOpBlobWriterImpl]
    }
    it("should return a valid blobs if filter test succeeds") {
      Given("a blobs factory with a predicate")
      val store = mock[BlobStore]
      val factory = new BlobsFactory[SimpleBlobContext](store, (c :BlobContext) => true)
      When("a new blobs instance is requested")
      val blobWriter = factory.create(new SimpleBlobContext("service", "operation"))
      Then("is should be a BlobWriterImpl")
      blobWriter should not be null
      blobWriter shouldBe a [BlobWriterImpl]
    }
  }
}
