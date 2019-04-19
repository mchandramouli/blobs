package com.expedia.blobs.stores.aws

import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.transfer.{TransferManager, Upload}
import com.expedia.blobs.core.{Blob, SimpleBlob}
import org.scalatest.{BeforeAndAfter, FunSpec, GivenWhenThen, Matchers}
import org.scalatest.easymock.EasyMockSugar._
import org.easymock.EasyMock.anyObject
import org.easymock.EasyMock.{replay, verify}

import scala.collection.JavaConverters._

object Support {
  def newBlob(): Blob = new SimpleBlob("key1",
    Map[String, String]("a"->"b", "c" -> "d").asJava,
    """{"key":"value"}""".getBytes)
}

class S3BlobStoreSpec extends FunSpec with GivenWhenThen with BeforeAndAfter with Matchers {
  describe("a class that acts as a blob store backed by s3") {
    it("should require valid bucket name, transfer manager and threadpool size") {
      Given("valid and invalid values")
      val transferManager = mock[TransferManager]
      val poolSz = 1
      val bucketName = "blobs"
      When("an instance of s3 store is created")
      Then("it should fail for invalid values")
      intercept[IllegalArgumentException] {
        new S3BlobStore(null, transferManager, poolSz)
      }
      intercept[IllegalArgumentException] {
        new S3BlobStore(bucketName, null, poolSz)
      }
      intercept[IllegalArgumentException] {
        new S3BlobStore(bucketName, transferManager, 0)
      }
      And("create an instance if all arguments are valid")
      new S3BlobStore(bucketName, transferManager, poolSz) should not be null
    }
  }
  it("should create a put request and submit it to transfer manager") {
    Given("a blob and a valid s3 store")
    val transferManager = mock[TransferManager]
    val poolSz = 1
    val bucketName = "blobs"
    val store = new S3BlobStore(bucketName, transferManager, poolSz)
    val blob = Support.newBlob()
    val result = mock[Upload]
    expecting {
      transferManager.upload(anyObject[PutObjectRequest]).andReturn(result).times(1)
    }
    replay(transferManager)
    When("store is requested to store the blob")
    store.store(blob)
    Thread.sleep(10)
    Then("it should create a put request and send it to the transfer manager")
    verify(transferManager)
  }
}
