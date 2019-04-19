package com.expedia.blobs.stores.aws

import java.io.{ByteArrayInputStream, IOException, InputStream}
import java.util.Optional
import java.util.function.BiConsumer

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest, S3Object, S3ObjectInputStream}
import com.amazonaws.services.s3.transfer.{TransferManager, Upload}
import com.expedia.blobs.core.{Blob, BlobReadWriteException, SimpleBlob}
import org.apache.commons.io.IOUtils
import org.easymock.EasyMock
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

class ErrorHandlingS3BlobStore(bucket: String, trfManager: TransferManager) extends S3BlobStore(bucket, trfManager) {
  var error: Throwable = _
  override def store(b: Blob): Unit = {
    super.store(b, (t: Void, u: Throwable) => error = u)
  }
  def getError: Throwable = error
  def throwError(): Unit = error = new RuntimeException

  override protected def readInputStream(is: InputStream): Array[Byte] = {
    if (error != null)  throw error
    super.readInputStream(new ByteArrayInputStream("""{"key":"value"}""".getBytes))
  }
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
  it("should create a put request and with right bucket, key, metadata and content") {
    Given("a blob and a valid s3 store")
    val transferManager = mock[TransferManager]
    val poolSz = 1
    val bucketName = "blobs"
    val store = new S3BlobStore(bucketName, transferManager, poolSz)
    val blob = Support.newBlob()
    val result = mock[Upload]
    val captured = EasyMock.newCapture[PutObjectRequest]
    expecting {
      transferManager.upload(EasyMock.capture(captured)).andReturn(result).times(1)
    }
    replay(transferManager)
    When("store is requested to store the blob")
    store.store(blob)
    Thread.sleep(10)
    Then("it should create a put request and send it to the transfer manager")
    verify(transferManager)
    val putObjectRequest = captured.getValue
    putObjectRequest should not be null
    putObjectRequest.getMetadata.getContentLength should equal (blob.getSize)
    putObjectRequest.getMetadata.getUserMetadata.asScala should equal(Map[String, String]("a"->"b", "c" -> "d"))
    putObjectRequest.getBucketName should equal ("blobs")
    putObjectRequest.getKey should equal("key1")
    IOUtils.readFully(putObjectRequest.getInputStream, blob.getSize) should equal ("""{"key":"value"}""".getBytes)
  }
  it("should handle errors in store method and invoke the error handler") {
    Given("a blob and a valid s3 store")
    val transferManager = mock[TransferManager]
    val poolSz = 1
    val bucketName = "blobs"
    val store = new ErrorHandlingS3BlobStore(bucketName, transferManager)
    val blob = Support.newBlob()
    val error = new RuntimeException
    expecting {
      transferManager.upload(anyObject[PutObjectRequest]).andThrow(error).times(1)
    }
    replay(transferManager)
    When("store is requested to store the blob and the transfer manager fails")
    store.store(blob)
    Thread.sleep(10)
    Then("it should handle the error, wrap and send it to error handler")
    verify(transferManager)
    store.getError.getCause shouldBe a [BlobReadWriteException]
  }
  it("should read a blob from s3 and return the data as expected") {
    Given("a blob store and a mock transfer manager")
    val transferManager = mock[TransferManager]
    val store = new ErrorHandlingS3BlobStore("blobs", transferManager)
    val s3Client = mock[AmazonS3Client]
    val s3Object = mock[S3Object]
    val objectMetadata = mock[ObjectMetadata]
    val userMetadata = Map[String, String]("a" -> "b").asJava
    val s3InputStream = mock[S3ObjectInputStream]
    expecting {
      transferManager.getAmazonS3Client.andReturn(s3Client).times(1)
      s3Client.getObject("blobs", "key1").andReturn(s3Object).once()
      s3Object.getObjectMetadata.andReturn(objectMetadata).once()
      objectMetadata.getUserMetadata.andReturn(userMetadata).once()
      s3Object.getObjectContent.andReturn(s3InputStream).once()
    }
    replay(transferManager, s3Client, s3Object, objectMetadata)
    When("a blob is read from s3")
    val optionalBlob = store.read("key1")
    Then("it should read the blob by key from s3 bucket")
    optionalBlob.isPresent should be (true)
    val blob = optionalBlob.get()
    And("blob data should be as expected")
    blob.getKey should equal ("key1")
    blob.getMetadata.asScala should equal (Map[String, String]("a" -> "b"))
    blob.getData should equal ("""{"key":"value"}""".getBytes)
  }
  it("should handle any exception at read and return empty object") {
    Given("a blob store that fails on read")
    val transferManager = mock[TransferManager]
    val store = new ErrorHandlingS3BlobStore("blobs", transferManager)
    store.throwError()
    var error : Throwable = null
    When("read is called with a key")
    store.read("key1", (v: Optional[Blob], t: Throwable) => error = t)
    Then("it should handle the error as expected")
    Thread.sleep(10)
    error shouldBe a [RuntimeException]
  }
}
