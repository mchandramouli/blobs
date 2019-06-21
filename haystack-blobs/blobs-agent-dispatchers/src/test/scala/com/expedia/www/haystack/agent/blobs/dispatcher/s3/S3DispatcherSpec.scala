package com.expedia.www.haystack.agent.blobs.dispatcher.s3

import com.amazonaws.auth.{AWSStaticCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.transfer.{TransferManager, Upload}
import com.expedia.www.haystack.agent.blobs.dispatcher.core.RateLimitException
import com.expedia.www.haystack.agent.blobs.grpc.Blob
import com.google.protobuf.ByteString
import com.typesafe.config.ConfigFactory
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.easymock.EasyMock
import org.scalatest.easymock.EasyMockSugar
import org.scalatest.{BeforeAndAfter, FunSpec, GivenWhenThen, Matchers}

import scala.collection.JavaConverters._

class S3DispatcherSpec extends FunSpec with GivenWhenThen with BeforeAndAfter with Matchers with EasyMockSugar {
  private val metadata = Map[String, String]("content-type" -> "application/json", "blob-type" -> "request", "a" -> "b", "c" -> "d").asJava
  private val blobKey = "key1"
  private val blob = Blob.newBuilder()
    .setKey(blobKey)
    .setServiceName("client")
    .setBlobType(Blob.BlobType.REQUEST)
    .setContentType("text")
    .setContent(ByteString.copyFromUtf8(StringUtils.repeat("request-data", 5)))
    .putAllMetadata(metadata)
    .build()

  describe("S3 Dispatcher") {

    it("should return it's name as 's3'") {
      val transferManager = mock[TransferManager]
      val dispatcher = new S3Dispatcher(transferManager, "haystack", false, 50)

      When("getName method of dispatcher is called")
      Then("It should return 's3' as name")
      dispatcher.getName shouldEqual "s3"
    }

    it("should check if bucket name is present while initializing the dispatcher") {
      val config = ConfigFactory.parseString(
        """
          |bucketName = ""
          |maxOutstandingRequests = 50
          |shouldWaitForUpload = false
          |awsAccessKey = "my-access-key"
          |awsSecretKey = "my-secret-key"
        """.stripMargin)

      val dispatcher = new S3Dispatcher()

      val exception = intercept[Exception] {
        dispatcher.initialize(config)
      }

      exception.getMessage shouldEqual "s3 bucket name can't be empty"
    }

    it("should check if maxOutstandingRequests is positive while initializing the dispatcher") {
      val config = ConfigFactory.parseString(
        """
          |bucketName = "haystack"
          |maxOutstandingRequests = 0
          |shouldWaitForUpload = false
          |awsAccessKey = "my-access-key"
          |awsSecretKey = "my-secret-key"
        """.stripMargin)

      val dispatcher = new S3Dispatcher()

      val exception = intercept[Exception] {
        dispatcher.initialize(config)
      }

      exception.getMessage shouldEqual "max parallel uploads has to be greater than 0"
    }

    it("should check if regionName is present while initializing the dispatcher") {
      val config = ConfigFactory.parseString(
        """
          |bucketName = "haystack"
          |maxOutstandingRequests = 50
          |shouldWaitForUpload = false
          |awsAccessKey = "my-access-key"
          |awsSecretKey = "my-secret-key"
          |region = ""
        """.stripMargin)

      val dispatcher = new S3Dispatcher()

      val exception = intercept[Exception] {
        dispatcher.initialize(config)
      }

      exception.getMessage shouldEqual "s3 bucket region can't be empty"
    }

    it("should dispatch the blob") {
      val transferManager = mock[TransferManager]
      val upload = mock[Upload]
      val dispatcher = new S3Dispatcher(transferManager, "haystack", false, 50)

      val putRequest = EasyMock.newCapture[PutObjectRequest]

      expecting {
        transferManager.upload(EasyMock.capture(putRequest)).andReturn(upload)
      }

      whenExecuting(transferManager) {
        dispatcher.dispatch(blob)
        val requestObject = putRequest.getValue
        requestObject.getBucketName shouldEqual "haystack"
        requestObject.getKey shouldEqual blobKey
        requestObject.getMetadata.getUserMetadata shouldEqual metadata
        IOUtils.readLines(requestObject.getInputStream).get(0) shouldEqual StringUtils.repeat("request-data", 5)
      }
    }

    it("should throw an error in dispatchInternal while uploading blob") {
      val transferManager = mock[TransferManager]
      val upload = mock[Upload]
      val dispatcher = new S3Dispatcher(transferManager, "haystack", false, 50)

      expecting {
        transferManager.upload(EasyMock.anyObject(classOf[PutObjectRequest])).andThrow(new RuntimeException("some error"))
      }

      whenExecuting(transferManager, upload) {
        val caught = intercept[Exception] {
          dispatcher.dispatchInternal(blob)
        }
        caught should not be null
        caught.getMessage shouldEqual "Unable to upload blob to S3 for  key key1 : some error"
      }
    }

    it("should throw rate limit exceeded error") {
      val transferManager = mock[TransferManager]
      val dispatcher = new S3Dispatcher(transferManager, "haystack", false, 0)

      val caught = intercept[RateLimitException] {
        dispatcher.dispatch(blob)
      }
      caught should not be null
      caught.getMessage should include("RateLimit is hit with outstanding(pending) requests=0")
    }

    it("should build the credential provider using access and secret key") {
      val config = ConfigFactory.parseString(
        """
          |bucketName = "haystack"
          |maxOutstandingRequests = 50
          |shouldWaitForUpload = false
          |awsAccessKey = "my-access-key"
          |awsSecretKey = "my-secret-key"
        """.stripMargin)

      val provider = S3Dispatcher.buildCredentialProvider(config)
      provider.isInstanceOf[AWSStaticCredentialsProvider] shouldBe true
      val creds = provider.asInstanceOf[AWSStaticCredentialsProvider].getCredentials
      creds.getAWSSecretKey shouldEqual "my-secret-key"
      creds.getAWSAccessKeyId shouldEqual "my-access-key"
    }

    it("should build the credential provider using default credential provider") {
      val config = ConfigFactory.parseString(
        """
          |bucketName = "haystack"
          |maxOutstandingRequests = 50
        """.stripMargin)

      val provider = S3Dispatcher.buildCredentialProvider(config)
      provider.isInstanceOf[DefaultAWSCredentialsProviderChain] shouldBe true
    }
  }
}
