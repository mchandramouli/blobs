/*
 *  Copyright 2019 Expedia, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.expedia.www.haystack.agent.blobs.server.api

import java.util.{Collections, Optional}

import com.expedia.blobs.core.ContentType
import com.expedia.www.blobs.model.Blob
import com.expedia.www.haystack.agent.blobs.api.{BlobReadResponse, BlobSearch, DispatchResult, FormattedBlobReadResponse}
import com.expedia.www.haystack.agent.blobs.dispatcher.core.{BlobDispatcher, RateLimitException}
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.easymock.EasyMock
import org.scalatest.easymock.EasyMockSugar
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}

import scala.collection.JavaConverters._

class BlobAgentGrpcServerSpec extends FunSpec with BeforeAndAfter with Matchers with EasyMockSugar {
  private val metadata = Map[String, String]("blob-type" -> "request", "content-type" -> "text", "test1" -> "This is a test meta").asJava
  private val xmlResourceInBytes = IOUtils.toByteArray(getClass.getClassLoader.getResourceAsStream("xmlData.xml"))
  private val fastinfosetResourceInBytes = IOUtils.toByteArray(getClass.getClassLoader.getResourceAsStream("fastinfosetData.xml"))
  private val parsedFastinfosetResourceInBytes = IOUtils.toByteArray(getClass.getClassLoader.getResourceAsStream("fastinfosetParsedData.xml"))

  private val blob = Blob.newBuilder()
    .setKey("Key1")
    .setContent(ByteString.copyFromUtf8(StringUtils.repeat("request-data", 5)))
    .putAllMetadata(metadata)
    .build()

  describe("blob agent server") {

    before {

    }

    it("should fail during construction if no dispatcher exists") {
      val caught = intercept[Exception] {
        new BlobAgentGrpcServer(Collections.emptyList(), 1024 * 1024)
      }
      caught.getMessage shouldEqual "Dispatchers can't be empty"
    }

    it("should send the record to dispatcher with success response") {
      val dispatcher = mock[BlobDispatcher]
      val observer = mock[StreamObserver[DispatchResult]]
      val blobAgentGrpcServer = new BlobAgentGrpcServer(Collections.singletonList(dispatcher), 1024 * 1024)

      val dispatchResult = EasyMock.newCapture[DispatchResult]()

      expecting {
        dispatcher.dispatch(blob)
        observer.onNext(EasyMock.capture(dispatchResult))
        observer.onCompleted()
      }

      whenExecuting(observer) {
        blobAgentGrpcServer.dispatch(blob, observer)
        dispatchResult.getValue.getCode shouldEqual DispatchResult.ResultCode.SUCCESS
        dispatchResult.getValue.getErrorMessage shouldEqual ""
      }
    }

    it("should throw back error if the bloc size exceeds the limit") {
      val dispatcher = mock[BlobDispatcher]
      val observer = mock[StreamObserver[DispatchResult]]
      val blobAgentGrpcServer = new BlobAgentGrpcServer(Collections.singletonList(dispatcher), 50)

      val dispatchResult = EasyMock.newCapture[DispatchResult]()

      expecting {
        observer.onNext(EasyMock.capture(dispatchResult))
        observer.onCompleted()
      }

      whenExecuting(observer) {
        blobAgentGrpcServer.dispatch(blob, observer)
        dispatchResult.getValue.getCode shouldEqual DispatchResult.ResultCode.MAX_SIZE_EXCEEDED_ERROR
        dispatchResult.getValue.getErrorMessage shouldEqual "Fail to dispatch as the blob size=60 exceeds the limit of 50 bytes"
      }
    }

    it("should dispatch the blob with rate limit error if dispatcher throws RateLimitException") {
      val dispatcher = mock[BlobDispatcher]
      val observer = mock[StreamObserver[DispatchResult]]
      val blobAgentGrpcServer = new BlobAgentGrpcServer(Collections.singletonList(dispatcher), 1024 * 1024)

      val dispatchResult = EasyMock.newCapture[DispatchResult]()

      expecting {
        dispatcher.dispatch(blob).andThrow(new RateLimitException("Rate Limit Error!"))
        dispatcher.getName.andReturn("test-dispatcher")
        observer.onNext(EasyMock.capture(dispatchResult))
        observer.onCompleted()
      }

      whenExecuting(observer, dispatcher) {
        blobAgentGrpcServer.dispatch(blob, observer)
        dispatchResult.getValue.getCode shouldEqual DispatchResult.ResultCode.RATE_LIMIT_ERROR
        dispatchResult.getValue.getErrorMessage shouldEqual "Fail to dispatch the blob to the dispatchers=test-dispatcher"
      }
    }

    it("should return correct BlobReadResponse on successful read") {
      val dispatcher = mock[BlobDispatcher]
      val observer = mock[StreamObserver[BlobReadResponse]]
      val blobAgentGrpcServer = new BlobAgentGrpcServer(Collections.singletonList(dispatcher), 1024 * 1024)
      val blobSearch: BlobSearch = BlobSearch.newBuilder().setKey("key1").build()
      val result = EasyMock.newCapture[BlobReadResponse]

      expecting {
        dispatcher.read("key1").andReturn(Optional.of(blob))
        observer.onNext(EasyMock.capture(result)).once()
        observer.onCompleted().once()
      }

      whenExecuting(dispatcher, observer) {
        blobAgentGrpcServer.read(blobSearch, observer)
        result.getValue.getBlob shouldEqual blob
        result.getValue.getCode shouldEqual BlobReadResponse.ResultCode.SUCCESS
      }
    }

    it("should return correct BlobReadResponse on unsuccessful read") {
      val dispatcher = mock[BlobDispatcher]
      val observer = mock[StreamObserver[BlobReadResponse]]
      val blobAgentGrpcServer = new BlobAgentGrpcServer(Collections.singletonList(dispatcher), 1024 * 1024)
      val blobSearch: BlobSearch = BlobSearch.newBuilder().setKey("key1").build()
      val result = EasyMock.newCapture[BlobReadResponse]

      expecting {
        dispatcher.read("key1").andReturn(Optional.empty()).anyTimes()
        dispatcher.getName.andReturn("s3").anyTimes()
        observer.onNext(EasyMock.capture(result)).once()
        observer.onCompleted().once()
      }

      whenExecuting(dispatcher, observer) {
        blobAgentGrpcServer.read(blobSearch, observer)
        result.getValue.getCode shouldEqual BlobReadResponse.ResultCode.UNKNOWN_ERROR
        result.getValue.getErrorMessage shouldEqual "Failed to read blob with key key1 from [s3]"
      }
    }

    it("should return correct xml string on successful readBlobAsString call") {
      val dispatcher = mock[BlobDispatcher]
      val observer = mock[StreamObserver[FormattedBlobReadResponse]]
      val blobAgentGrpcServer = new BlobAgentGrpcServer(Collections.singletonList(dispatcher), 1024 * 1024)
      val blobSearch: BlobSearch = BlobSearch.newBuilder().setKey("key1").build()
      val result = EasyMock.newCapture[FormattedBlobReadResponse]
      val actualBlob = Blob.newBuilder()
        .setKey("Key1")
        .setContent(ByteString.copyFrom(xmlResourceInBytes))
        .putAllMetadata(Map[String, String]("content-type" -> "application/xml").asJava)
        .build()
      val expectedBlobString = IOUtils.toString(xmlResourceInBytes)

      Thread.sleep(50)

      expecting {
        dispatcher.read("key1").andReturn(Optional.of(actualBlob))
        observer.onNext(EasyMock.capture(result)).once()
        observer.onCompleted().once()
      }

      whenExecuting(dispatcher, observer) {
        blobAgentGrpcServer.readBlobAsString(blobSearch, observer)
        result.getValue.getData shouldEqual expectedBlobString
      }
    }

    it("should return correct xml string (for fastinfoset blob) on successful readBlobAsString call") {
      val dispatcher = mock[BlobDispatcher]
      val observer = mock[StreamObserver[FormattedBlobReadResponse]]
      val blobAgentGrpcServer = new BlobAgentGrpcServer(Collections.singletonList(dispatcher), 1024 * 1024)
      val blobSearch: BlobSearch = BlobSearch.newBuilder().setKey("key1").build()
      val result = EasyMock.newCapture[FormattedBlobReadResponse]
      val actualBlob = Blob.newBuilder()
        .setKey("Key1")
        .setContent(ByteString.copyFrom(fastinfosetResourceInBytes))
        .putAllMetadata(Map[String, String]("content-type" -> ContentType.FAST_INFOSET.getType).asJava)
        .build()
      val expectedBlobString = IOUtils.toString(parsedFastinfosetResourceInBytes)

      Thread.sleep(50)

      expecting {
        dispatcher.read("key1").andReturn(Optional.of(actualBlob))
        observer.onNext(EasyMock.capture(result)).once()
        observer.onCompleted().once()
      }

      whenExecuting(dispatcher, observer) {
        blobAgentGrpcServer.readBlobAsString(blobSearch, observer)
        result.getValue.getData shouldEqual expectedBlobString
      }
    }
  }
}
