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

import java.util
import java.util.Collections

import com.expedia.www.haystack.agent.blobs.dispatcher.core.{BlobDispatcher, RateLimitException}
import com.expedia.www.haystack.agent.blobs.grpc.Blob
import com.expedia.www.haystack.agent.blobs.grpc.api.DispatchResult
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver
import org.apache.commons.lang3.StringUtils
import org.easymock.EasyMock
import org.scalatest.easymock.EasyMockSugar
import org.scalatest.{FunSpec, Matchers}

class BlobAgentGrpcServerSpec extends FunSpec with Matchers with EasyMockSugar {
  private var metadata = new util.HashMap[String, String]()
  metadata.put("blob-type", "request")
  metadata.put("content-type", "text")
  metadata.put("test1", "This is a test meta")

  private val blob = Blob.newBuilder()
    .setKey("Key1")
    .setServiceName("client")
    .setBlobType(Blob.BlobType.REQUEST)
    .setContentType("text")
    .setContent(ByteString.copyFromUtf8(StringUtils.repeat("request-data", 5)))
    .putAllMetadata(metadata)
    .build()

  describe("blob agent server") {
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

    it("should fail during construction if no dispatcher exists") {
      val caught = intercept[Exception] {
        new BlobAgentGrpcServer(Collections.emptyList(), 1024*1024)
      }
      caught.getMessage shouldEqual "Dispatchers can't be empty"
    }
  }
}
