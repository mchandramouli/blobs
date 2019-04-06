package com.expedia.blobs.core

import java.io.OutputStream
import java.util.concurrent.atomic.AtomicBoolean

import org.easymock.EasyMock
import org.scalatest.easymock.EasyMockSugar._
import org.scalatest.{BeforeAndAfter, FunSpec, GivenWhenThen, Matchers}
import scala.collection.JavaConverters._

class WritableBlobsSpec extends FunSpec with GivenWhenThen with BeforeAndAfter with Matchers {
  describe("writable blobs") {
    it("should call store to write the blob with a made up key") {
      Given("a mock store, blob context and a writable blobs")
      val store = mock[BlobStore]
      val context = new SimpleBlobContext("service1", "operation1")
      val writableBlobs = new WritableBlobs(context, store)
      val captured = EasyMock.newCapture[Blob]
      val blobToWrite = "{}".getBytes()
      expecting {
        store.store(EasyMock.capture(captured))
      }
      EasyMock.replay(store)
      When("a call is made to store a blob")
      writableBlobs.write(BlobType.REQUEST, ContentType.JSON, (o: OutputStream) => o.write(blobToWrite), (m: Metadata) => {})
      Then("store is called with a blob")
      captured.hasCaptured should be (true)
      And("blob's key is generated as expected")
      val capturedBlob = captured.getValue
      """service1/operation1/.*/request-.*""".r.pattern.matcher(capturedBlob.getKey).matches() should be (true)
    }
    it("should call store to with a blob object that invokes the lambda to serialize blob only when data is fetched") {
      Given("a mock store, blob context and a writable blobs")
      val store = mock[BlobStore]
      val context = new SimpleBlobContext("service1", "operation1")
      val writableBlobs = new WritableBlobs(context, store)
      val captured = EasyMock.newCapture[Blob]
      val blobToWrite = "{}"
      val blobWritten = new AtomicBoolean(false)
      expecting {
        store.store(EasyMock.capture(captured))
      }
      EasyMock.replay(store)
      When("a call is made to store a blob")
      writableBlobs.write(BlobType.REQUEST, ContentType.JSON, (o: OutputStream) => {
        blobWritten.set(true)
        o.write(blobToWrite.getBytes)
      }, (m: Metadata) => {})
      Then("store is called with a blob object")
      captured.hasCaptured should be (true)
      And ("blob is not yet written to that object")
      blobWritten.get() should be (false)
      And("blob is written only when getData is called")
      val capturedBlob = captured.getValue
      val data = capturedBlob.getData
      blobWritten.get should be (true)
      data should be (blobToWrite.getBytes)
    }
    it("should call store to with a blob object that invokes the lambda to add metadata only when metadata is read") {
      Given("a mock store, blob context and a writable blobs")
      val store = mock[BlobStore]
      val context = new SimpleBlobContext("service1", "operation1")
      val writableBlobs = new WritableBlobs(context, store)
      val captured = EasyMock.newCapture[Blob]
      val blobToWrite = "{}"
      val metaDataWritten = new AtomicBoolean(false)
      expecting {
        store.store(EasyMock.capture(captured))
      }
      EasyMock.replay(store)
      When("a call is made to store a blob")
      writableBlobs.write(BlobType.REQUEST, ContentType.JSON, (o: OutputStream) => {
        o.write(blobToWrite.getBytes)
      }, (m: Metadata) => {
        metaDataWritten.set(true)
        m.add("key", "value")
      })
      Then("store is called with a blob object")
      captured.hasCaptured should be (true)
      And ("metadata is not yet written to that object")
      metaDataWritten.get() should be (false)
      And("metadata is written only when getMetadata is called")
      val capturedBlob = captured.getValue
      val data = capturedBlob.getMetadata
      metaDataWritten.get should be (true)
      data.asScala should equal (Map(
        "blob-type" -> "request",
        "content-type" -> ContentType.JSON.getType,
        "key" -> "value"))
    }
  }
}
