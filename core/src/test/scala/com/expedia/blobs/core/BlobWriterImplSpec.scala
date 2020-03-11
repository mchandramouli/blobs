package com.expedia.blobs.core

import java.io.{IOException, OutputStream}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import org.easymock.EasyMock
import org.scalatest.easymock.EasyMockSugar._
import org.scalatest.{BeforeAndAfter, FunSpec, GivenWhenThen, Matchers}

import scala.collection.JavaConverters._

class BlobWriterImplSpec extends FunSpec with GivenWhenThen with BeforeAndAfter with Matchers {
  describe("writable blobs") {
    it("should call store to write the blob with a made up key") {
      Given("a mock store, blob context and a writable blobs")
      val store = mock[BlobStore]
      val context = new SimpleBlobContext("service1", "operation1")
      val writableBlobs = new BlobWriterImpl(context, store)
      val captured = EasyMock.newCapture[BlobWriterImpl.BlobBuilder]
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
      """service1_operation1_.*_request""".r.pattern.matcher(capturedBlob.build.getKey).matches() should be (true)
    }
    it("should call store to with a blob object that invokes the callback to serialize blob only " +
      "the first time when data is fetched") {
      Given("a mock store, blob context and a writable blobs")
      val store = mock[BlobStore]
      val context = new SimpleBlobContext("service1", "operation1")
      val writableBlobs = new BlobWriterImpl(context, store)
      val captured = EasyMock.newCapture[BlobWriterImpl.BlobBuilder]
      val blobToWrite = "{}"
      val blobWritten = new AtomicBoolean(false)
      val blobWrittenCount = new AtomicInteger(0)
      expecting {
        store.store(EasyMock.capture(captured))
      }
      EasyMock.replay(store)
      When("a call is made to store a blob")
      writableBlobs.write(BlobType.REQUEST, ContentType.JSON, (o: OutputStream) => {
        blobWritten.set(true)
        blobWrittenCount.incrementAndGet()
        o.write(blobToWrite.getBytes)
      }, (m: Metadata) => {})
      Then("store is called with a blob object")
      captured.hasCaptured should be (true)
      And ("blob is not yet written to that object")
      blobWritten.get() should be (false)
      And("blob is written only when getData is called")
      val capturedBlob = captured.getValue
      val data = capturedBlob.build.getContent.toByteArray
      blobWritten.get should be (true)
      blobWrittenCount.get should be (1)
      data should be (blobToWrite.getBytes)
      And("calling getData the second time does not call the callback")
      capturedBlob.build.getContent.toByteArray
      blobWrittenCount.get should be (1)
    }
    it("should call store to with a blob object that invokes the callback to add metadata only " +
      "the first time when metadata is read") {
      Given("a mock store, blob context and a writable blobs")
      val store = mock[BlobStore]
      val context = new SimpleBlobContext("service1", "operation1")
      val writableBlobs = new BlobWriterImpl(context, store)
      val captured = EasyMock.newCapture[BlobWriterImpl.BlobBuilder]
      val blobToWrite = "{}"
      val metaDataWritten = new AtomicBoolean(false)
      val metaDataWriteCount = new AtomicInteger(0)
      expecting {
        store.store(EasyMock.capture(captured))
      }
      EasyMock.replay(store)
      When("a call is made to store a blob")
      writableBlobs.write(BlobType.REQUEST, ContentType.JSON, (o: OutputStream) => {
        o.write(blobToWrite.getBytes)
      }, (m: Metadata) => {
        metaDataWritten.set(true)
        metaDataWriteCount.incrementAndGet()
        m.add("key", "value")
      })
      Then("store is called with a blob object")
      captured.hasCaptured should be (true)
      And ("metadata is not yet written to that object")
      metaDataWritten.get() should be (false)
      And("metadata is written only when getMetadata is called")
      val capturedBlob = captured.getValue
      val data = capturedBlob.build.getMetadataMap
      metaDataWritten.get should be (true)
      metaDataWriteCount.get() should be (1)
      data.asScala should equal (Map(
        "blob-type" -> "request",
        "content-type" -> ContentType.JSON.getType,
        "key" -> "value"))
      And("calling getMetadata the second time does not call the callback")
      capturedBlob.build.getMetadataMap
      metaDataWriteCount.get should be (1)
    }
    it("should call the blob store to with a blob object and getData should throw an exception if callback " +
      "throws an exception") {
      Given("a mock store, blob context and a writable blobs")
      val store = mock[BlobStore]
      val context = new SimpleBlobContext("service1", "operation1")
      val writableBlobs = new BlobWriterImpl(context, store)
      val captured = EasyMock.newCapture[BlobWriterImpl.BlobBuilder]
      expecting {
        store.store(EasyMock.capture(captured))
      }
      EasyMock.replay(store)

      When("a call is made to store a blob with a callback that fails")
      writableBlobs.write(BlobType.REQUEST, ContentType.JSON, (o: OutputStream) => {
        throw new IOException("something went wrong")
      }, (m: Metadata) => {})
      Then("store is called with a blob object")
      captured.hasCaptured should be (true)
      And("getData throws an exception if the callback throws an exception")
      intercept[BlobReadWriteException] {
        captured.getValue.build.getContent
      }
    }
    it("should call the given blob store to with a blob object that provides the size of the blob correctly") {
      Given("a mock store, blob context and a writable blobs")
      val store = mock[BlobStore]
      val context = new SimpleBlobContext("service1", "operation1")
      val writableBlobs = new BlobWriterImpl(context, store)
      val captured = EasyMock.newCapture[BlobWriterImpl.BlobBuilder]
      val blobToWrite = "{}"
      val blobWritten = new AtomicBoolean(false)
      val blobWrittenCount = new AtomicInteger(0)
      expecting {
        store.store(EasyMock.capture(captured))
      }
      EasyMock.replay(store)
      When("a call is made to store a blob")
      writableBlobs.write(BlobType.REQUEST, ContentType.JSON, (o: OutputStream) => {
        blobWritten.set(true)
        blobWrittenCount.incrementAndGet()
        o.write(blobToWrite.getBytes)
      }, (m: Metadata) => {})
      Then("store is called with a blob object")
      captured.hasCaptured should be (true)
      And ("and the size of the blob object is correct")
      captured.getValue.build.getContent.size() should equal (2)
    }
  }
}
