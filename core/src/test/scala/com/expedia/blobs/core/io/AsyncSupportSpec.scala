package com.expedia.blobs.core.io

import java.io.IOException
import java.util.Optional
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.expedia.blobs.core._
import com.expedia.www.haystack.agent.blobs.grpc.Blob
import com.google.protobuf.ByteString
import org.scalatest.easymock.EasyMockSugar
import org.scalatest.{BeforeAndAfter, FunSpec, GivenWhenThen, Matchers}

import scala.collection.JavaConverters._

object Support {
  def newBlob(): Blob = Blob.newBuilder()
    .setKey("key1")
    .setContent(ByteString.copyFrom("""{"key":"value"}""".getBytes))
    .putAllMetadata(Map[String, String]("content-type" -> "application/json", "blob-type" -> "request", "a" -> "b", "c" -> "d").asJava)
    .build()
}

class InMemoryStore extends AsyncSupport(Runtime.getRuntime.availableProcessors(), 60) {

  private var blobs = List[Blob]()
  private var failBit = false

  override protected def storeInternal(blobBuilder: BlobWriterImpl.BlobBuilder): Unit = {
    val blob: Blob = blobBuilder.build()

    if (failBit) {
      throw new BlobReadWriteException("storage failure", new IOException())
    }

    Thread.sleep(5)
    blobs = blob :: blobs
  }

  override protected def readInternal(key: String): Optional[Blob] = {
    if (failBit) {
      throw new BlobReadWriteException("storage failure", new IOException())
    }

    Thread.sleep(5)
    blobs.find(b => b.getKey.equals(key)) match {
      case Some(value) => Optional.of(value)
      case _ => Optional.empty()
    }
  }

  def ++(blobBuilder: BlobWriterImpl.BlobBuilder): Unit = storeInternal(blobBuilder)

  def throwError(bool: Boolean): Unit = failBit = bool

  def size: Int = blobs.size
}

class AsyncSupportSpec extends FunSpec with GivenWhenThen with BeforeAndAfter with Matchers with EasyMockSugar {
  describe("async supported blob store") {
    var store: InMemoryStore = null
    before {
      store = new InMemoryStore
    }

    after {
      store.close()
    }

    it("should store a blob") {
      Given(" a simple blob")
      val blob = Support.newBlob()
      val blobBuilder = mock[BlobWriterImpl.BlobBuilder]

      expecting {
        blobBuilder.build().andReturn(blob)
      }
      When("it is stored using the given store")
      store.throwError(false)
      whenExecuting(blobBuilder) {
        store.store(blobBuilder)
        Then("it should successfully store it")
        Thread.sleep(10)
        store.size should equal(1)
      }
    }
    it("should fail to store a blob and exception is not propagated") {
      Given(" a simple blob")
      val blob = Support.newBlob()
      val blobBuilder = mock[BlobWriterImpl.BlobBuilder]
      When("it is stored using the given store")
      store.throwError(true)
      store.store(blobBuilder)
      Then("it should not successfully store it")
      Thread.sleep(10)
      store.size should equal(0)
    }
    it("should read a blob and call the callback") {
      Given(" a store with blob already in it")
      val blob = Support.newBlob()
      val blobBuilder = mock[BlobWriterImpl.BlobBuilder]

      expecting {
        blobBuilder.build().andReturn(blob)
      }
      whenExecuting(blobBuilder) {
        store ++ blobBuilder
        val blobRead = new AtomicBoolean(false)
        When("it is read from the store with a callback")
        store.read("key1", (t: Optional[Blob], e: Throwable) => {
          blobRead.set(t.isPresent)
        })
        Then("it should successfully read it")
        Thread.sleep(10)
        blobRead.get should be(true)
      }
    }
    it("should try to read a blob and returns empty on exception") {
      Given(" a store with blob already in it")
      val blob = Support.newBlob()
      val blobBuilder = mock[BlobWriterImpl.BlobBuilder]

      expecting {
        blobBuilder.build().andReturn(blob)
      }
      whenExecuting(blobBuilder) {
        store ++ blobBuilder
        store.throwError(true)
        When("it tries to read from the store")
        val readBlob = store.read("key5")
        Then("it should return empty blob")
        readBlob should equal(Optional.empty())
      }
    }
    it("should read a blob and return before a given timeout") {
      Given(" a store with blob already in it")
      val blob = Support.newBlob()
      val blobBuilder = mock[BlobWriterImpl.BlobBuilder]

      expecting {
        blobBuilder.build().andReturn(blob)
      }
      whenExecuting(blobBuilder) {
        store ++ blobBuilder
        When("it is read from the store with a timeout")
        val read = store.read("key1", 100, TimeUnit.MILLISECONDS)
        Then("it should successfully read it")
        read.get().getKey should equal("key1")
        read.get().getContent.toByteArray should equal("""{"key":"value"}""".getBytes)
        read.get().getMetadataMap.asScala should equal(Map[String, String]("content-type" -> "application/json", "blob-type" -> "request", "a" -> "b", "c" -> "d"))
      }
    }
    it("should return an empty object if timeout occurs before the read") {
      Given(" a store with blob already in it")
      val blob = Support.newBlob()
      val blobBuilder = mock[BlobWriterImpl.BlobBuilder]

      expecting {
        blobBuilder.build().andReturn(blob)
      }
      whenExecuting(blobBuilder) {
        store ++ blobBuilder
        When("it is read from the store with a timeout")
        val read = store.read("key1", 1, TimeUnit.MILLISECONDS)
        Then("it should return an empty object")
        read.isPresent should be(false)
      }
    }
  }
}


