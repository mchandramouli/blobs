package com.expedia.blobs.core.io

import java.io.IOException
import java.util.Optional
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.expedia.blobs.core.{Blob, BlobReadWriteException, SimpleBlob}
import org.scalatest.{BeforeAndAfter, FunSpec, GivenWhenThen, Matchers}

import scala.collection.JavaConverters._

object Support {
  def newBlob(): Blob = new SimpleBlob("key1",
    Map[String, String]("a"->"b", "c" -> "d").asJava,
    """{"key":"value"}""".getBytes)
}

class InMemoryStore extends AsyncSupport(Runtime.getRuntime.availableProcessors(), 60) {
  
  private var blobs = List[Blob]()
  private var failBit = false

  override protected def storeInternal(blob: Blob): Unit = {
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

  def ++(blob: Blob): Unit = storeInternal(blob)

  def throwError(bool: Boolean): Unit = failBit = bool

  def size: Int = blobs.size
}

class AsyncSupportSpec extends FunSpec with GivenWhenThen with BeforeAndAfter with Matchers {
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
      When("it is stored using the given store")
      store.throwError(false)
      store.store(blob)
      Then("it should successfully store it")
      Thread.sleep(10)
      store.size should equal(1)
    }
    it("should fail to store a blob and exception is not propagated") {
      Given(" a simple blob")
      val blob = Support.newBlob()
      When("it is stored using the given store")
      store.throwError(true)
      store.store(blob)
      Then("it should not successfully store it")
      Thread.sleep(10)
      store.size should equal(0)
    }
    it("should read a blob and call the callback") {
      Given(" a store with blob already in it")
      val blob = Support.newBlob()
      store ++ blob
      val blobRead = new AtomicBoolean(false)
      When("it is read from the store with a callback")
      store.read("key1", (t: Optional[Blob], e: Throwable) => {
        blobRead.set(t.isPresent)
      })
      Then("it should successfully read it")
      Thread.sleep(10)
      blobRead.get should be(true)
    }
    it("should try to read a blob and returns empty on exception") {
      Given(" a store with blob already in it")
      val blob = Support.newBlob()
      store ++ blob
      store.throwError(true)
      When("it tries to read from the store")
      val readBlob = store.read("key5")
      Then("it should return empty blob")
      readBlob should equal(Optional.empty())
    }
    it("should read a blob and return before a given timeout") {
      Given(" a store with blob already in it")
      val blob = Support.newBlob()
      store ++ blob
      When("it is read from the store with a timeout")
      val read = store.read("key1", 100, TimeUnit.MILLISECONDS)
      Then("it should successfully read it")
      read.get().getKey should equal("key1")
      read.get().getData should equal("""{"key":"value"}""".getBytes)
      read.get().getMetadata.asScala should equal(Map[String, String]("a"->"b", "c"->"d"))
    }
    it("should return an empty object if timeout occurs before the read") {
      Given(" a store with blob already in it")
      val blob = Support.newBlob()
      store ++ blob
      When("it is read from the store with a timeout")
      val read = store.read("key1", 1, TimeUnit.MILLISECONDS)
      Then("it should return an empty object")
      read.isPresent should be(false)
    }
  }
}


