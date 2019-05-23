package com.expedia.blobs.core.io

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.{BiConsumer, Supplier}

import org.scalatest.{BeforeAndAfter, FunSpec, GivenWhenThen, Matchers}

class ManagedAsyncOperationSpec extends FunSpec with GivenWhenThen with BeforeAndAfter with Matchers {
  describe("a class that manages async operations") {
    var asyncOperation: ManagedAsyncOperation = null
    before {
      asyncOperation = new ManagedAsyncOperation(1, 60)
    }

    after {
      asyncOperation.close()
    }

    it("should execute a runnable and call the callback when complete") {
      Given("an slow runnable and a callback")
      val executed = new AtomicBoolean(false)
      val operation: Runnable = () => {
        Thread.sleep(10)
        executed.set(true)
      }
      val callback: BiConsumer[Void, Throwable] = (v, t) => {}
      When("a runnable is executed asynchronously")
      asyncOperation.execute(operation, callback)
      Then("it should call back when completed")
      Thread.sleep(50)
      executed.get should be(true)
    }
    it("should execute a runnable and call the callback when it completes with an error") {
      Given("a slow runnable that fails and a callback")
      val executed = new AtomicBoolean(false)
      val operation: Runnable = () => {
        Thread.sleep(10)
        throw new RuntimeException("some error")
      }
      val callback: BiConsumer[Void, Throwable] = (v, t) => {
        if (t.isInstanceOf[RuntimeException]) {
          executed.set(true)
        }
      }
      When("a runnable is executed asynchronously")
      asyncOperation.execute(operation, callback)
      Then("it should call the callback with the exception when completed")
      Thread.sleep(50)
      executed.get should be(true)
    }

    it("should execute an operation and call the callback when complete with the returned object") {
      Given("a slow operation and a callback")
      val executed = new AtomicBoolean(false)
      val operation: Supplier[Long] = () => {
        Thread.sleep(10)
        100
      }
      val callback: BiConsumer[Long, Throwable] = (v, t) => {
        if (v == 100) {
          executed.set(true)
        }
      }
      When("an operation is executed asynchronously")
      asyncOperation.execute(operation, callback)
      Then("it should call back when completed")
      Thread.sleep(50)
      executed.get should be(true)
    }
    it("should execute an operation and call the callback when it completes with an error") {
      Given("a slow operation that fails and a callback")
      val executed = new AtomicBoolean(false)
      val operation: Supplier[Long] = () => {
        Thread.sleep(10)
        throw new RuntimeException("some error")
      }
      val callback: BiConsumer[Long, Throwable] = (v, t) => {
        if (t.isInstanceOf[RuntimeException]) {
          executed.set(true)
        }
      }
      When("an operation is executed asynchronously")
      asyncOperation.execute(operation, callback)
      Then("it should call the callback with the exception when completed")
      Thread.sleep(50)
      executed.get should be(true)
    }

    it("should execute an operation and return the response if timeout has not elapsed") {
      Given("a slow operation")
      val operation: Supplier[Long] = () => {
        Thread.sleep(10)
        100
      }
      val defaultValue : Long = -1
      When("an operation is executed asynchronously with a longer timeout")
      val response : Long = asyncOperation.execute(operation, defaultValue, 50, TimeUnit.MILLISECONDS)
      Then("it should return the exoected response without timing out")
      response should equal(100)
    }

    it("should execute an operation and return the default response if the time has elapsed") {
      Given("a slow operation")
      val operation: Supplier[Long] = () => {
        Thread.sleep(50)
        100
      }
      val defaultValue : Long = -1
      When("an operation is executed asynchronously with a longer timeout")
      val response : Long = asyncOperation.execute(operation, defaultValue, 10, TimeUnit.MILLISECONDS)
      Then("it should return the exoected response without timing out")
      response should equal(-1)
    }

    it("should force shutdown thread pool if wait times out") {
      Given("a slow runnable, a callback and an instance of ManagedAsyncOperation with 1 sec wait timeout")
      val executed = new AtomicBoolean(false)
      val operation: Runnable = () => {
        Thread.sleep(10000)
        executed.set(true)
      }
      val callback: BiConsumer[Void, Throwable] = (v, t) => {}
      val asyncOp = new ManagedAsyncOperation(1, 1)
      When("a runnable is executed asynchronously")
      asyncOp.execute(operation, callback)
      And("when ManagedAsyncOperation instanced is closed")
      asyncOp.close()
      Then("it should close before the operation has completed")
      executed.get should be(false)
    }
  }
}
