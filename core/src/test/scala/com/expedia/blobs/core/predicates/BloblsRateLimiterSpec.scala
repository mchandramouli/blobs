package com.expedia.blobs.core.predicates

import java.util.concurrent.TimeUnit

import com.expedia.blobs.core.{BlobContext, SimpleBlobContext}
import com.revinate.guava.base.Stopwatch
import org.scalatest.{BeforeAndAfter, FunSpec, GivenWhenThen, Matchers}

class BloblsRateLimiterSpec extends FunSpec with GivenWhenThen with BeforeAndAfter with Matchers {
  describe("rate limiter") {
    it("should limit after specified rate is reached") {
      Given("a rate limiter with a limit of 2")
      val rateLimiter = new BlobsRateLimiter[BlobContext](2.0)
      val context = new SimpleBlobContext("service1", "operation1")
      var counter = 0
      val stopwatch = Stopwatch.createUnstarted
      When("rate limit check is invoked multiple times")
      stopwatch.start()
      for (_ <- 1 to 200) {
        if (rateLimiter.test(context))
          counter += 1
        Thread.sleep(10)
      }
      stopwatch.stop()
      Then("average check succeeded per second should be no more than 2.0")
      val elapsed = stopwatch.elapsed(TimeUnit.SECONDS)
      val ratePerSecond = counter / elapsed
      (ratePerSecond <= 2.0) should be (true)
    }
    it("rate limit of less than 1 per second always returns false") {
      Given("a rate of 0")
      val rateLimiter = new BlobsRateLimiter[BlobContext](0.0)
      val context = new SimpleBlobContext("service1", "operation1")
      var counter = 0
      When("rate limiter is invoked")
      for (_ <- 1 to 200) {
        if (rateLimiter.test(context))
          counter += 1
        Thread.sleep(10)
      }
      Then("it should always fail the check")
      counter should equal(0)
    }
  }
}
