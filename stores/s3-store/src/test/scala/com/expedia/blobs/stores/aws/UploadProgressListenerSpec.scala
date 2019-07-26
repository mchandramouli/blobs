package com.expedia.blobs.stores.aws

import com.amazonaws.event.{ProgressEvent, ProgressEventType}
import org.easymock.EasyMock.{replay, verify}
import org.scalatest.easymock.EasyMockSugar._
import org.scalatest.{BeforeAndAfter, FunSpec, GivenWhenThen, Matchers}
import org.slf4j.Logger

class UploadProgressListenerSpec extends FunSpec with GivenWhenThen with BeforeAndAfter with Matchers {
  describe("class the listens to the state changes during an upload") {
    it("should log an error when transfer failed event occurs") {
      Given("a progress event of type failure and a listener")
      val event = new ProgressEvent(ProgressEventType.TRANSFER_FAILED_EVENT)
      val logger = mock[Logger]
      val listener = new UploadProgressListener(logger, "some-file-key")
      expecting {
        logger.error("Progress event=TRANSFER_FAILED_EVENT file=some-file-key transferred=0").once()
      }
      replay(logger)
      When("the event is sent to the listener")
      listener.progressChanged(event)
      Then("it should log as expected")
      verify(logger)
    }
    it("should log an error when transfer part failed event occurs") {
      Given("a progress event of type part failure and a listener")
      val event = new ProgressEvent(ProgressEventType.TRANSFER_PART_FAILED_EVENT, 50)
      val logger = mock[Logger]
      val listener = new UploadProgressListener(logger, "some-file-key")
      expecting {
        logger.error("Progress event=TRANSFER_PART_FAILED_EVENT file=some-file-key transferred=0").once()
      }
      replay(logger)
      When("the event is sent to the listener")
      listener.progressChanged(event)
      Then("it should log as expected")
      verify(logger)
    }
    it("should simply write an info event for all other state changes") {
      Given("a progress event of type success and a listener")
      val event = new ProgressEvent(ProgressEventType.TRANSFER_COMPLETED_EVENT, 50)
      val logger = mock[Logger]
      val listener = new UploadProgressListener(logger, "some-file-key")
      expecting {
        logger.info("Progress event=TRANSFER_COMPLETED_EVENT file=some-file-key transferred=0").once()
      }
      replay(logger)
      When("the event is sent to the listener")
      listener.progressChanged(event)
      Then("it should log as expected")
      verify(logger)
    }
  }
}
