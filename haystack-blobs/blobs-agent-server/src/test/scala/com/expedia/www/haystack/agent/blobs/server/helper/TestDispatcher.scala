package com.expedia.www.haystack.agent.blobs.server.helper

import java.util.Optional

import com.expedia.www.blobs.model.Blob
import com.expedia.www.haystack.agent.blobs.dispatcher.core.BlobDispatcher
import com.typesafe.config.Config

class TestDispatcher extends BlobDispatcher {

  private var isInitialized = false

  /**
    * returns the unique name for this dispatcher
    *
    * @return
    */
  override def getName: String = "test-dispatcher"

  /**
    * Dispatch the blob to the sink
    *
    * @param blob complete { @link Blob} that will be dispatched
    */
  override def dispatch(blob: Blob): Unit = ()


  /**
    *
    * @param key is the blob key that was used to save the blob
    * @return { @link Blob}
    */
  override def read(key: String): Optional[Blob] = Optional.empty()

  /**
    * initializes the dispatcher for pushing blobs to the sink
    *
    * @param conf
    */
  override def initialize(conf: Config): Unit = {
    isInitialized = true
    assert(conf != null && conf.getString("bucketName") == "mybucket" && conf.getString("region") == "us-east-1")
  }

  /**
    * close the dispatcher, this is called when the agent is shutting down.
    */
  override def close(): Unit = {
    assert(isInitialized, "Fail to close as the dispatcher isn't initialized yet")
  }
}
