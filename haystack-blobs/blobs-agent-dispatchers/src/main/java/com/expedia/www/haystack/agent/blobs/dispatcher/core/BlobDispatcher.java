package com.expedia.www.haystack.agent.blobs.dispatcher.core;

import com.expedia.blobs.core.Blob;
import com.typesafe.config.Config;

public interface BlobDispatcher extends AutoCloseable {
    /**
     * returns the unique name for this dispatcher
     * @return
     */
    String getName();

    /**
     * Dispatch the blob to the sink
     * @param blob complete {@link Blob} that will be dispatched
     */
    void dispatch(Blob blob);

    /**
     * initializes the dispatcher for pushing blobs to the sink
     * @param conf
     */
    void initialize(final Config conf);
}
