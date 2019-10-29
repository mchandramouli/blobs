package com.expedia.haystack.agent.blobs.dispatcher.core;

import com.expedia.blobs.model.Blob;
import com.typesafe.config.Config;

import java.util.Optional;

//TODO: Rename this according to read and write functionality
public interface BlobDispatcher extends AutoCloseable {
    /**
     * @return the unique name for this dispatcher
     */
    String getName();

    /**
     * Dispatch the blob to the sink
     * @param blob complete {@link Blob} that will be dispatched
     */
    void dispatch(Blob blob);

    /**
     * @param key is the blob key that was used to save the blob
     * @return {@link Blob}
     */
    Optional<Blob> read(String key);

    /**
     * initializes the dispatcher for pushing blobs to the sink
     * @param conf to initialize the dispatcher
     */
    void initialize(final Config conf);
}
