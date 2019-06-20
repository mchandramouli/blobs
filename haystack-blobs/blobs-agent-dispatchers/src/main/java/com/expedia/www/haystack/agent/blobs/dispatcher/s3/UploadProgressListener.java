package com.expedia.www.haystack.agent.blobs.dispatcher.s3;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
//TODO: Duplicate of store
class UploadProgressListener implements ProgressListener {
    private final Logger logger;
    private final String key;

    UploadProgressListener(Logger logger, String objectKey) {
        Validate.notNull(logger);
        this.logger = logger;

        Validate.notEmpty(objectKey);
        this.key = objectKey;
    }

    @Override
    public void progressChanged(ProgressEvent progressEvent) {
        final String msg = String.format("Progress event=%s file=%s transferred=%d", progressEvent.getEventType(), key,
                progressEvent.getBytesTransferred());
        switch (progressEvent.getEventType()) {
            case TRANSFER_FAILED_EVENT:
            case TRANSFER_PART_FAILED_EVENT:
                logger.error(msg);
                break;
            default:
                logger.info(msg);
                break;
        }
    }
}
