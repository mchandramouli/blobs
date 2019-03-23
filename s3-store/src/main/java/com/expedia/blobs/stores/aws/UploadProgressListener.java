package com.expedia.blobs.stores.aws;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;

public class UploadProgressListener implements ProgressListener {
    private final Logger logger;
    private final String key;

    UploadProgressListener(Logger logger, String fileKey) {
        Validate.notNull(logger);
        this.logger = logger;

        Validate.notEmpty(fileKey);
        this.key = fileKey;
    }

    @Override
    public void progressChanged(ProgressEvent progressEvent) {
        switch (progressEvent.getEventType()) {
            case TRANSFER_FAILED_EVENT:
            case TRANSFER_PART_FAILED_EVENT:
                logger.error("Progress event failed for key {} : {} => {}", key, progressEvent.getEventType(), progressEvent.getBytesTransferred());
                break;
            default:
                logger.info("Progress event for key {} : {} => {}", key, progressEvent.getEventType(), progressEvent.getBytesTransferred());
                break;
        }
    }
}
