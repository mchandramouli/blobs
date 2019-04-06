/*
 *
 *     Copyright 2018 Expedia, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 *
 */
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
