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
package com.expedia.blobs.core;

import java.util.UUID;

/**
 * Operation context related to the blob
 */
public interface BlobContext {
    /**
     * unique id of the operation
     * @return string
     */
    default String getOperationId() {
        return UUID.randomUUID().toString();
    }

    /**
     * name of the operation
     * @return string
     */
    String getOperationName();

    /**
     * name of the service
     * @return string
     */
    String getServiceName();

    /**
     * create a unique key for the given {@link BlobType}
     * @param type type of the blob being saved
     * @return string
     */
    default String makeKey(BlobType type) {
        return String.format("%s/%s/%s/%s-%s",
                             getServiceName(),
                             getOperationName(),
                             getOperationId(),
                             type.getType(), UUID.randomUUID().toString());
    }

    /**
     * perform some task after creation of a blob key
     * @param blobKey created from {@link BlobContext#makeKey(BlobType)} function
     * @param blobType {@link BlobType}
     */
    void onBlobKeyCreate(String blobKey, BlobType blobType);
}
