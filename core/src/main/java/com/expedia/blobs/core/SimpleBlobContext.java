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

import org.apache.commons.lang.Validate;

/**
 * Simple class representing a {@link BlobContext} associated with {@link BlobWriter}
 */
public class SimpleBlobContext implements BlobContext {
    private final String serviceName;
    private final String operationName;

    /**
     * constructor
     * @param serviceName non-empty string
     * @param operationName non-empty string
     */
    public SimpleBlobContext(String serviceName, String operationName) {
        Validate.notEmpty(serviceName);
        Validate.notEmpty(operationName);
        this.serviceName = serviceName;
        this.operationName = operationName;
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    @Override
    public String getOperationName() {
        return operationName;
    }
}
