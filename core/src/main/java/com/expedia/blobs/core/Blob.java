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

import java.util.Map;

/**
 * Container class that represents an instance of a binary large object along with
 * its metadata and key
 */
public interface Blob {
    /**
     * unique key of the blob instance
     * @return string
     */
    String getKey();

    /**
     * Map of metadata
     * @return a valid map object
     */
    Map<String, String> getMetadata();

    /**
     * Array of bytes representing the object
     * @return valid array
     */
    byte[] getData();

    /**
     * size of the object
     * @return integer
     */
    int getSize();
}
