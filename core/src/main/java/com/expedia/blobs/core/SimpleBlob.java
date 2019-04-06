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
import org.apache.commons.lang3.Validate;

/**
 * Simple implementation of {@link Blob}
 */
public final class SimpleBlob implements Blob {
    private final String key;
    private final Map<String, String> metadata;
    private final byte[] data;

    /**
     * Constructor
     * @param key non-empty key
     * @param metadata non-null map
     * @param data non-null data
     */
    public SimpleBlob(String key, Map<String, String> metadata, byte[] data) {
        Validate.notEmpty(key);
        Validate.notNull(metadata);
        Validate.notNull(data);
        this.key = key;
        this.metadata = metadata;
        this.data = data;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public Map<String, String> getMetadata() {
        return metadata;
    }

    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public int getSize() {
        return data.length;
    }
}
