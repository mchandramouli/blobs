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
 * Class representing the content type of {@link Blob} object being saved or retrieved
 */
public class ContentType {
    private final String type;
    public static ContentType JSON = ContentType.from("application/json");
    public static ContentType XML = ContentType.from("application/xml");

    private ContentType(String type) {
        Validate.notEmpty(type);
        this.type = type;
    }

    public String getType() {
        return this.type;
    }

    /**
     * factory method to create a {@link ContentType} instance
     * @param type non-empty mime-type string
     * @return valid BlobType instance
     */
    public static ContentType from(String type) {
        return new ContentType(type);
    }
}
