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
package com.expedia.blobs.core.predicates;

import com.expedia.blobs.core.BlobContext;
import com.revinate.guava.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Predicate;

public class BlobsRateLimiter<T extends BlobContext> implements Predicate<T> {
    private final Logger LOGGER = LoggerFactory.getLogger(BlobsRateLimiter.class);
    private final RateLimiter rateLimiter;
    private final double ratePerSecond;

    public BlobsRateLimiter(double ratePerSecond) {
        this.ratePerSecond = ratePerSecond;
        this.rateLimiter = (this.ratePerSecond > 0) ? RateLimiter.create(this.ratePerSecond) : null;
    }

    @Override
    public boolean test(T t) {
        final boolean ok = (rateLimiter != null) && rateLimiter.tryAcquire();

        if (!ok && LOGGER.isInfoEnabled()) {
            LOGGER.info("rate limit of " + this.ratePerSecond + " blobs/sec has reached!");
        }

        return ok;
    }
}
