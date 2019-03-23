package com.expedia.blobs.core.predicates;

import com.expedia.blobs.core.BlobContext;
import com.revinate.guava.util.concurrent.RateLimiter;
import java.util.function.Predicate;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobsRateLimiter<T extends BlobContext> implements Predicate<T> {
    private final Logger LOGGER = LoggerFactory.getLogger(BlobsRateLimiter.class);
    private final RateLimiter rateLimiter;

    public BlobsRateLimiter(double ratePerSecond) {
        Validate.isTrue(ratePerSecond > 0);
        this.rateLimiter = RateLimiter.create(ratePerSecond);
    }

    @Override
    public boolean test(T t) {
        final boolean ok = rateLimiter.tryAcquire();

        if (!ok) {
            LOGGER.info("maximum dispatch rate limit of " + rateLimiter.getRate() + " blobs/sec has reached!");
        }

        return ok;
    }
}
