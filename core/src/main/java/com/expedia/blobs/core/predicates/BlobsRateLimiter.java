package com.expedia.blobs.core.predicates;

import com.expedia.blobs.core.BlobContext;
import java.util.function.Predicate;

public class BlobsRateLimiter<T extends BlobContext> implements Predicate<T> {
    @Override
    public boolean test(T t) {
        //implement rate limit logic
        return true;
    }
}
