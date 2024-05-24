package org.swisspush.reststorage.exception;

import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;

/**
 * Applies dependency inversion for exception instantiation.
 *
 * This class did arise because we had different use cases in different
 * applications. One of them has the need to perform fine-grained error
 * reporting. Whereas in the other application this led to performance issues.
 * So now through this abstraction, both applications can choose the behavior
 * they need.
 *
 * If dependency-injection gets applied properly, an app can even provide its
 * custom implementation to fine-tune the exact behavior even further.
 */
public interface RestStorageExceptionFactory {

    /** Convenience overload for {@link #newException(String, Throwable)}. */
    public default Exception newException(String msg){ return newException(msg, null); }

    /** Convenience overload for {@link #newException(String, Throwable)}. */
    public default Exception newException(Throwable cause){ return newException(null, cause); }

    public Exception newException(String message, Throwable cause);

    /** Convenience overload for {@link #newRuntimeException(String, Throwable)}. */
    public default RuntimeException newRuntimeException(String msg){ return newRuntimeException(msg, null); }

    /** Convenience overload for {@link #newRuntimeException(String, Throwable)}. */
    public default RuntimeException newRuntimeException(Throwable cause){ return newRuntimeException(null, cause); }

    public RuntimeException newRuntimeException(String message, Throwable cause);

    public ReplyException newReplyException(ReplyFailure failureType, int failureCode, String message);


    /**
     * See {@link RestStorageThriftyExceptionFactory}.
     */
    public static RestStorageExceptionFactory newRestStorageThriftyExceptionFactory() {
        return new RestStorageThriftyExceptionFactory();
    }

    /**
     * See {@link RestStorageWastefulExceptionFactory}.
     */
    public static RestStorageExceptionFactory newRestStorageWastefulExceptionFactory() {
        return new RestStorageWastefulExceptionFactory();
    }

}
