package org.swisspush.reststorage.exception;

import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;

/**
 * Trades maintainability for speed. For example prefers lightweight
 * exceptions without stacktrace recording. It may even decide to drop 'cause'
 * and 'suppressed' exceptions. Or to make other optimizations towards
 * performance. If an app needs more error details it should use
 * {@link RestStorageWastefulExceptionFactory}. If none of those fits the apps
 * needs, it can provide its own implementation.
 */
class RestStorageThriftyExceptionFactory implements RestStorageExceptionFactory {

    RestStorageThriftyExceptionFactory() {
    }

    @Override
    public Exception newException(String message, Throwable cause) {
        // This implementation cares about performance. So if 'cause' is already
        // of the correct type, we just re-use it directly. There's no need to
        // produce yet another exception instance.
        if (cause instanceof Exception) return (Exception) cause;
        return new RestStorageNoStacktraceException(message, cause);
    }

    @Override
    public RuntimeException newRuntimeException(String msg, Throwable cause) {
        // This implementation cares about performance. So if 'cause' is already
        // of the correct type, we just re-use it directly. There's no need to
        // produce yet another exception instance.
        if (cause instanceof RuntimeException) return (RuntimeException) cause;
        return new RestStorageNoStacktraceException(msg, cause);
    }

    @Override
    public ReplyException newReplyException(ReplyFailure failureType, int failureCode, String message) {
        return new RestStorageNoStackReplyException(failureType, failureCode, message);
    }

}
