package org.swisspush.reststorage.exception;

import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;

/**
 * Trades maintainability for speed. For example prefers lightweight
 * exceptions without stacktrace recording. It may even decide to drop 'cause'
 * and 'suppressed' exceptions. If an app needs more error details it should use
 * {@link WastefulExceptionFactory}. If none of those fits the apps needs, it
 * can provide its own implementation.
 */
class ThriftyExceptionFactory implements ExceptionFactory {

    ThriftyExceptionFactory() {
    }

    @Override
    public Exception newException(String message, Throwable cause) {
        return new NoStacktraceException(message, cause);
    }

    @Override
    public ReplyException newReplyException(ReplyFailure failureType, int failureCode, String message) {
        return new NoStackReplyException(failureType, failureCode, message);
    }

}
