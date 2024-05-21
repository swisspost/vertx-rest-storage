package org.swisspush.reststorage.exception;

import io.vertx.core.eventbus.ReplyFailure;

/**
 * There was once a fix in vertx for this (https://github.com/eclipse-vertx/vert.x/issues/4840)
 * but for whatever reason in our case we still see stack-trace recordings. Passing
 * this subclass to {@link io.vertx.core.eventbus.Message#reply(Object)} seems to
 * do the trick.
 */
public class NoStackReplyException extends io.vertx.core.eventbus.ReplyException {

    public NoStackReplyException(ReplyFailure failureType, int failureCode, String message) {
        super(failureType, failureCode, message);
    }

    public NoStackReplyException(ReplyFailure failureType, String message) {
        this(failureType, -1, message);
    }

    public NoStackReplyException(ReplyFailure failureType) {
        this(failureType, -1, null);
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }

}
