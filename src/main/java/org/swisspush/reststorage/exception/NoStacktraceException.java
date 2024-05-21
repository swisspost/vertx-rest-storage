package org.swisspush.reststorage.exception;

/**
 * Basically same as in vertx, But adding the forgotten contructors.
 */
public class NoStacktraceException extends RuntimeException {

    public NoStacktraceException() {
    }

    public NoStacktraceException(String message) {
        super(message);
    }

    public NoStacktraceException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoStacktraceException(Throwable cause) {
        super(cause);
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }

}
