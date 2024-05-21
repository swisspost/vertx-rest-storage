package org.swisspush.reststorage.exception;

/**
 * Basically same as in vertx, But adding the forgotten contructors.
 */
public class RestStorageNoStacktraceException extends RuntimeException {

    public RestStorageNoStacktraceException() {
    }

    public RestStorageNoStacktraceException(String message) {
        super(message);
    }

    public RestStorageNoStacktraceException(String message, Throwable cause) {
        super(message, cause);
    }

    public RestStorageNoStacktraceException(Throwable cause) {
        super(cause);
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }

}
