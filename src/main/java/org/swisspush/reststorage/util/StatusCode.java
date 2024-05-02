package org.swisspush.reststorage.util;

/**
 * Enum for HTTP status codes
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public enum StatusCode {
    OK(200, "OK"),
    FOUND(302, "Found"),
    NOT_MODIFIED(304, "Not Modified"),
    BAD_REQUEST(400, "Bad Request"),
    UNAUTHORIZED(401, "Unauthorized"),
    NOT_FOUND(404, "Not Found"),
    METHOD_NOT_ALLOWED(405, "Method Not Allowed"),
    PAYLOAD_TOO_LARGE(413, "Payload Too Large"),
    INTERNAL_SERVER_ERROR(500, "Internal Server Error"),
    INSUFFICIENT_STORAGE(507, "Insufficient Storage"),
    CONFLICT(409, "Conflict");

    private final int statusCode;
    private final String statusMessage;

    StatusCode(int statusCode, String statusMessage) {
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    @Override
    public String toString() {
        return statusCode + " " + statusMessage;
    }
}