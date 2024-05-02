package org.swisspush.reststorage.util;

import io.vertx.core.MultiMap;

/**
 * Enum for HTTP request headers used in vertx-rest-storage
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public enum HttpRequestHeader {

    ETAG_HEADER("Etag"),
    IF_NONE_MATCH_HEADER("if-none-match"),
    LOCK_HEADER("x-lock"),
    LOCK_MODE_HEADER("x-lock-mode"),
    LOCK_EXPIRE_AFTER_HEADER("x-lock-expire-after"),
    EXPIRE_AFTER_HEADER("x-expire-after"),
    IMPORTANCE_LEVEL_HEADER("x-importance-level"),
    MAX_EXPAND_RESOURCES_HEADER("x-max-expand-resources"),
    COMPRESS_HEADER("x-stored-compressed"),
    CONTENT_TYPE("Content-Type"),
    CONTENT_LENGTH("Content-Length");

    private final String name;

    HttpRequestHeader(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static boolean containsHeader(MultiMap headers, HttpRequestHeader httpRequestHeader){
        if(headers == null){
            return false;
        }
        return headers.contains(httpRequestHeader.getName());
    }

    /**
     * Get the value of the provided {@link HttpRequestHeader} as Integer.
     * <p>Returns <code>null</code> in the following cases:</p>
     *
     * <ul>
     *     <li>headers are <code>null</code></li>
     *     <li>headers does not contain httpRequestHeader</li>
     *     <li>httpRequestHeader is no parsable Integer i.e. empty string, non-digit characters, numbers to bigger than Integer allows</li>
     * </ul>
     *
     * @param headers the http request headers
     * @param httpRequestHeader the http request header to get the value from
     * @return a Integer representing the value of the httpRequestHeader or null
     */
    public static Integer getInteger(MultiMap headers, HttpRequestHeader httpRequestHeader) {
        return getInteger(headers, httpRequestHeader, null);
    }

    /**
     * Get the value of the provided {@link HttpRequestHeader} or a default value as Integer.
     * <p>Returns the default value in the following cases:</p>
     *
     * <ul>
     *     <li>headers are <code>null</code></li>
     *     <li>headers does not contain httpRequestHeader</li>
     *     <li>httpRequestHeader is no parsable Integer i.e. empty string, non-digit characters, numbers to bigger than Integer allows</li>
     * </ul>
     *
     * @param headers the http request headers
     * @param httpRequestHeader the http request header to get the value from
     * @param defaultValue the default value to return when no value from httpRequestHeader is extractable
     * @return a Integer representing the value of the httpRequestHeader or the default value
     */
    public static Integer getInteger(MultiMap headers, HttpRequestHeader httpRequestHeader, Integer defaultValue) {
        String headerValue = null;
        if(headers != null) {
            headerValue = headers.get(httpRequestHeader.getName());
        }

        try {
            return Integer.parseInt(headerValue);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    /**
     * Get the value of the provided {@link HttpRequestHeader} as Long.
     * <p>Returns <code>null</code> in the following cases:</p>
     *
     * <ul>
     *     <li>headers are <code>null</code></li>
     *     <li>headers does not contain httpRequestHeader</li>
     *     <li>httpRequestHeader is no parsable Long i.e. empty string, non-digit characters, numbers to bigger than Long allows</li>
     * </ul>
     *
     * @param headers the http request headers
     * @param httpRequestHeader the http request header to get the value from
     * @return a Long representing the value of the httpRequestHeader or null
     */
    public static Long getLong(MultiMap headers, HttpRequestHeader httpRequestHeader) {
        return getLong(headers, httpRequestHeader, null);
    }

    /**
     * Get the value of the provided {@link HttpRequestHeader} or a default value as Long.
     * <p>Returns the default value in the following cases:</p>
     *
     * <ul>
     *     <li>headers are <code>null</code></li>
     *     <li>headers does not contain httpRequestHeader</li>
     *     <li>httpRequestHeader is no parsable Long i.e. empty string, non-digit characters, numbers to bigger than Long allows</li>
     * </ul>
     *
     * @param headers the http request headers
     * @param httpRequestHeader the http request header to get the value from
     * @param defaultValue the default value to return when no value from httpRequestHeader is extractable
     * @return a Long representing the value of the httpRequestHeader or the default value
     */
    public static Long getLong(MultiMap headers, HttpRequestHeader httpRequestHeader, Long defaultValue) {
        String headerValue = null;
        if(headers != null) {
            headerValue = headers.get(httpRequestHeader.getName());
        }

        try {
            return Long.parseLong(headerValue);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    /**
     * Get the value of the provided {@link HttpRequestHeader} as String.
     * <p>Returns <code>null</code> in the following cases:</p>
     *
     * <ul>
     *     <li>headers are <code>null</code></li>
     *     <li>headers does not contain httpRequestHeader</li>
     * </ul>
     *
     * @param headers the http request headers
     * @param httpRequestHeader the http request header to get the value from
     * @return a String representing the value of the httpRequestHeader or null
     */
    public static String getString(MultiMap headers, HttpRequestHeader httpRequestHeader) {
        if(headers == null) {
            return null;
        }
        return headers.get(httpRequestHeader.getName());
    }
}
