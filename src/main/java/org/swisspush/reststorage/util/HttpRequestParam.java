package org.swisspush.reststorage.util;

import io.vertx.core.MultiMap;

/**
 * Enum for HTTP request params used in vertx-rest-storage
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public enum HttpRequestParam {

    RECURSIVE_PARAMETER("recursive"),
    STORAGE_EXPAND_PARAMETER("storageExpand"),
    LIST_ONLY_PARAMETER("listOnly"),
    LIMIT_PARAMETER("limit"),
    OFFSET_PARAMETER("offset");

    private final String name;

    HttpRequestParam(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static boolean containsParam(MultiMap params, HttpRequestParam httpRequestParam){
        if(params == null){
            return false;
        }
        return params.contains(httpRequestParam.getName());
    }

    /**
     * Get the value of the provided {@link HttpRequestParam} as String.
     * <p>Returns <code>null</code> in the following cases:</p>
     *
     * <ul>
     *     <li>params are <code>null</code></li>
     *     <li>params does not contain httpRequestParam</li>
     * </ul>
     *
     * @param params the http request params
     * @param httpRequestParam the http request param to get the value from
     * @return a String representing the value of the httpRequestParam or null
     */
    public static String getString(MultiMap params, HttpRequestParam httpRequestParam) {
        if(params == null) {
            return null;
        }
        return params.get(httpRequestParam.getName());
    }

    /**
     * Get the value of the provided {@link HttpRequestParam} as boolean.
     * <p>Returns <code>false</code> in the following cases:</p>
     *
     * <ul>
     *     <li>params are <code>null</code></li>
     *     <li>params does not contain httpRequestParam</li>
     *     <li>value of httpRequestParam does not equal "true" ignoring the case</li>
     * </ul>
     *
     * @param params the http request params
     * @param httpRequestParam the http request param to get the value from
     * @return a boolean representing the value of the httpRequestParam
     */
    public static boolean getBoolean(MultiMap params, HttpRequestParam httpRequestParam) {
        if(params == null) {
            return false;
        }
        String paramValue = params.get(httpRequestParam.getName());
        return "true".equalsIgnoreCase(paramValue);
    }
}
