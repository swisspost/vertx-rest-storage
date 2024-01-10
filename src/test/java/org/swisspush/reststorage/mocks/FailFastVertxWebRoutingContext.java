package org.swisspush.reststorage.mocks;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.Cookie;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.*;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class FailFastVertxWebRoutingContext implements RoutingContext {

    protected final String msg;

    public FailFastVertxWebRoutingContext() {
        this("Method not implemented in mock. Override method to provide your behaviour.");
    }

    public FailFastVertxWebRoutingContext(String msg) {
        this.msg = msg;
    }

    @Override
    public HttpServerRequest request() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse response() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void next() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void fail(int i) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void fail(Throwable throwable) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void fail(int i, Throwable throwable) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public RoutingContext put(String s, Object o) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public <T> T get(String s) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public <T> T get(String s, T t) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public <T> T remove(String s) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Map<String, Object> data() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Vertx vertx() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public String mountPoint() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Route currentRoute() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public String normalisedPath() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public String normalizedPath() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Cookie getCookie(String s) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public RoutingContext addCookie(Cookie cookie) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Cookie removeCookie(String s, boolean b) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public int cookieCount() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Map<String, Cookie> cookieMap() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public String getBodyAsString() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public String getBodyAsString(String s) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public JsonObject getBodyAsJson(int i) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public JsonArray getBodyAsJsonArray(int i) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public JsonObject getBodyAsJson() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public JsonArray getBodyAsJsonArray() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Buffer getBody() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public RequestBody body() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public List<FileUpload> fileUploads() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void cancelAndCleanupFileUploads() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Session session() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public boolean isSessionAccessed() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public User user() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Throwable failure() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public int statusCode() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public String getAcceptableContentType() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public ParsedHeaderValues parsedHeaders() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public int addHeadersEndHandler(Handler<Void> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public boolean removeHeadersEndHandler(int i) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public int addBodyEndHandler(Handler<Void> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public boolean removeBodyEndHandler(int i) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public int addEndHandler(Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public boolean removeEndHandler(int i) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public boolean failed() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void setBody(Buffer buffer) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void setSession(Session session) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void setUser(User user) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void clearUser() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void setAcceptableContentType(String s) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void reroute(HttpMethod httpMethod, String s) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Map<String, String> pathParams() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public String pathParam(String s) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public MultiMap queryParams() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public MultiMap queryParams(Charset charset) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public List<String> queryParam(String s) {
        throw new UnsupportedOperationException(msg);
    }
}
