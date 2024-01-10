package org.swisspush.reststorage.mocks;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.Cookie;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.net.HostAndPort;

import java.util.Set;


/**
 * Simple base class for mocking.
 */
public class FailFastVertxHttpServerResponse implements HttpServerResponse {

    protected final String msg;

    public FailFastVertxHttpServerResponse() {
        this("Behaviour not specified for mock. Override to implement your behaviour.");
    }

    public FailFastVertxHttpServerResponse(String msg) {
        this.msg = msg;
    }

    @Override
    public HttpServerResponse exceptionHandler(Handler<Throwable> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> write(Buffer data) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void write(Buffer data, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse setWriteQueueMaxSize(int maxSize) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public boolean writeQueueFull() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse drainHandler(Handler<Void> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public int getStatusCode() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse setStatusCode(int statusCode) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public String getStatusMessage() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse setStatusMessage(String statusMessage) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse setChunked(boolean chunked) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public boolean isChunked() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public MultiMap headers() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse putHeader(String name, String value) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse putHeader(CharSequence name, CharSequence value) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse putHeader(String name, Iterable<String> values) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse putHeader(CharSequence name, Iterable<CharSequence> values) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public MultiMap trailers() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse putTrailer(String name, String value) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse putTrailer(CharSequence name, CharSequence value) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse putTrailer(String name, Iterable<String> values) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse putTrailer(CharSequence name, Iterable<CharSequence> value) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse closeHandler(Handler<Void> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse endHandler(Handler<Void> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> write(String chunk, String enc) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void write(String chunk, String enc, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> write(String chunk) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void write(String chunk, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse writeContinue() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> writeEarlyHints(MultiMap headers) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void writeEarlyHints(MultiMap headers, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> end(String chunk) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void end(String chunk, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> end(String chunk, String enc) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void end(String chunk, String enc, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> end(Buffer chunk) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void end(Buffer chunk, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> end() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void end(Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> sendFile(String filename, long offset, long length) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse sendFile(String filename, long offset, long length, Handler<AsyncResult<Void>> resultHandler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public boolean ended() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public boolean closed() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public boolean headWritten() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse headersEndHandler(Handler<Void> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse bodyEndHandler(Handler<Void> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public long bytesWritten() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public int streamId() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse push(HttpMethod method, String host, String path, Handler<AsyncResult<HttpServerResponse>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse push(HttpMethod method, String path, MultiMap headers, Handler<AsyncResult<HttpServerResponse>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse push(HttpMethod method, String path, Handler<AsyncResult<HttpServerResponse>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse push(HttpMethod method, String host, String path, MultiMap headers, Handler<AsyncResult<HttpServerResponse>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<HttpServerResponse> push(HttpMethod method, HostAndPort authority, String path, MultiMap headers) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<HttpServerResponse> push(HttpMethod method, String host, String path, MultiMap headers) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public boolean reset(long code) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse writeCustomFrame(int type, int flags, Buffer payload) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse addCookie(Cookie cookie) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Cookie removeCookie(String name, boolean invalidate) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Set<Cookie> removeCookies(String name, boolean invalidate) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Cookie removeCookie(String name, String domain, String path, boolean invalidate) {
        throw new UnsupportedOperationException(msg);
    }
}
