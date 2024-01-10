package org.swisspush.reststorage.mocks;

import io.netty.handler.codec.DecoderResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.*;
import io.vertx.core.http.impl.HttpServerRequestInternal;
import io.vertx.core.net.HostAndPort;
import io.vertx.core.net.NetSocket;
import io.vertx.core.net.SocketAddress;

import javax.net.ssl.SSLSession;
import javax.security.cert.X509Certificate;
import java.util.Set;


/**
 * Simple base class for mocking.
 */
public class FailFastVertxHttpServerRequest extends HttpServerRequestInternal {

    protected final String msg;

    public FailFastVertxHttpServerRequest() {
        this("Behaviour not specified for mock. Override to implement your behaviour.");
    }

    public FailFastVertxHttpServerRequest(String msg) {
        this.msg = msg;
    }

    @Override
    public HttpServerRequest exceptionHandler(Handler<Throwable> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerRequest handler(Handler<Buffer> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerRequest pause() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerRequest resume() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerRequest fetch(long amount) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerRequest endHandler(Handler<Void> endHandler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpVersion version() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpMethod method() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public boolean isSSL() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public String scheme() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public String uri() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public String path() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public String query() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HostAndPort authority() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public String host() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public long bytesRead() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerResponse response() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public MultiMap headers() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public String getHeader(String headerName) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public String getHeader(CharSequence headerName) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerRequest setParamsCharset(String charset) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public String getParamsCharset() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public MultiMap params() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public String getParam(String paramName) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public SocketAddress remoteAddress() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public SocketAddress localAddress() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public SSLSession sslSession() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public X509Certificate[] peerCertificateChain() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public String absoluteURI() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Buffer> body() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> end() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<NetSocket> toNetSocket() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerRequest setExpectMultipart(boolean expect) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public boolean isExpectMultipart() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerRequest uploadHandler(Handler<HttpServerFileUpload> uploadHandler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public MultiMap formAttributes() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public String getFormAttribute(String attributeName) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<ServerWebSocket> toWebSocket() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public boolean isEnded() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerRequest customFrameHandler(Handler<HttpFrame> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpConnection connection() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServerRequest streamPriorityHandler(Handler<StreamPriority> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public DecoderResult decoderResult() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Cookie getCookie(String name) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Cookie getCookie(String name, String domain, String path) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Set<Cookie> cookies(String name) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Set<Cookie> cookies() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Context context() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Object metric() {
        throw new UnsupportedOperationException(msg);
    }
}
