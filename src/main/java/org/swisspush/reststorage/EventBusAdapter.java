package org.swisspush.reststorage;

import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.*;
import io.vertx.core.http.impl.HttpServerRequestInternal;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.HostAndPort;
import io.vertx.core.net.NetSocket;
import io.vertx.core.net.SocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLSession;
import javax.security.cert.X509Certificate;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Provides a direct eventbus interface.
 *
 * @author lbovet
 */
public class EventBusAdapter {

    private static final Logger log = getLogger(EventBusAdapter.class);

    public void init(final Vertx vertx, String address, final Handler<HttpServerRequest> requestHandler) {
        vertx.eventBus().consumer(address, (Handler<Message<Buffer>>) message -> requestHandler.handle(new MappedHttpServerRequest(vertx, message)));
    }

    private static class MappedHttpServerRequest extends HttpServerRequestInternal {
        private final Vertx vertx;
        private final Buffer requestPayload;
        private final HttpMethod method;
        private final String uri;
        private final MultiMap requestHeaders;
        private final Message<Buffer> message;
        private String path;
        private String query;
        private MultiMap params;
        private Charset paramsCharset = StandardCharsets.UTF_8;
        private Handler<Buffer> dataHandler;
        private Handler<Void> endHandler;
        private HttpServerResponse response;

        private MappedHttpServerRequest(Vertx vertx, Message<Buffer> message) {
            this.vertx = vertx;
            this.message = message;
            Buffer buffer = message.body();
            int headerLength = buffer.getInt(0);
            JsonObject header = new JsonObject(buffer.getString(4, headerLength + 4));
            method = httpMethodFromHeader(header);
            uri = header.getString("uri");
            requestPayload = buffer.getBuffer(headerLength + 4, buffer.length());

            JsonArray headerArray = header.getJsonArray("headers");
            if (headerArray != null) {
                requestHeaders = fromJson(headerArray);
            } else {
                requestHeaders = new HeadersMultiMap();
            }
        }

        private HttpMethod httpMethodFromHeader(JsonObject header) {
            String method = header.getString("method");
            if (method != null) {
                return HttpMethod.valueOf(method.toUpperCase());
            }
            return null;
        }

        @Override
        public HttpVersion version() {
            return HttpVersion.HTTP_1_0;
        }

        @Override
        public HttpMethod method() {
            return method;
        }


        @Override
        public boolean isSSL() {
            return false;
        }

        @Override
        public String scheme() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String uri() {
            return uri;
        }

        @Override
        public String path() {
            if (path == null) {
                path = UrlParser.path(uri);
            }
            return path;
        }

        @Override
        public String query() {
            if (query == null) {
                query = UrlParser.query(uri);
            }
            return query;
        }

        @Override
        public HostAndPort authority() {
            return null;
        }

        @Override
        public String host() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long bytesRead() {
            return 0;
        }

        @Override
        public HttpServerResponse response() {
            if (response == null) {
                response = new HttpServerResponse() {

                    private int statusCode;
                    private String statusMessage;
                    private final MultiMap responseHeaders = new HeadersMultiMap();
                    private final Buffer responsePayload = Buffer.buffer();

                    @Override
                    public int getStatusCode() {
                        return statusCode;
                    }

                    @Override
                    public HttpServerResponse setStatusCode(int i) {
                        statusCode = i;
                        return this;
                    }

                    @Override
                    public String getStatusMessage() {
                        return statusMessage;
                    }

                    @Override
                    public HttpServerResponse setStatusMessage(String s) {
                        statusMessage = s;
                        return this;
                    }

                    @Override
                    public HttpServerResponse setChunked(boolean b) {
                        return this;
                    }

                    @Override
                    public boolean isChunked() {
                        return false;
                    }

                    @Override
                    public MultiMap headers() {
                        return responseHeaders;
                    }

                    @Override
                    public HttpServerResponse putHeader(String s, String s2) {
                        responseHeaders.set(s, s2);
                        return this;
                    }

                    @Override
                    public HttpServerResponse putHeader(CharSequence charSequence, CharSequence charSequence2) {
                        responseHeaders.set(charSequence, charSequence2);
                        return this;
                    }

                    @Override
                    public HttpServerResponse putHeader(String s, Iterable<String> strings) {
                        for (String value : strings) {
                            responseHeaders.add(s, value);
                        }
                        return this;
                    }

                    @Override
                    public HttpServerResponse putHeader(CharSequence charSequence, Iterable<CharSequence> charSequences) {
                        for (CharSequence value : charSequences) {
                            responseHeaders.add(charSequence, value);
                        }
                        return this;
                    }

                    @Override
                    public MultiMap trailers() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public HttpServerResponse putTrailer(String s, String s2) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public HttpServerResponse putTrailer(CharSequence charSequence, CharSequence charSequence2) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public HttpServerResponse putTrailer(String s, Iterable<String> strings) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public HttpServerResponse putTrailer(CharSequence charSequence, Iterable<CharSequence> charSequences) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public HttpServerResponse closeHandler(Handler<Void> voidHandler) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public HttpServerResponse endHandler(Handler<Void> handler) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Future<Void> write(String chunk, String enc) {
                        responsePayload.appendBuffer(Buffer.buffer(chunk, enc));
                        return Future.succeededFuture();
                    }

                    @Override
                    public Future<Void> write(Buffer buffer) {
                        responsePayload.appendBuffer(buffer);
                        return Future.succeededFuture();
                    }

                    @Override
                    public void write(Buffer data, Handler<AsyncResult<Void>> handler) {
                        responsePayload.appendBuffer(data);
                        handler.handle(Future.succeededFuture());
                    }

                    @Override
                    public void write(String chunk, String enc, Handler<AsyncResult<Void>> handler) {
                        responsePayload.appendBuffer(Buffer.buffer(chunk, enc));
                        handler.handle(Future.succeededFuture());
                    }

                    @Override
                    public Future<Void> write(String chunk) {
                        responsePayload.appendBuffer(Buffer.buffer(chunk));
                        return Future.succeededFuture();
                    }

                    @Override
                    public void write(String chunk, Handler<AsyncResult<Void>> handler) {
                        write(chunk).onComplete(handler);
                    }

                    @Override
                    public HttpServerResponse writeContinue() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Future<Void> writeEarlyHints(MultiMap headers) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void writeEarlyHints(MultiMap headers, Handler<AsyncResult<Void>> handler) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Future<Void> end(String chunk) {
                        write(Buffer.buffer(chunk));
                        return end();
                    }

                    @Override
                    public void end(String chunk, Handler<AsyncResult<Void>> handler) {
                        write(Buffer.buffer(chunk));
                        end().onComplete(handler);
                    }

                    @Override
                    public Future<Void> end(String chunk, String enc) {
                        write(chunk, enc);
                        return end();
                    }

                    @Override
                    public void end(String chunk, String enc, Handler<AsyncResult<Void>> handler) {
                        write(chunk, enc);
                        end().onComplete(handler);
                    }

                    @Override
                    public Future<Void> end(Buffer chunk) {
                        write(chunk);
                        return end();
                    }

                    @Override
                    public void end(Buffer chunk, Handler<AsyncResult<Void>> handler) {
                        write(chunk);
                        end().onComplete(handler);
                    }

                    @Override
                    public Future<Void> end() {
                        JsonObject header = new JsonObject();
                        if (statusCode == 0) {
                            statusCode = 200;
                            statusMessage = "OK";
                        }
                        header.put("statusCode", statusCode);
                        header.put("statusMessage", statusMessage);
                        header.put("headers", toJson(responseHeaders));
                        Buffer bufferHeader = Buffer.buffer(header.encode());
                        Buffer response = Buffer.buffer(4 + bufferHeader.length() + responsePayload.length());
                        response.setInt(0, bufferHeader.length()).appendBuffer(bufferHeader).appendBuffer(responsePayload);
                        message.reply(response);
                        return Future.succeededFuture();
                    }

                    @Override
                    public void end(Handler<AsyncResult<Void>> handler) {
                        end().onComplete(handler);
                    }

                    @Override
                    public Future<Void> sendFile(String filename, long offset, long length) {
                        throw new UnsupportedOperationException();
                    }


                    @Override
                    public HttpServerResponse sendFile(String s, Handler<AsyncResult<Void>> asyncResultHandler) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public HttpServerResponse sendFile(String filename, long offset, Handler<AsyncResult<Void>> resultHandler) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public HttpServerResponse sendFile(String filename, long offset, long length, Handler<AsyncResult<Void>> resultHandler) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void close() {
                    }

                    @Override
                    public boolean ended() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public boolean closed() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public boolean headWritten() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public HttpServerResponse headersEndHandler(Handler<Void> handler) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public HttpServerResponse bodyEndHandler(Handler<Void> handler) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public long bytesWritten() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public int streamId() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public HttpServerResponse push(HttpMethod method, String host, String path, Handler<AsyncResult<HttpServerResponse>> handler) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public HttpServerResponse push(HttpMethod method, String path, MultiMap headers, Handler<AsyncResult<HttpServerResponse>> handler) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public HttpServerResponse push(HttpMethod method, String path, Handler<AsyncResult<HttpServerResponse>> handler) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public HttpServerResponse push(HttpMethod method, String host, String path, MultiMap headers, Handler<AsyncResult<HttpServerResponse>> handler) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Future<HttpServerResponse> push(HttpMethod method, HostAndPort authority, String path, MultiMap headers) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Future<HttpServerResponse> push(HttpMethod method, String host, String path, MultiMap headers) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public boolean reset(long code) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public HttpServerResponse writeCustomFrame(int type, int flags, Buffer payload) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public HttpServerResponse addCookie(Cookie cookie) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Cookie removeCookie(String name, boolean invalidate) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Set<Cookie> removeCookies(String name, boolean invalidate) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Cookie removeCookie(String name, String domain, String path, boolean invalidate) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public HttpServerResponse setWriteQueueMaxSize(int i) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public boolean writeQueueFull() {
                        return false;
                    }

                    @Override
                    public HttpServerResponse drainHandler(Handler<Void> voidHandler) {
                        log.warn("I wish you a happy timeout as this method ignores drainHandler anyway.",
                                new Exception("may this stacktrace help you"));
                        return this;
                    }

                    @Override
                    public HttpServerResponse exceptionHandler(Handler<Throwable> throwableHandler) {
                        log.warn("I wish you a happy debugging session as this method ignores exceptionHandler anyway.",
                                new Exception("may this stacktrace help you"));
                        return this;
                    }
                };
            }
            return response;
        }

        @Override
        public MultiMap headers() {
            return requestHeaders;
        }

        @Override
        public String getHeader(String headerName) {
            return requestHeaders.get(headerName);
        }

        @Override
        public String getHeader(CharSequence headerName) {
            return requestHeaders.get(headerName);
        }

        @Override
        public HttpServerRequest setParamsCharset(String charset) {
            Objects.requireNonNull(charset, "Charset must not be null");
            Charset current = paramsCharset;
            paramsCharset = Charset.forName(charset);
            if (!paramsCharset.equals(current)) {
                params = null;
            }
            return this;
        }

        @Override
        public String getParamsCharset() {
            return paramsCharset.name();
        }

        @Override
        public MultiMap params() {
            if (params == null) {
                QueryStringDecoder queryStringDecoder = new QueryStringDecoder(uri(), paramsCharset);
                Map<String, List<String>> prms = queryStringDecoder.parameters();
                params = new HeadersMultiMap();
                if (!prms.isEmpty()) {
                    for (Map.Entry<String, List<String>> entry : prms.entrySet()) {
                        params.add(entry.getKey(), entry.getValue());
                    }
                }
            }
            return params;
        }

        @Override
        public String getParam(String paramName) {
            return params.get(paramName);
        }

        @Override
        public SocketAddress remoteAddress() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SocketAddress localAddress() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SSLSession sslSession() {
            throw new UnsupportedOperationException();
        }

        @Override
        public X509Certificate[] peerCertificateChain() {
            return new X509Certificate[0];
        }

        @Override
        public String absoluteURI() {
            return this.uri;
        }

        @Override
        public Future<Buffer> body() {
            Promise<Buffer> promise = Promise.promise();
            this.handler(promise::complete);
            return promise.future();
        }

        @Override
        public Future<Void> end() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Future<NetSocket> toNetSocket() {
            throw new UnsupportedOperationException();
        }

        @Override
        public HttpServerRequest setExpectMultipart(boolean expect) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isExpectMultipart() {
            throw new UnsupportedOperationException();
        }

        @Override
        public HttpServerRequest uploadHandler(Handler<HttpServerFileUpload> httpServerFileUploadHandler) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MultiMap formAttributes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getFormAttribute(String attributeName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Future<ServerWebSocket> toWebSocket() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isEnded() {
            throw new UnsupportedOperationException();
        }

        @Override
        public HttpServerRequest customFrameHandler(Handler<HttpFrame> handler) {
            throw new UnsupportedOperationException();
        }

        @Override
        public HttpConnection connection() {
            throw new UnsupportedOperationException();
        }

        @Override
        public HttpServerRequest streamPriorityHandler(Handler<StreamPriority> handler) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DecoderResult decoderResult() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Cookie getCookie(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Cookie getCookie(String name, String domain, String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<Cookie> cookies(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<Cookie> cookies() {
            throw new UnsupportedOperationException();
        }

        @Override
        public HttpServerRequest endHandler(Handler<Void> voidHandler) {
            endHandler = voidHandler;
            if (requestPayload == null) {
                endHandler.handle(null);
            }
            return this;
        }

        @Override
        public HttpServerRequest pause() {
            return this;
        }

        @Override
        public HttpServerRequest resume() {
            return this;
        }

        @Override
        public HttpServerRequest fetch(long amount) {
            throw new UnsupportedOperationException();
        }

        @Override
        public HttpServerRequest exceptionHandler(Handler<Throwable> throwableHandler) {
            log.warn("I wish you happy time wasting, as this method just ignores your exceptionHandler",
                    new Exception("may this stacktrace help you"));
            return this;
        }

        @Override
        public HttpServerRequest handler(Handler<Buffer> bufferHandler) {
            if (requestPayload != null) {
                dataHandler = bufferHandler;
                vertx.runOnContext(aVoid -> {
                    if (dataHandler != null) dataHandler.handle(requestPayload);
                    if (endHandler != null) endHandler.handle(null);
                });
            }
            return this;
        }

        @Override
        public Context context() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object metric() {
            throw new UnsupportedOperationException();
        }
    }

    public static JsonArray toJson(MultiMap multiMap) {
        JsonArray result = new JsonArray();
        for (Map.Entry<String, String> entry : multiMap.entries()) {
            result.add(new JsonArray().add(entry.getKey()).add(entry.getValue()));
        }
        return result;
    }

    public static MultiMap fromJson(JsonArray json) {
        MultiMap result = new HeadersMultiMap();
        for (Object next : json) {
            if (next instanceof JsonArray) {
                JsonArray pair = (JsonArray) next;
                if (pair.size() == 2) {
                    result.add(pair.getString(0), pair.getString(1));
                }
            }
        }
        return result;
    }
}
