package org.swisspush.reststorage.mocks;

import io.netty.channel.EventLoopGroup;
import io.vertx.core.*;
import io.vertx.core.datagram.DatagramSocket;
import io.vertx.core.datagram.DatagramSocketOptions;
import io.vertx.core.dns.DnsClient;
import io.vertx.core.dns.DnsClientOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.file.FileSystem;
import io.vertx.core.http.*;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.shareddata.SharedData;
import io.vertx.core.spi.VerticleFactory;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;


public class FailFastVertx implements Vertx {

    protected final String msg;

    public FailFastVertx() {
        this("Override this to provide your behaviour.");
    }

    public FailFastVertx(String msg) {
        this.msg = msg;
    }

    @Override
    public Context getOrCreateContext() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public NetServer createNetServer(NetServerOptions netServerOptions) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public NetServer createNetServer() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public NetClient createNetClient(NetClientOptions netClientOptions) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public NetClient createNetClient() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServer createHttpServer(HttpServerOptions httpServerOptions) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpServer createHttpServer() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public WebSocketClient createWebSocketClient(WebSocketClientOptions options) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpClientBuilder httpClientBuilder() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpClient createHttpClient(HttpClientOptions httpClientOptions) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public HttpClient createHttpClient() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public DatagramSocket createDatagramSocket(DatagramSocketOptions datagramSocketOptions) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public DatagramSocket createDatagramSocket() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public FileSystem fileSystem() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public EventBus eventBus() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public DnsClient createDnsClient(int i, String s) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public DnsClient createDnsClient() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public DnsClient createDnsClient(DnsClientOptions dnsClientOptions) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public SharedData sharedData() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Timer timer(long delay, TimeUnit unit) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public long setTimer(long l, Handler<Long> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public TimeoutStream timerStream(long l) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public long setPeriodic(long l, Handler<Long> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public long setPeriodic(long initialDelay, long delay, Handler<Long> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public TimeoutStream periodicStream(long l) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public TimeoutStream periodicStream(long initialDelay, long delay) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public boolean cancelTimer(long l) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void runOnContext(Handler<Void> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> close() {
        throw new UnsupportedOperationException(msg);
    }


    @Override
    public void close(Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<String> deployVerticle(Verticle verticle) {
        throw new UnsupportedOperationException(msg);
    }


    @Override
    public void deployVerticle(Verticle verticle, Handler<AsyncResult<String>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<String> deployVerticle(Verticle verticle, DeploymentOptions options) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<String> deployVerticle(Class<? extends Verticle> verticleClass, DeploymentOptions options) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<String> deployVerticle(Supplier<Verticle> verticleSupplier, DeploymentOptions options) {
        throw new UnsupportedOperationException(msg);
    }


    @Override
    public void deployVerticle(Verticle verticle, DeploymentOptions deploymentOptions, Handler<AsyncResult<String>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void deployVerticle(Class<? extends Verticle> aClass, DeploymentOptions deploymentOptions, Handler<AsyncResult<String>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void deployVerticle(Supplier<Verticle> supplier, DeploymentOptions deploymentOptions, Handler<AsyncResult<String>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<String> deployVerticle(String name) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void deployVerticle(String s, Handler<AsyncResult<String>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<String> deployVerticle(String name, DeploymentOptions options) {
        throw new UnsupportedOperationException(msg);
    }


    @Override
    public void deployVerticle(String s, DeploymentOptions deploymentOptions, Handler<AsyncResult<String>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Future<Void> undeploy(String deploymentID) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void undeploy(String s, Handler<AsyncResult<Void>> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Set<String> deploymentIDs() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void registerVerticleFactory(VerticleFactory verticleFactory) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void unregisterVerticleFactory(VerticleFactory verticleFactory) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Set<VerticleFactory> verticleFactories() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public boolean isClustered() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public <T> void executeBlocking(Handler<Promise<T>> blockingCodeHandler, boolean ordered, Handler<AsyncResult<T>> asyncResultHandler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public <T> void executeBlocking(Handler<Promise<T>> blockingCodeHandler, Handler<AsyncResult<T>> asyncResultHandler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public <T> Future<T> executeBlocking(Handler<Promise<T>> blockingCodeHandler, boolean ordered) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public <T> Future<T> executeBlocking(Handler<Promise<T>> blockingCodeHandler) {
        throw new UnsupportedOperationException(msg);
    }


    @Override
    public EventLoopGroup nettyEventLoopGroup() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public WorkerExecutor createSharedWorkerExecutor(String s) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public WorkerExecutor createSharedWorkerExecutor(String s, int i) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public WorkerExecutor createSharedWorkerExecutor(String s, int i, long l) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public WorkerExecutor createSharedWorkerExecutor(String name, int poolSize, long maxExecuteTime, TimeUnit maxExecuteTimeUnit) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public boolean isNativeTransportEnabled() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Throwable unavailableNativeTransportCause() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Vertx exceptionHandler(Handler<Throwable> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Handler<Throwable> exceptionHandler() {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public boolean isMetricsEnabled() {
        throw new UnsupportedOperationException(msg);
    }
}
