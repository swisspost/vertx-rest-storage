package org.swisspush.reststorage.redis;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.RedisAPI;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.swisspush.reststorage.exception.RestStorageExceptionFactory;
import org.swisspush.reststorage.util.ModuleConfiguration;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.swisspush.reststorage.exception.RestStorageExceptionFactory.newRestStorageWastefulExceptionFactory;

public class DefaultRedisProviderRestartTest {

    private Vertx vertx;
    private NetServer server;
    private final AtomicReference<NetSocket> socket = new AtomicReference<>();
    private final AtomicBoolean respondToHello = new AtomicBoolean(true);
    private final AtomicBoolean respondToPing = new AtomicBoolean(true);
    private final AtomicBoolean refuseHello = new AtomicBoolean();
    private final AtomicBoolean helloReceived = new AtomicBoolean();
    private final AtomicBoolean pingReceived = new AtomicBoolean();
    private final AtomicInteger connectionCount = new AtomicInteger();
    private final AtomicInteger openConnections = new AtomicInteger();
    private final List<NetSocket> sockets = new CopyOnWriteArrayList<>();
    private ModuleConfiguration configuration;

    @Before
    public void setUp() throws Exception {
        vertx = Vertx.vertx();
        server = startServer(0);
    }

    @After
    public void tearDown() throws Exception {
        if (configuration != null) {
            configuration.redisReconnectAttempts(0);
        }
        get(vertx.close());
    }

    @Test(timeout = 15000)
    public void reconnectsWhenRedisRestartsAfterReconnectAttemptsAreExhausted() throws Exception {
        DefaultRedisProvider provider = provider(1);
        RedisAPI oldApi = get(provider.redis());
        assertEquals("PONG", get(oldApi.send(Command.PING)).toString());
        NetSocket oldSocket = socket.get();

        int port = server.actualPort();
        get(server.close());
        get(oldSocket.close());

        // Keep Redis down past the finite automatic retry window.
        Thread.sleep(4000);
        server = startServer(port);

        RedisAPI apiAfterRestart = get(provider.redis());
        assertNotSame(oldApi, apiAfterRestart);
        assertEquals("PONG", get(apiAfterRestart.send(Command.PING)).toString());
    }

    @Test(timeout = 15000)
    public void callersDuringReconnectWaitForReplacementConnection() throws Exception {
        DefaultRedisProvider provider = provider(-1);
        RedisAPI oldApi = get(provider.redis());
        assertEquals("PONG", get(oldApi.send(Command.PING)).toString());
        respondToHello.set(false);
        helloReceived.set(false);
        get(socket.get().close());
        await().atMost(5, TimeUnit.SECONDS).untilTrue(helloReceived);

        Future<RedisAPI> pending = provider.redis();
        assertFalse(pending.isComplete());
        for (int i = 0; i < 16; i++) {
            assertSame(pending, provider.redis());
        }
        socket.get().write("%1\r\n$5\r\nproto\r\n:3\r\n");
        RedisAPI newApi = get(pending);
        assertNotSame(oldApi, newApi);
        assertEquals("PONG", get(newApi.send(Command.PING)).toString());
        assertEquals(2, connectionCount.get());
    }

    @Test(timeout = 15000)
    public void respectsReconnectDelayAndAttemptLimit() throws Exception {
        DefaultRedisProvider provider = provider(1);
        RedisAPI oldApi = get(provider.redis());
        assertEquals("PONG", get(oldApi.send(Command.PING)).toString());
        refuseHello.set(true);
        long disconnectedAt = System.nanoTime();
        get(socket.get().close());
        await().atMost(5, TimeUnit.SECONDS).until(() -> connectionCount.get() == 2);
        assertTrue("Reconnect delay must be in seconds, not milliseconds",
                System.nanoTime() - disconnectedAt >= TimeUnit.MILLISECONDS.toNanos(800));
        Thread.sleep(2500);
        assertEquals("Only one automatic reconnect attempt is configured", 2, connectionCount.get());

        refuseHello.set(false);
        assertEquals("PONG", get(get(provider.redis()).send(Command.PING)).toString());
    }

    @Test(timeout = 15000)
    public void requestsDuringReconnectBackoffDoNotStartEarlyConnection() throws Exception {
        DefaultRedisProvider provider = provider(3);
        configuration.redisReconnectDelaySec(30);
        RedisAPI oldApi = get(provider.redis());
        assertEquals("PONG", get(oldApi.send(Command.PING)).toString());

        refuseHello.set(true);
        get(socket.get().close());
        await().atMost(5, TimeUnit.SECONDS).until(() -> reconnectTimer(provider) != null);
        Long pendingTimer = reconnectTimer(provider);
        assertNotNull("A reconnect timer must be pending before issuing requests", pendingTimer);
        assertEquals(1, connectionCount.get());

        for (int i = 0; i < 4; i++) {
            provider.redis();
        }

        Thread.sleep(500);
        assertEquals("Requests during reconnect backoff must not initiate an early TCP connection",
                1, connectionCount.get());
        assertEquals("Requests must not cancel the pending reconnect timer",
                pendingTimer, reconnectTimer(provider));
    }

    @Test(timeout = 15000)
    public void throttledProbeDuringBackoffRecoversWithoutDisturbingPendingTimer() throws Exception {
        DefaultRedisProvider provider = provider(3);
        configuration.redisReconnectDelaySec(30);
        provider.probeIntervalMs = 1000;
        get(provider.redis());

        refuseHello.set(true);
        get(socket.get().close());
        await().atMost(5, TimeUnit.SECONDS).until(() -> reconnectTimer(provider) != null);
        Long pendingTimer = reconnectTimer(provider);

        Thread.sleep(1200);
        // Failed probe: one connection attempt, pending timer untouched.
        assertTrue(provider.redis().toCompletionStage().toCompletableFuture()
                .handle((r, e) -> e != null).get(5, TimeUnit.SECONDS));
        assertEquals(2, connectionCount.get());
        assertEquals(pendingTimer, reconnectTimer(provider));

        // Throttled: an immediate further request does not connect.
        provider.redis();
        Thread.sleep(300);
        assertEquals(2, connectionCount.get());

        refuseHello.set(false);
        Thread.sleep(1000);
        RedisAPI api = get(provider.redis());
        assertEquals("PONG", get(api.send(Command.PING)).toString());
        assertEquals(3, connectionCount.get());
        assertEquals("Successful probe must cancel the pending timer", null, reconnectTimer(provider));
    }

    @Test(timeout = 15000)
    public void automaticRetryResumesWhenRequestDrivenAttemptWinsTheRace() throws Exception {
        DefaultRedisProvider provider = provider(0);
        get(provider.redis());
        get(socket.get().close());
        awaitConnectionLost(provider);

        // Request-driven attempt in flight (no HELLO answer), then the timer-driven attempt arrives.
        configuration.redisReconnectAttempts(3);
        respondToHello.set(false);
        Future<RedisAPI> inFlight = provider.redis();
        await().atMost(5, TimeUnit.SECONDS).until(() -> connectionCount.get() == 2);
        Method connectToRedis = DefaultRedisProvider.class.getDeclaredMethod("connectToRedis", int.class);
        connectToRedis.setAccessible(true);
        assertSame(inFlight, connectToRedis.invoke(provider, 0));

        get(socket.get().close());
        await().atMost(5, TimeUnit.SECONDS).until(() -> reconnectTimer(provider) != null);
    }

    /**
     * Combined scenario test: many threads hammer {@code redis()} while Redis suffers three outages in a row
     * (handshake refused, server down, handshake refused). It verifies, in one run, that the provider
     * <ul>
     *   <li>does not open a connection storm during an outage (backoff + throttled probes are respected),</li>
     *   <li>fails requests fast while a reconnect is pending instead of hanging them,</li>
     *   <li>recovers after every outage without any manual intervention,</li>
     *   <li>never invokes caller callbacks while holding its internal lock,</li>
     *   <li>does not leak connections (exactly one live connection at the end), and</li>
     *   <li>leaves no pending reconnect timer once the connection is healthy again.</li>
     * </ul>
     */
    @Test(timeout = 60000)
    public void survivesRepeatedOutagesUnderLoad() throws Exception {
        // Unlimited automatic retries (1s base delay) plus a short probe interval, so that
        // request-driven probes are allowed every 500ms while a retry timer is pending.
        DefaultRedisProvider provider = provider(-1);
        provider.probeIntervalMs = 500;
        // Establish the initial healthy connection.
        get(get(provider.redis()).send(Command.PING));

        // ---- Load generation: many threads issue requests continuously for the whole test ----
        int workers = 24;
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        // Counts callbacks that ran on a thread holding the provider's monitor (must stay 0).
        AtomicInteger callbacksUnderLock = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        for (int i = 0; i < workers; i++) {
            pool.submit(() -> {
                while (running.get()) {
                    provider.redis().onComplete(ar -> {
                        if (Thread.holdsLock(provider)) {
                            callbacksUnderLock.incrementAndGet();
                        }
                        if (ar.succeeded()) {
                            successes.incrementAndGet();
                        } else {
                            failures.incrementAndGet();
                        }
                    });
                    try {
                        // Small pause to avoid a hot spin; still thousands of requests per outage.
                        Thread.sleep(2);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            });
        }

        try {
            for (int cycle = 0; cycle < 3; cycle++) {
                int connectionsBeforeOutage = connectionCount.get();
                int port = server.actualPort();

                // ---- Outage: cycles 0 and 2 refuse the handshake, cycle 1 takes the whole server down ----
                if (cycle == 1) {
                    // Redis down: new TCP connections are refused outright.
                    get(server.close());
                } else {
                    // Redis reachable but every HELLO handshake is rejected by closing the socket.
                    refuseHello.set(true);
                }
                // Kill all established connections so the provider notices the loss.
                new ArrayList<>(sockets).forEach(s -> s.close());
                Thread.sleep(2500);

                // With a 1s base delay, 500ms probe interval and a 2.5s outage, only a handful of automatic
                // retries and probes are legitimate. Without throttling, 24 busy threads would open hundreds.
                int attemptsDuringOutage = connectionCount.get() - connectionsBeforeOutage;
                assertTrue("Cycle " + cycle + ": connection storm during outage (" + attemptsDuringOutage + " attempts)",
                        attemptsDuringOutage <= 12);

                // ---- Recovery: bring Redis back; no manual reconnect, only the provider's own mechanisms ----
                if (cycle == 1) {
                    server = startServer(port);
                }
                refuseHello.set(false);

                // Requests must succeed again (via a probe or the scheduled retry) shortly after recovery.
                int successesBefore = successes.get();
                await().atMost(10, TimeUnit.SECONDS).until(() -> successes.get() > successesBefore + 50);
                // The recovered connection must actually be usable, not just handed out.
                assertEquals("PONG", get(get(provider.redis()).send(Command.PING)).toString());
            }
        } finally {
            running.set(false);
            pool.shutdown();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        }

        // ---- Final invariants ----
        assertEquals("Callbacks must never run while holding the provider lock", 0, callbacksUnderLock.get());
        // Proves the provider failed requests fast during outages rather than blocking or hanging them.
        assertTrue("Requests during outages must have failed fast", failures.get() > 0);
        // Leak check: all clients created for failed attempts/probes were closed; only the live one remains.
        await().atMost(5, TimeUnit.SECONDS).until(() -> openConnections.get() == 1);
        // A healthy connection must not leave a stale reconnect timer behind.
        await().atMost(5, TimeUnit.SECONDS).until(() -> reconnectTimer(provider) == null);
    }

    @Test(timeout = 15000)
    public void disabledAutomaticReconnectStillAllowsRequestDrivenRecovery() throws Exception {
        DefaultRedisProvider provider = provider(0);
        RedisAPI oldApi = get(provider.redis());
        assertEquals("PONG", get(oldApi.send(Command.PING)).toString());
        NetSocket oldSocket = socket.get();
        int port = server.actualPort();
        get(server.close());
        get(oldSocket.close());
        server = startServer(port);
        Thread.sleep(1500);
        assertEquals(1, connectionCount.get());
        RedisAPI newApi = get(provider.redis());
        assertNotSame(oldApi, newApi);
        assertEquals("PONG", get(newApi.send(Command.PING)).toString());
    }

    @Test(timeout = 15000)
    public void reconnectBackoffIsCapped() throws Exception {
        Vertx mockedVertx = mock(Vertx.class);
        configuration = new ModuleConfiguration()
                .redisReconnectAttempts(-1)
                .redisReconnectDelaySec(30)
                .redisReadyCheckIntervalMs(0);
        DefaultRedisProvider provider = new DefaultRedisProvider(
                mockedVertx, configuration, newRestStorageWastefulExceptionFactory());

        Method attemptReconnect = DefaultRedisProvider.class.getDeclaredMethod("attemptReconnect", int.class);
        attemptReconnect.setAccessible(true);
        attemptReconnect.invoke(provider, 10);

        ArgumentCaptor<Long> delayMs = ArgumentCaptor.forClass(Long.class);
        verify(mockedVertx).setTimer(delayMs.capture(), any(Handler.class));
        assertTrue("Backoff must be capped (was " + delayMs.getValue() + "ms)",
                delayMs.getValue() <= TimeUnit.MINUTES.toMillis(5));
    }

    @Test(timeout = 20000)
    public void requestDrivenFailureDoesNotRestartExhaustedAutomaticReconnect() throws Exception {
        DefaultRedisProvider provider = provider(2);
        get(provider.redis());
        refuseHello.set(true);
        get(socket.get().close());

        // initial connection + 2 automatic attempts (after 1s and 2s)
        await().atMost(8, TimeUnit.SECONDS).until(() -> connectionCount.get() == 3);
        Thread.sleep(500);

        provider.redis();
        await().atMost(2, TimeUnit.SECONDS).until(() -> connectionCount.get() == 4);
        Thread.sleep(3500);
        assertEquals("A failed request-driven attempt must not re-arm automatic retries",
                4, connectionCount.get());
    }

    @Test(timeout = 15000)
    public void connectCallbacksAreNotInvokedWhileHoldingProviderLock() throws Exception {
        DefaultRedisProvider provider = provider(0);
        get(provider.redis());
        refuseHello.set(true);
        get(socket.get().close());
        awaitConnectionLost(provider);

        AtomicBoolean lockHeld = new AtomicBoolean();
        AtomicBoolean called = new AtomicBoolean();
        provider.redis().onComplete(ar -> {
            lockHeld.set(Thread.holdsLock(provider));
            called.set(true);
        });
        await().atMost(5, TimeUnit.SECONDS).untilTrue(called);
        assertFalse("Callbacks must not run while holding the provider lock", lockHeld.get());
    }

    @Test(timeout = 15000)
    public void failedConnectClosesRedisClient() throws Exception {
        DefaultRedisProvider provider = provider(0);
        get(provider.redis());
        refuseHello.set(true);
        get(socket.get().close());
        awaitConnectionLost(provider);

        AtomicBoolean called = new AtomicBoolean();
        provider.redis().onComplete(ar -> called.set(true));
        await().atMost(5, TimeUnit.SECONDS).untilTrue(called);

        Field field = DefaultRedisProvider.class.getDeclaredField("redis");
        field.setAccessible(true);
        synchronized (provider) {
            assertNull("Redis client of a failed connect must be closed and released", field.get(provider));
        }
    }

    private DefaultRedisProvider provider(int reconnectAttempts) {
        configuration = new ModuleConfiguration()
                .redisHost("127.0.0.1")
                .redisPort(server.actualPort())
                .redisReconnectAttempts(reconnectAttempts)
                .redisReconnectDelaySec(1)
                .redisPoolRecycleTimeoutMs(-1)
                .redisReadyCheckIntervalMs(0);
        RestStorageExceptionFactory exceptionFactory = newRestStorageWastefulExceptionFactory();
        return new DefaultRedisProvider(vertx, configuration, exceptionFactory);
    }

    private void awaitConnectionLost(DefaultRedisProvider provider) {
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            Field field = DefaultRedisProvider.class.getDeclaredField("redisAPI");
            field.setAccessible(true);
            synchronized (provider) {
                return field.get(provider) == null;
            }
        });
    }

    private Long reconnectTimer(DefaultRedisProvider provider) {
        try {
            Field field = DefaultRedisProvider.class.getDeclaredField("reconnectTimer");
            field.setAccessible(true);
            synchronized (provider) {
                return (Long) field.get(provider);
            }
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private NetServer startServer(int port) throws Exception {
        return get(vertx.createNetServer().connectHandler(connection -> {
            connectionCount.incrementAndGet();
            openConnections.incrementAndGet();
            sockets.add(connection);
            connection.closeHandler(v -> {
                openConnections.decrementAndGet();
                sockets.remove(connection);
            });
            socket.set(connection);
            List<String> command = new ArrayList<>();
            int[] remaining = {0};
            RecordParser parser = RecordParser.newDelimited("\r\n", connection);
            parser.handler(record -> {
                String value = record.toString();
                if (remaining[0] == 0) {
                    remaining[0] = Integer.parseInt(value.substring(1));
                } else if (!value.startsWith("$")) {
                    command.add(value);
                    if (--remaining[0] == 0) {
                        respond(connection, command.get(0));
                        command.clear();
                    }
                }
            });
        }).listen(port, "127.0.0.1"));
    }

    private void respond(NetSocket connection, String command) {
        if ("HELLO".equalsIgnoreCase(command)) {
            helloReceived.set(true);
            if (refuseHello.get()) {
                connection.close();
            } else if (respondToHello.get()) {
                connection.write("%1\r\n$5\r\nproto\r\n:3\r\n");
            }
        } else if ("PING".equalsIgnoreCase(command)) {
            pingReceived.set(true);
            if (respondToPing.get()) {
                connection.write("+PONG\r\n");
            }
        } else {
            connection.write("+OK\r\n");
        }
    }

    private static <T> T get(Future<T> future) throws Exception {
        return future.toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }
}
