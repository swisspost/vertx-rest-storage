package org.swisspush.reststorage.redis;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.RedisAPI;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.swisspush.reststorage.exception.RestStorageExceptionFactory;
import org.swisspush.reststorage.util.ModuleConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
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

    private NetServer startServer(int port) throws Exception {
        return get(vertx.createNetServer().connectHandler(connection -> {
            connectionCount.incrementAndGet();
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
