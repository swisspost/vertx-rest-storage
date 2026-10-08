package org.swisspush.reststorage.redis;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisConnection;
import io.vertx.redis.client.RedisOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.reststorage.exception.RestStorageExceptionFactory;
import org.swisspush.reststorage.util.ModuleConfiguration;

import java.util.ArrayList;
import java.util.List;

/**
 * Default implementation for a Provider for {@link RedisAPI}
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class DefaultRedisProvider implements RedisProvider {

    private static final Logger log = LoggerFactory.getLogger(DefaultRedisProvider.class);
    private final Vertx vertx;

    private final ModuleConfiguration configuration;
    private final RestStorageExceptionFactory exceptionFactory;

    private RedisAPI redisAPI;
    private Redis redis;
    private RedisConnection client;
    private RedisReadyProvider readyProvider;
    private Promise<RedisAPI> connectPromise;
    private Long reconnectTimer;
    private boolean reconnecting;

    public DefaultRedisProvider(
            Vertx vertx,
            ModuleConfiguration configuration,
            RestStorageExceptionFactory exceptionFactory
    ) {
        this.vertx = vertx;
        this.configuration = configuration;
        this.exceptionFactory = exceptionFactory;

        maybeInitRedisReadyProvider();
    }

    private void maybeInitRedisReadyProvider() {
        if (configuration.getRedisReadyCheckIntervalMs() > 0) {
            this.readyProvider = new DefaultRedisReadyProvider(vertx, configuration.getRedisReadyCheckIntervalMs());
        }
    }

    @Override
    public synchronized Future<RedisAPI> redis() {
        if (redisAPI == null) {
            return connectToRedis(0);
        }
        RedisAPI api = redisAPI;
        if (readyProvider == null) {
            return Future.succeededFuture(api);
        }
        return readyProvider.ready(api).compose(ready -> {
            if (ready) {
                return Future.succeededFuture(api);
            }
            return Future.failedFuture("Not yet ready!");
        });
    }

    private boolean reconnectEnabled() {
        return configuration.getRedisReconnectAttempts() != 0;
    }

    private synchronized Future<RedisAPI> connectToRedis(int retry) {
        if (connectPromise != null) {
            return connectPromise.future();
        }
        if (reconnectTimer != null) {
            vertx.cancelTimer(reconnectTimer);
            reconnectTimer = null;
        }
        String redisAuth = configuration.getRedisAuth();
        int redisMaxPoolSize = configuration.getMaxRedisConnectionPoolSize();
        int redisMaxPoolWaitingSize = configuration.getMaxQueueWaiting();
        int redisMaxPipelineWaitingSize = configuration.getMaxRedisWaitingHandlers();
        int redisPoolRecycleTimeoutMs = configuration.getRedisPoolRecycleTimeoutMs();

        Promise<RedisAPI> promise = Promise.promise();
        connectPromise = promise;
        try {
            if (redis != null) {
                redis.close();
                redis = null;
            }
            RedisOptions redisOptions = new RedisOptions()
                    .setPassword((redisAuth == null ? "" : redisAuth))
                    .setMaxPoolSize(redisMaxPoolSize)
                    .setMaxPoolWaiting(redisMaxPoolWaitingSize)
                    .setPoolRecycleTimeout(redisPoolRecycleTimeoutMs)
                    .setMaxWaitingHandlers(redisMaxPipelineWaitingSize)
                    .setType(configuration.getRedisClientType());

            if (configuration.isRedisEnableTls()) {
                redisOptions = redisOptions.setNetClientOptions(
                        redisOptions.getNetClientOptions()
                                .setSsl(configuration.isSsl())
                                .setTrustAll(configuration.isTrustAll())
                                .setHostnameVerificationAlgorithm(configuration.getHostnameVerificationAlgorithm())
                );
            }

            createConnectStrings().forEach(redisOptions::addConnectionString);
            redis = Redis.createClient(vertx, redisOptions);

            redis.connect().onComplete(ev -> {
                synchronized (DefaultRedisProvider.this) {
                    if (ev.failed()) {
                        connectionFailed(promise, ev.cause(), retry);
                        return;
                    }
                    connectPromise = null;
                    RedisConnection conn = ev.result();
                    log.info("Successfully connected to redis");
                    client = conn;
                    conn.exceptionHandler(ex -> connectionLost(conn, ex));
                    conn.endHandler(nothing -> connectionLost(conn, null));
                    redisAPI = RedisAPI.api(conn);
                    reconnecting = false;
                    promise.complete(redisAPI);
                }
            });
        } catch (RuntimeException ex) {
            connectionFailed(promise, ex, retry);
        }
        return promise.future();
    }

    private synchronized void connectionFailed(Promise<RedisAPI> promise, Throwable cause, int retry) {
        connectPromise = null;
        if (reconnecting && reconnectEnabled()) {
            log.warn("redis reconnect attempt #{} failed", retry, cause);
            attemptReconnect(retry + 1);
        }
        promise.fail(exceptionFactory.newException("redis.connect() failed", cause));
    }

    private synchronized void connectionLost(RedisConnection connection, Throwable cause) {
        // Ignore duplicate notifications and notifications from a superseded connection.
        if (client != connection) {
            return;
        }
        client = null;
        redisAPI = null;
        reconnecting = true;
        if (cause == null) {
            log.warn("redis connection got closed");
        } else {
            log.warn("redis connection reports problem", cause);
            connection.close();
        }
        if (reconnectEnabled()) {
            attemptReconnect(0);
        }
    }

    private List<String> createConnectStrings() {
        String redisPassword = configuration.getRedisPassword();
        String redisUser = configuration.getRedisUser();
        StringBuilder connectionStringPrefixBuilder = new StringBuilder();
        connectionStringPrefixBuilder.append(configuration.isRedisEnableTls() ? "rediss://" : "redis://");
        if (redisUser != null && !redisUser.isEmpty()) {
            connectionStringPrefixBuilder.append(redisUser).append(":").append((redisPassword == null ? "" : redisPassword)).append("@");
        }
        List<String> connectionString = new ArrayList<>();
        String connectionStringPrefix = connectionStringPrefixBuilder.toString();
        for (int i = 0; i < configuration.getRedisHosts().size(); i++) {
            String host = configuration.getRedisHosts().get(i);
            int port = configuration.getRedisPorts().get(i);
            connectionString.add(connectionStringPrefix + host + ":" + port);
        }
        return connectionString;
    }

    private synchronized void attemptReconnect(int retry) {
        if (client != null || connectPromise != null || reconnectTimer != null) {
            return;
        }
        int reconnectAttempts = configuration.getRedisReconnectAttempts();
        if (reconnectAttempts >= 0 && retry >= reconnectAttempts) {
            log.warn("Not reconnecting anymore since max reconnect attempts ({}) are reached", reconnectAttempts);
            return;
        }
        long backoffMs = (1L << Math.min(retry, 10)) * configuration.getRedisReconnectDelaySec() * 1000L;
        log.debug("Schedule reconnect #{} in {}ms.", retry, backoffMs);
        reconnectTimer = vertx.setTimer(backoffMs, timer -> {
            synchronized (DefaultRedisProvider.this) {
                if (!timer.equals(reconnectTimer)) {
                    return;
                }
                reconnectTimer = null;
                if (!reconnectEnabled()) {
                    return;
                }
                connectToRedis(retry);
            }
        });
    }
}
