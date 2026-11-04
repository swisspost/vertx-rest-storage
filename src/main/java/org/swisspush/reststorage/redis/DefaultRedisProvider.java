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
import java.util.concurrent.TimeUnit;

/**
 * Default implementation for a Provider for {@link RedisAPI}
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class DefaultRedisProvider implements RedisProvider {

    private static final Logger log = LoggerFactory.getLogger(DefaultRedisProvider.class);
    private static final int REQUEST_DRIVEN = -1;
    private static final long MAX_RECONNECT_BACKOFF_MS = 5 * 60 * 1000L;
    private static final long DEFAULT_PROBE_INTERVAL_MS = 10_000L;
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
    private long lastAttemptNanos = System.nanoTime();
    // Retry index of a scheduled attempt whose timer fired while a request-driven probe was in flight.
    private int deferredRetry = -1;
    // Minimum time between request-driven probes while an automatic reconnect is pending.
    volatile long probeIntervalMs = DEFAULT_PROBE_INTERVAL_MS;

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
    public Future<RedisAPI> redis() {
        RedisAPI api;
        synchronized (this) {
            api = redisAPI;
        }
        if (api == null) {
            return connectToRedis(REQUEST_DRIVEN);
        }
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

    private boolean probeDue() {
        return System.nanoTime() - lastAttemptNanos >= TimeUnit.MILLISECONDS.toNanos(probeIntervalMs);
    }

    /**
     * @param retry index of the automatic reconnect attempt, or {@link #REQUEST_DRIVEN} when triggered by a request
     */
    private Future<RedisAPI> connectToRedis(int retry) {
        Promise<RedisAPI> promise = Promise.promise();
        Throwable startFailure = null;
        synchronized (this) {
            if (connectPromise != null) {
                if (retry >= 0) {
                    // A request-driven attempt got in first; resume the automatic sequence if it fails.
                    deferredRetry = retry;
                }
                return connectPromise.future();
            }
            if (redisAPI != null) {
                // Already connected, e.g. a request-driven attempt succeeded before a timer-driven one could start.
                return Future.succeededFuture(redisAPI);
            }
            if (retry == REQUEST_DRIVEN) {
                // A pending reconnect timer is never cancelled by requests. At most one throttled probe is allowed.
                if (reconnectTimer != null && !probeDue()) {
                    return Future.failedFuture(exceptionFactory.newException("redis reconnect pending"));
                }
            }
            lastAttemptNanos = System.nanoTime();
            connectPromise = promise;
            try {
                startConnect(promise, retry);
            } catch (RuntimeException ex) {
                startFailure = ex;
                connectFailed(retry);
            }
        }
        // Complete outside the lock so callers' callbacks never run while holding it.
        if (startFailure != null) {
            promise.fail(exceptionFactory.newException("redis.connect() failed", startFailure));
        }
        return promise.future();
    }

    private void startConnect(Promise<RedisAPI> promise, int retry) {
        String redisAuth = configuration.getRedisAuth();
        int redisMaxPoolSize = configuration.getMaxRedisConnectionPoolSize();
        int redisMaxPoolWaitingSize = configuration.getMaxQueueWaiting();
        int redisMaxPipelineWaitingSize = configuration.getMaxRedisWaitingHandlers();
        int redisPoolRecycleTimeoutMs = configuration.getRedisPoolRecycleTimeoutMs();

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
            RedisAPI connected = null;
            synchronized (DefaultRedisProvider.this) {
                if (ev.succeeded()) {
                    connectPromise = null;
                    RedisConnection conn = ev.result();
                    log.info("Successfully connected to redis");
                    client = conn;
                    conn.exceptionHandler(ex -> connectionLost(conn, ex));
                    conn.endHandler(nothing -> connectionLost(conn, null));
                    redisAPI = RedisAPI.api(conn);
                    reconnecting = false;
                    deferredRetry = -1;
                    if (reconnectTimer != null) {
                        // A successful probe makes the pending automatic retry obsolete.
                        vertx.cancelTimer(reconnectTimer);
                        reconnectTimer = null;
                    }
                    connected = redisAPI;
                } else {
                    if (retry >= 0 && reconnecting && reconnectEnabled()) {
                        log.warn("redis reconnect attempt #{} failed", retry, ev.cause());
                    }
                    connectFailed(retry);
                }
            }
            // Complete outside the lock so callers' callbacks never run while holding it.
            if (connected != null) {
                promise.complete(connected);
            } else {
                promise.fail(exceptionFactory.newException("redis.connect() failed", ev.cause()));
            }
        });
    }

    private synchronized void connectFailed(int retry) {
        connectPromise = null;
        if (redis != null) {
            redis.close();
            redis = null;
        }
        // Only automatic attempts continue the retry sequence; request-driven ones must not re-arm it.
        if (retry >= 0 && reconnecting && reconnectEnabled()) {
            attemptReconnect(retry + 1);
        } else if (retry < 0 && deferredRetry >= 0) {
            // The scheduled timer fired during this failed probe; continue its retry sequence.
            int resume = deferredRetry;
            deferredRetry = -1;
            if (reconnecting && reconnectEnabled()) {
                attemptReconnect(resume);
            }
        }
    }

    private synchronized void connectionLost(RedisConnection connection, Throwable cause) {
        // Ignore duplicate notifications and notifications from a superseded connection.
        if (client != connection) {
            return;
        }
        client = null;
        redisAPI = null;
        reconnecting = true;
        lastAttemptNanos = System.nanoTime();
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
        long baseMs = configuration.getRedisReconnectDelaySec() * 1000L;
        long backoffMs = Math.min((1L << Math.min(retry, 10)) * baseMs, Math.max(MAX_RECONNECT_BACKOFF_MS, baseMs));
        log.debug("Schedule reconnect #{} in {}ms.", retry, backoffMs);
        reconnectTimer = vertx.setTimer(backoffMs, timer -> {
            synchronized (DefaultRedisProvider.this) {
                if (!timer.equals(reconnectTimer)) {
                    return;
                }
                reconnectTimer = null;
                if (!reconnectEnabled() || client != null) {
                    return;
                }
                if (connectPromise != null) {
                    deferredRetry = retry;
                    return;
                }
            }
            connectToRedis(retry);
        });
    }
}
