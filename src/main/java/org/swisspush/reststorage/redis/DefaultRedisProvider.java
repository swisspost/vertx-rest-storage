package org.swisspush.reststorage.redis;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisConnection;
import io.vertx.redis.client.RedisClientType;
import io.vertx.redis.client.RedisOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.reststorage.exception.RestStorageExceptionFactory;
import org.swisspush.reststorage.util.ModuleConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
    private final AtomicBoolean connecting = new AtomicBoolean();
    private RedisConnection client;
    private RedisReadyProvider readyProvider;

    private final AtomicReference<Promise<RedisAPI>> connectPromiseRef = new AtomicReference<>();

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
        if(redisAPI == null) {
            return setupRedisClient();
        }
        if(readyProvider == null) {
            return Future.succeededFuture(redisAPI);
        }
        return readyProvider.ready(redisAPI).compose(ready -> {
            if (ready) {
                return Future.succeededFuture(redisAPI);
            }
            return Future.failedFuture("Not yet ready!");

        }, Future::failedFuture);
    }

    private boolean reconnectEnabled() {
        return configuration.getRedisReconnectAttempts() != 0;
    }

    private Future<RedisAPI> setupRedisClient() {
        Promise<RedisAPI> currentPromise = Promise.promise();
        Promise<RedisAPI> masterPromise = connectPromiseRef.accumulateAndGet(
                currentPromise, (oldVal, newVal) -> (oldVal != null) ? oldVal : newVal);
        if (currentPromise == masterPromise) {
            // Our promise is THE promise. So WE have to resolve it.
            connectToRedis().onComplete(event -> {
                connectPromiseRef.getAndSet(null);
                if (event.failed()) {
                    currentPromise.fail(event.cause());
                } else {
                    redisAPI = event.result();
                    currentPromise.complete(redisAPI);
                }
            });
        }

        // Always return master promise (even if we didn't create it ourselves)
        return masterPromise.future();
    }

    private Future<RedisAPI> connectToRedis() {
        String redisAuth = configuration.getRedisAuth();
        int redisMaxPoolSize = configuration.getMaxRedisConnectionPoolSize();
        int redisMaxPoolWaitingSize = configuration.getMaxQueueWaiting();
        int redisMaxPipelineWaitingSize = configuration.getMaxRedisWaitingHandlers();
        int redisPoolRecycleTimeoutMs = configuration.getRedisPoolRecycleTimeoutMs();

        Promise<RedisAPI> promise = Promise.promise();

        // make sure to invalidate old connection if present
        if (redis != null) {
            redis.close();
        }

        if (connecting.compareAndSet(false, true)) {
            RedisOptions redisOptions = new RedisOptions()
                    .setPassword((redisAuth == null ? "" : redisAuth))
                    .setMaxPoolSize(redisMaxPoolSize)
                    .setMaxPoolWaiting(redisMaxPoolWaitingSize)
                    .setPoolRecycleTimeout(redisPoolRecycleTimeoutMs)
                    .setMaxWaitingHandlers(redisMaxPipelineWaitingSize)
                    .setType(configuration.getRedisClientType());

            createConnectStrings().forEach(redisOptions::addConnectionString);
            redis = Redis.createClient(vertx, redisOptions);

            redis.connect().onComplete(ev -> {
                if (ev.failed()) {
                    promise.fail(exceptionFactory.newException("redis.connect() failed", ev.cause()));
                    connecting.set(false);
                    return;
                }
                var conn = ev.result();
                log.info("Successfully connected to redis");
                client = conn;

                if (configuration.getRedisClientType() == RedisClientType.STANDALONE) {
                    client.close();
                }

                // make sure the client is reconnected on error
                // eg, the underlying TCP connection is closed but the client side doesn't know it yet
                // the client tries to use the staled connection to talk to server. An exceptions will be raised
                if (reconnectEnabled()) {
                    conn.exceptionHandler(ex -> {
                        log.warn("redis connection reports problem", ex);
                        attemptReconnect(0);
                    });
                }

                // make sure the client is reconnected on connection close
                // eg, the underlying TCP connection is closed with normal 4-Way-Handshake
                // this handler will be notified instantly
                if (reconnectEnabled()) {
                    conn.endHandler(nothing -> {
                        log.warn("redis connection got closed");
                        attemptReconnect(0);
                    });
                }

                // allow further processing
                redisAPI = RedisAPI.api(conn);
                promise.complete(redisAPI);
                connecting.set(false);
            });
        } else {
            promise.complete(redisAPI);
        }

        return promise.future();
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

    private void attemptReconnect(int retry) {

        log.info("About to reconnect to redis with attempt #{}", retry);
        int reconnectAttempts = configuration.getRedisReconnectAttempts();
        if (reconnectAttempts < 0) {
            doReconnect(retry);
        } else if (retry > reconnectAttempts) {
            log.warn("Not reconnecting anymore since max reconnect attempts ({}) are reached", reconnectAttempts);
            connecting.set(false);
        } else {
            doReconnect(retry);
        }
    }

    private void doReconnect(int retry) {
        long backoffMs = (long) (Math.pow(2, Math.min(retry, 10)) * configuration.getRedisReconnectDelaySec());
        log.debug("Schedule reconnect #{} in {}ms.", retry, backoffMs);
        vertx.setTimer(backoffMs, timer -> connectToRedis()
                .onFailure(t -> attemptReconnect(retry + 1)));
    }
}
