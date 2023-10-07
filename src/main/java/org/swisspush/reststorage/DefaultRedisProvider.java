package org.swisspush.reststorage;

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
import org.swisspush.reststorage.redis.RedisProvider;
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

    private RedisAPI redisAPI;
    private Redis redis;
    private final AtomicBoolean connecting = new AtomicBoolean();
    private RedisConnection client;

    private final AtomicReference<Promise<RedisAPI>> connectPromiseRef = new AtomicReference<>();

    public DefaultRedisProvider(Vertx vertx, ModuleConfiguration configuration) {
        this.vertx = vertx;
        this.configuration = configuration;
    }

    @Override
    public Future<RedisAPI> redis() {
        if (redisAPI != null) {
            return Future.succeededFuture(redisAPI);
        } else {
            return setupRedisClient();
        }
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

            redis.connect().onSuccess(conn -> {
                log.info("Successfully connected to redis");
                client = conn;

                if (configuration.getRedisClientType() == RedisClientType.STANDALONE) {
                    // don't do this in cluster mode!!!
                    client.close();
                }

                // make sure the client is reconnected on error
                // eg, the underlying TCP connection is closed but the client side doesn't know it yet
                // the client tries to use the staled connection to talk to server. An exceptions will be raised
                if (reconnectEnabled()) {
                    conn.exceptionHandler(e -> attemptReconnect(0));
                }

                // make sure the client is reconnected on connection close
                // eg, the underlying TCP connection is closed with normal 4-Way-Handshake
                // this handler will be notified instantly
                if (reconnectEnabled()) {
                    conn.endHandler(placeHolder -> attemptReconnect(0));
                }

                // allow further processing
                redisAPI = RedisAPI.api(conn);
                promise.complete(redisAPI);
                connecting.set(false);
            }).onFailure(t -> {
                promise.fail(t);
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
