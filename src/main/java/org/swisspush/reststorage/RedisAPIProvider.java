package org.swisspush.reststorage;

import io.vertx.core.*;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisConnection;
import io.vertx.redis.client.RedisOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.reststorage.util.ModuleConfiguration;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Provider for {@link RedisAPI}. Handles reconnect to redis
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class RedisAPIProvider {

    private Vertx vertx;
    private final ModuleConfiguration moduleConfiguration;
    private RedisAPI redisAPI;

    private Redis redis;
    private final AtomicBoolean CONNECTING = new AtomicBoolean();
    private RedisConnection client;

    private Logger log = LoggerFactory.getLogger(RedisAPIProvider.class);

    private final int redisReconnectAttempts;
    private final int redisReconnectDelaySec;

    public RedisAPIProvider(Vertx vertx, ModuleConfiguration moduleConfiguration) {
        this.vertx = vertx;
        this.moduleConfiguration = moduleConfiguration;
        this.redisReconnectAttempts = moduleConfiguration.getRedisReconnectAttempts();
        this.redisReconnectDelaySec = moduleConfiguration.getRedisReconnectDelaySec();
    }

    public Future<RedisAPI> redisAPI() {
        if (redisAPI == null) {
            return createRedisClient();
        } else {
            return Future.succeededFuture(redisAPI);
        }
    }

    private Future<RedisAPI> createRedisClient() {
        Promise<RedisAPI> promise = Promise.promise();

        // make sure to invalidate old connection if present
        if (redis != null) {
            redis.close();
        }

        RedisOptions redisOptions = new RedisOptions()
                .setConnectionString("redis://" + moduleConfiguration.getRedisHost() + ":" + moduleConfiguration.getRedisPort())
                .setPassword((moduleConfiguration.getRedisAuth() == null ? "" : moduleConfiguration.getRedisAuth()))
                .setMaxPoolSize(moduleConfiguration.getMaxRedisConnectionPoolSize())
                .setMaxPoolWaiting(moduleConfiguration.getMaxQueueWaiting())
                .setMaxWaitingHandlers(moduleConfiguration.getMaxRedisWaitingHandlers());

        if (CONNECTING.compareAndSet(false, true)) {
            redis = Redis.createClient(vertx, redisOptions);
            redis
                    .connect()
                    .onSuccess(conn -> {
                        log.info("Successfully connected to redis");
                        client = conn;
                        client.close();

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
                        CONNECTING.set(false);
                    }).onFailure(t -> {
                        promise.fail(t);
                        CONNECTING.set(false);
                    });
        } else {
            promise.complete(redisAPI);
        }

        return promise.future();
    }

    private boolean reconnectEnabled() {
        return redisReconnectAttempts != 0;
    }

    private void attemptReconnect(int retry) {
        log.info("About to reconnect to redis with attempt #" + retry);
        if(redisReconnectAttempts < 0) {
            doReconnect(retry);
        } else if (retry > redisReconnectAttempts) {
            log.warn("Not reconnecting anymore since max reconnect attempts are reached");
            CONNECTING.set(false);
        } else {
            doReconnect(retry);
        }
    }

    private void doReconnect(int retry) {
        long backoffMs = (long) (Math.pow(2, Math.min(retry, 10)) * redisReconnectDelaySec);

        vertx.setTimer(backoffMs, timer -> createRedisClient()
                .onFailure(t -> attemptReconnect(retry + 1)));
    }
}
