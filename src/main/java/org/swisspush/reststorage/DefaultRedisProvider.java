package org.swisspush.reststorage;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisOptions;
import org.swisspush.reststorage.util.ModuleConfiguration;

import java.util.concurrent.atomic.AtomicReference;

public class DefaultRedisProvider implements RedisProvider {

    private final Vertx vertx;

    private ModuleConfiguration configuration;

    private RedisAPI redisAPI;

    private final AtomicReference<Promise<RedisAPI>> connectPromiseRef = new AtomicReference<>();

    public DefaultRedisProvider(Vertx vertx, ModuleConfiguration configuration) {
        this.vertx = vertx;
        this.configuration = configuration;
    }

    @Override
    public Future<RedisAPI> redis() {
        if(redisAPI != null) {
            return Future.succeededFuture(redisAPI);
        } else {
            return setupRedisClient();
        }
    }

    private Future<RedisAPI> setupRedisClient(){
        Promise<RedisAPI> currentPromise = Promise.promise();
        Promise<RedisAPI> masterPromise = connectPromiseRef.accumulateAndGet(
                currentPromise, (oldVal, newVal) -> (oldVal != null) ? oldVal : newVal);
        if( currentPromise == masterPromise ){
            // Our promise is THE promise. So WE have to resolve it.
            connectToRedis().onComplete(event -> {
                if(event.failed()) {
                    currentPromise.fail(event.cause());
                } else {
                    connectPromiseRef.getAndSet(null);
                    redisAPI = event.result();
                    currentPromise.complete(redisAPI);
                }
            });
        }

        // Always return master promise (even if we didn't create it ourselves)
        return masterPromise.future();
    }

    private Future<RedisAPI> connectToRedis() {
        Promise<RedisAPI> promise = Promise.promise();
        Redis.createClient(vertx, new RedisOptions()
                .setConnectionString("redis://" + configuration.getRedisHost() + ":" + configuration.getRedisPort())
                .setPassword((configuration.getRedisAuth() == null ? "" : configuration.getRedisAuth()))
                .setMaxPoolSize(configuration.getMaxRedisConnectionPoolSize())
                .setMaxPoolWaiting(configuration.getMaxQueueWaiting())
                .setMaxWaitingHandlers(configuration.getMaxRedisWaitingHandlers())
        ).connect(event -> {
            if (event.failed()) {
                promise.fail(event.cause());
            } else {
                promise.complete(RedisAPI.api(event.result()));
            }
        });

        return promise.future();
    }
}
