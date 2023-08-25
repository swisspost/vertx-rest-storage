package org.swisspush.reststorage;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisOptions;
import org.apache.commons.lang.StringUtils;
import org.swisspush.reststorage.util.ModuleConfiguration;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Default implementation for a Provider for {@link RedisAPI}
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
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
                connectPromiseRef.getAndSet(null);
                if(event.failed()) {
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
        Promise<RedisAPI> promise = Promise.promise();
        Redis.createClient(vertx, new RedisOptions()
                .setConnectionString(createConnectString())
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

    private String createConnectString() {
        StringBuilder connectionStringBuilder = new StringBuilder();
        connectionStringBuilder.append(configuration.isRedisEnableTls() ? "rediss://" : "redis://");
        String redisUser = configuration.getRedisUser();
        if (StringUtils.isNotEmpty(redisUser)) {
            connectionStringBuilder.append(configuration.getRedisUser()).append(":").append(configuration.getRedisAuth()).append("@");
        }
        connectionStringBuilder.append(configuration.getRedisHost()).append(":").append(configuration.getRedisPort());
        return connectionStringBuilder.toString();
    }
}
