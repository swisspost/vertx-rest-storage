package org.swisspush.reststorage.redis;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.redis.client.RedisAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Default implementation of the {@link RedisReadyProvider} based on the <code>INFO</code> command in Redis
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class DefaultRedisReadyProvider implements RedisReadyProvider {

    private static final Logger log = LoggerFactory.getLogger(DefaultRedisReadyProvider.class);
    private static final String DELIMITER = ":";
    private static final String LOADING = "loading";
    final AtomicBoolean redisReady = new AtomicBoolean(true);
    final AtomicBoolean updateRedisReady = new AtomicBoolean(true);

    /**
     * Constructor defining the "ready-state" update interval
     * @param vertx
     * @param updateIntervalMs interval in ms how often to update the "ready-state"
     */
    public DefaultRedisReadyProvider(Vertx vertx, int updateIntervalMs) {
        vertx.setPeriodic(updateIntervalMs, l -> {
           updateRedisReady.set(true);
        });
    }

    @Override
    public Future<Boolean> ready(RedisAPI redisAPI) {
        if(updateRedisReady.compareAndSet(true, false)){
            return updateRedisReadyState(redisAPI);
        }
        return Future.succeededFuture(redisReady.get());
    }

    /**
     * Call the <code>INFO</code> command in Redis with a constraint to persistence related information
     *
     * @param redisAPI
     * @return async boolean true when Redis is ready, otherwise false
     */
    public Future<Boolean> updateRedisReadyState(RedisAPI redisAPI) {
        return redisAPI.info(List.of("Persistence")).compose(response -> {
            boolean ready = getReadyStateFromResponse(response.toString());
            redisReady.set(ready);
            return Future.succeededFuture(ready);
        }, throwable -> {
            log.error("Error reading redis info", throwable);
            redisReady.set(false);
            return Future.succeededFuture(false);
        });
    }

    /**
     * Check the response having a <code>loading:0</code> entry. If so, Redis is ready. When the response contains a
     * <code>loading:1</code> entry or not related entry at all, we consider Redis to be not ready
     *
     * @param persistenceInfo the response from Redis _INFO_ command
     * @return boolean true when Redis is ready, otherwise false
     */
    private boolean getReadyStateFromResponse(String persistenceInfo) {
        byte loadingValue;
        try {
            Optional<String> loadingOpt = persistenceInfo
                    .lines()
                    .filter(source -> source.startsWith(LOADING + DELIMITER))
                    .findAny();
            if (loadingOpt.isEmpty()) {
                log.warn("No 'loading' section received from redis. Unable to calculate ready state");
                return false;
            }
            loadingValue = Byte.parseByte(loadingOpt.get().split(DELIMITER)[1]);
            if (loadingValue == 0) {
                return true;
            }

        } catch (NumberFormatException ex) {
            log.warn("Invalid 'loading' section received from redis. Unable to calculate ready state");
            return false;
        }

        return false;
    }
}
