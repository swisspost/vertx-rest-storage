package org.swisspush.reststorage.redis;

import io.vertx.core.Future;
import io.vertx.redis.client.RedisAPI;

/**
 * Provides the "ready state" of the Redis database. The connection to Redis may be already established, but Redis is not
 * yet ready to be used
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public interface RedisReadyProvider {

    /**
     * Get the "ready state" of the Redis database.
     *
     * @param redisAPI API to access redis database
     * @return An async boolean true when Redis can be used. Returns async boolean false otherwise or in case of an error
     */
    Future<Boolean> ready(RedisAPI redisAPI);
}
