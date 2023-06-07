package org.swisspush.reststorage;

import io.vertx.core.Future;
import io.vertx.redis.client.RedisAPI;

/**
 * Provider for {@link RedisAPI}
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public interface RedisProvider {

    Future<RedisAPI> redis();
}
