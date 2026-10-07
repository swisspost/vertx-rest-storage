package org.swisspush.reststorage.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.redis.client.RedisAPI;
import org.swisspush.reststorage.exception.RestStorageExceptionFactory;

import java.util.Collections;

public final class RedisVersion {
    private final String value;
    private final int major;
    private final int minor;
    private final int patch;

    private RedisVersion(String value) {
        if (!value.matches("\\d+\\.\\d+\\.\\d+(?:[-+].*)?")) {
            throw new IllegalArgumentException("Invalid redis_version in INFO server: " + value);
        }
        this.value = value;
        String[] parts = value.split("[-+]", 2)[0].split("\\.");
        this.major = Integer.parseInt(parts[0]);
        this.minor = Integer.parseInt(parts[1]);
        this.patch = Integer.parseInt(parts[2]);
    }

    public static Future<RedisVersion> readRedisVersion(RedisAPI redisAPI, RestStorageExceptionFactory exceptionFactory) {
        Promise<RedisVersion> promise = Promise.promise();
        redisAPI.info(Collections.singletonList("server"), event -> {
            if (event.failed()) {
                promise.fail(exceptionFactory.newException("redisAPI.info([\"server\"]) failed", event.cause()));
                return;
            }
            if (event.result() == null) {
                promise.fail("Redis INFO server returned no response");
                return;
            }
            for (String line : event.result().toString().split("\\r?\\n")) {
                if (line.startsWith("redis_version:")) {
                    String version = line.substring("redis_version:".length()).trim();
                    try {
                        promise.complete(new RedisVersion(version));
                    } catch (IllegalArgumentException ex) {
                        promise.fail(exceptionFactory.newException("Invalid redis_version in INFO server: " + version, ex));
                    }
                    return;
                }
            }
            promise.fail("Redis INFO server is missing redis_version");
        });
        return promise.future();
    }

    public boolean isAtLeast(int major, int minor, int patch) {
        if (this.major != major) {
            return this.major > major;
        }
        if (this.minor != minor) {
            return this.minor > minor;
        }
        return this.patch >= patch;
    }

    @Override
    public String toString() {
        return value;
    }
}
