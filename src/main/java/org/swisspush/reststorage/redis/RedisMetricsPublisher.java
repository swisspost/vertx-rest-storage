package org.swisspush.reststorage.redis;

public interface RedisMetricsPublisher {

    void publishMetric(String name, long value);
}
