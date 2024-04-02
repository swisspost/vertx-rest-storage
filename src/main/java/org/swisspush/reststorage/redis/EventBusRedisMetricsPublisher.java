package org.swisspush.reststorage.redis;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

public class EventBusRedisMetricsPublisher implements RedisMetricsPublisher {

    private final Vertx vertx;
    private final String monitoringAddress;
    private final String prefix;

    public EventBusRedisMetricsPublisher(Vertx vertx, String monitoringAddress, String prefix) {
        this.vertx = vertx;
        this.monitoringAddress = monitoringAddress;
        this.prefix = prefix;
    }

    @Override
    public void publishMetric(String name, long value) {
        vertx.eventBus().publish(monitoringAddress,
                new JsonObject().put("name", prefix + name).put("action", "set").put("n", value));
    }
}
