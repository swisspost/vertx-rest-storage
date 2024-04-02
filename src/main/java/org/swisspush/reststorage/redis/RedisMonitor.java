package org.swisspush.reststorage.redis;

import com.google.common.base.Splitter;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static java.util.Collections.emptyList;

public class RedisMonitor {
    private final Vertx vertx;
    private final RedisProvider redisProvider;
    private final int periodMs;
    private final String expirableKey;
    private long timer;
    private final Logger log = LoggerFactory.getLogger(RedisMonitor.class);

    private static final String DELIMITER = ":";

    private final RedisMetricsPublisher publisher;

    /**
     * @param vertx         vertx
     * @param redisProvider RedisProvider
     * @param monitoringAddress The EventBus address to send metrics to
     * @param name          name used in the metrics EventBus message
     * @param expirableKey  name of the expirable resources entry
     * @param periodSec     period in seconds to gather redis metrics
     */
    public RedisMonitor(Vertx vertx, RedisProvider redisProvider, String monitoringAddress, String name, String expirableKey, int periodSec) {
        this(vertx, redisProvider, expirableKey, periodSec,
                new EventBusRedisMetricsPublisher(vertx, monitoringAddress, "redis." + name + ".")
        );
    }

    public RedisMonitor(Vertx vertx, RedisProvider redisProvider, String expirableKey, int periodSec, RedisMetricsPublisher publisher) {
        this.vertx = vertx;
        this.redisProvider = redisProvider;
        this.expirableKey = expirableKey;
        this.periodMs = periodSec * 1000;
        this.publisher = publisher;
    }

    public void start() {
        timer = vertx.setPeriodic(periodMs, timer -> redisProvider.redis().onSuccess(redisAPI -> {
            redisAPI.info(emptyList()).onComplete(event -> {
                if (event.succeeded()) {
                    collectMetrics(event.result().toBuffer());
                } else {
                    log.warn("Cannot collect INFO from redis", event.cause());
                }
            });

            redisAPI.zcard(expirableKey, reply -> {
                if (reply.succeeded()) {
                    long value = reply.result().toLong();
                    publisher.publishMetric("expirable", value);
                } else {
                    log.warn("Cannot collect zcard from redis for key {}", expirableKey, reply.cause());
                }
            });

        }).onFailure(throwable -> log.warn("Cannot collect INFO from redis", throwable)));
    }

    public void stop() {
        if (timer != 0) {
            vertx.cancelTimer(timer);
            timer = 0;
        }
    }

    private void collectMetrics(Buffer buffer) {
        Map<String, String> map = new HashMap<>();

        Splitter.on(System.lineSeparator()).omitEmptyStrings()
                .trimResults().splitToList(buffer.toString()).stream()
                .filter(input -> input != null && input.contains(DELIMITER)
                        && !input.contains("executable")
                        && !input.contains("config_file")).forEach(entry -> {
                    List<String> keyValue = Splitter.on(DELIMITER).omitEmptyStrings().trimResults().splitToList(entry);
                    if (keyValue.size() == 2) {
                        map.put(keyValue.get(0), keyValue.get(1));
                    }
                });

        log.debug("got redis metrics {}", map);

        map.forEach((key, valueStr) -> {
            long value;
            try {
                if (key.startsWith("db")) {
                    String[] pairs = valueStr.split(",");
                    for (String pair : pairs) {
                        String[] tokens = pair.split("=");
                        if (tokens.length == 2) {
                            value = Long.parseLong(tokens[1]);
                            publisher.publishMetric("keyspace." + key + "." + tokens[0], value);
                        } else {
                            log.warn("Invalid keyspace property. Will be ignored");
                        }
                    }
                } else if (key.contains("_cpu_")) {
                    value = (long) (Double.parseDouble(valueStr) * 1000.0);
                    publisher.publishMetric(key, value);
                } else if (key.contains("fragmentation_ratio")) {
                    value = (long) (Double.parseDouble(valueStr));
                    publisher.publishMetric(key, value);
                } else {
                    value = Long.parseLong(valueStr);
                    publisher.publishMetric(key, value);
                }
            } catch (NumberFormatException e) {
                log.warn("ignore field '{}' because '{}' doesnt look number-ish enough", key, valueStr);
            }
        });
    }
}
