package org.swisspush.reststorage;

import io.vertx.core.*;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisOptions;
import org.swisspush.reststorage.util.ModuleConfiguration;

public class RestStorageMod extends AbstractVerticle {

    private Logger log = LoggerFactory.getLogger(RestStorageMod.class);

    @Override
    public void start(Promise<Void> promise) {
        ModuleConfiguration modConfig = ModuleConfiguration.fromJsonObject(config());
        log.info("Starting RestStorageMod with configuration: {}", modConfig);

        createStorage(modConfig).onComplete(event -> {
            if (event.failed()) {
                promise.fail(event.cause());
            } else {
                Handler<HttpServerRequest> handler = new RestStorageHandler(vertx, log, event.result(), modConfig);

                // in Vert.x 2x 100-continues was activated per default, in vert.x 3x it is off per default.
                HttpServerOptions options = new HttpServerOptions().setHandle100ContinueAutomatically(true);

                vertx.createHttpServer(options).requestHandler(handler).listen(modConfig.getPort(), result -> {
                    if (result.succeeded()) {
                        new EventBusAdapter().init(vertx, modConfig.getStorageAddress(), handler);
                        promise.complete();
                    } else {
                        promise.fail(result.cause());
                    }
                });
            }
        });
    }

    private Future<Storage> createStorage(ModuleConfiguration moduleConfiguration) {
        Promise<Storage> promise = Promise.promise();

        switch (moduleConfiguration.getStorageType()) {
            case filesystem:
                promise.complete(new FileSystemStorage(vertx, moduleConfiguration.getRoot()));
                break;
            case redis:
                createRedisStorage(vertx, moduleConfiguration, promise);
                break;
            default:
                promise.fail(new RuntimeException("Storage not supported: " + moduleConfiguration.getStorageType()));
        }

        return promise.future();
    }

    private void createRedisStorage(Vertx vertx, ModuleConfiguration moduleConfiguration, Promise<Storage> promise) {
        Redis.createClient(vertx, new RedisOptions()
                .setConnectionString("redis://" + moduleConfiguration.getRedisHost() + ":" + moduleConfiguration.getRedisPort())
                .setPassword((moduleConfiguration.getRedisAuth() == null ? "" : moduleConfiguration.getRedisAuth()))
                .setMaxPoolSize(moduleConfiguration.getMaxRedisConnectionPoolSize())
        ).connect(redisConnectionEvent -> {
            if (redisConnectionEvent.failed()) {
                promise.fail(redisConnectionEvent.cause());
            } else {
                promise.complete(new RedisStorage(vertx, moduleConfiguration, RedisAPI.api(redisConnectionEvent.result())));
            }
        });
    }
}
