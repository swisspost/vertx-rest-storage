package org.swisspush.reststorage;

import io.vertx.core.*;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.reststorage.redis.RedisProvider;
import org.swisspush.reststorage.redis.RedisStorage;
import org.swisspush.reststorage.util.ModuleConfiguration;

public class RestStorageMod extends AbstractVerticle {

    private final Logger log = LoggerFactory.getLogger(RestStorageMod.class);

    private RedisProvider redisProvider;

    public RestStorageMod() {}

    public RestStorageMod(RedisProvider redisProvider) {
        this.redisProvider = redisProvider;
    }

    @Override
    public void start(Promise<Void> promise) {
        ModuleConfiguration modConfig = ModuleConfiguration.fromJsonObject(config());
        log.info("Starting RestStorageMod with configuration: {}", modConfig);

        createStorage(modConfig).onComplete(event -> {
            if (event.failed()) {
                promise.fail(event.cause());
            } else {
                Handler<HttpServerRequest> handler = new RestStorageHandler(vertx, log, event.result(), modConfig);

                if(modConfig.isHttpRequestHandlerEnabled()) {
                    // in Vert.x 2x 100-continues was activated per default, in vert.x 3x it is off per default.
                    HttpServerOptions options = new HttpServerOptions().setHandle100ContinueAutomatically(true);

                    vertx.createHttpServer(options).requestHandler(handler).listen(modConfig.getPort(), result -> {
                        if (result.succeeded()) {
                            new EventBusAdapter().init(vertx, modConfig.getStorageAddress(), handler);
                            promise.complete();
                        } else {
                            promise.fail(new Exception(result.cause()));
                        }
                    });
                } else {
                    new EventBusAdapter().init(vertx, modConfig.getStorageAddress(), handler);
                    promise.complete();
                }
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
                createRedisStorage(vertx, moduleConfiguration).onComplete(event -> {
                    if(event.succeeded()){
                        promise.complete(event.result());
                    } else {
                        promise.fail(new Exception(event.cause()));
                    }
                });
                break;
            default:
                promise.fail(new RuntimeException("Storage not supported: " + moduleConfiguration.getStorageType()));
        }

        return promise.future();
    }

    private Future<RedisStorage> createRedisStorage(Vertx vertx, ModuleConfiguration moduleConfiguration) {
        Promise<RedisStorage> initPromise = Promise.promise();

        if(redisProvider == null) {
            redisProvider = new DefaultRedisProvider(vertx, moduleConfiguration);
        }

        redisProvider.redis().onComplete(event -> {
            if(event.succeeded()) {
                initPromise.complete(new RedisStorage(vertx, moduleConfiguration, redisProvider));
            } else {
                initPromise.fail(new Exception(event.cause()));
            }
        });

        return initPromise.future();
    }
}
