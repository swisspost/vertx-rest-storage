package org.swisspush.reststorage;

import io.vertx.core.*;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.reststorage.exception.RestStorageExceptionFactory;
import org.swisspush.reststorage.redis.DefaultRedisProvider;
import org.swisspush.reststorage.redis.RedisProvider;
import org.swisspush.reststorage.redis.RedisStorage;
import org.swisspush.reststorage.util.ModuleConfiguration;

import static org.swisspush.reststorage.exception.RestStorageExceptionFactory.newRestStorageThriftyExceptionFactory;

public class RestStorageMod extends AbstractVerticle {

    private final Logger log = LoggerFactory.getLogger(RestStorageMod.class);

    private RedisProvider redisProvider;
    private final RestStorageExceptionFactory exceptionFactory;

    public RestStorageMod() {
        this.exceptionFactory = newRestStorageThriftyExceptionFactory();
    }

    public RestStorageMod(
        RedisProvider redisProvider,
        RestStorageExceptionFactory exceptionFactory
    ) {
        assert exceptionFactory != null;
        this.redisProvider = redisProvider;
        this.exceptionFactory = exceptionFactory;
    }

    @Override
    public void start(Promise<Void> promise) {
        ModuleConfiguration modConfig = ModuleConfiguration.fromJsonObject(config());
        log.info("Starting RestStorageMod with configuration: {}", modConfig);

        createStorage(modConfig).onComplete(event -> {
            if (event.failed()) {
                promise.fail(event.cause());
            } else {
                Handler<HttpServerRequest> handler = new RestStorageHandler(
                    vertx, log, event.result(), exceptionFactory, modConfig);

                if(modConfig.isHttpRequestHandlerEnabled()) {
                    // in Vert.x 2x 100-continues was activated per default, in vert.x 3x it is off per default.
                    HttpServerOptions options = new HttpServerOptions().setHandle100ContinueAutomatically(true);

                    vertx.createHttpServer(options).requestHandler(handler).listen(modConfig.getPort(), result -> {
                        if (result.succeeded()) {
                            new EventBusAdapter(exceptionFactory).init(vertx, modConfig.getStorageAddress(), handler);
                            promise.complete();
                        } else {
                            promise.fail(exceptionFactory.newException(
                                "vertx.HttpServer.listen(" + modConfig.getPort() + ") failed", result.cause()));
                        }
                    });
                } else {
                    new EventBusAdapter(exceptionFactory).init(vertx, modConfig.getStorageAddress(), handler);
                    promise.complete();
                }
            }
        });
    }

    private Future<Storage> createStorage(ModuleConfiguration moduleConfiguration) {
        Promise<Storage> promise = Promise.promise();

        switch (moduleConfiguration.getStorageType()) {
            case filesystem:
                promise.complete(new FileSystemStorage(vertx, exceptionFactory, moduleConfiguration.getRoot()));
                break;
            case redis:
                createRedisStorage(vertx, moduleConfiguration).onComplete(event -> {
                    if(event.succeeded()){
                        promise.complete(event.result());
                    } else {
                        promise.fail(exceptionFactory.newException("createRedisStorage() failed", event.cause()));
                    }
                });
                break;
            default:
                promise.fail(exceptionFactory.newException("Storage not supported: " + moduleConfiguration.getStorageType()));
        }

        return promise.future();
    }

    private Future<RedisStorage> createRedisStorage(Vertx vertx, ModuleConfiguration moduleConfiguration) {
        Promise<RedisStorage> initPromise = Promise.promise();

        if(redisProvider == null) {
            redisProvider = new DefaultRedisProvider(vertx, moduleConfiguration, exceptionFactory);
        }

        redisProvider.redis().onComplete(event -> {
            if(event.succeeded()) {
                initPromise.complete(new RedisStorage(vertx, moduleConfiguration, redisProvider, exceptionFactory));
            } else {
                initPromise.fail(exceptionFactory.newException("redisProvider.redis() failed", event.cause()));
            }
        });

        return initPromise.future();
    }
}
