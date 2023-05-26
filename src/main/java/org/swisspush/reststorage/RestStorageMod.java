package org.swisspush.reststorage;

import io.vertx.core.*;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

                if(modConfig.isHttpRequestHandlerEnabled()) {
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
                        promise.fail(event.cause());
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
        RedisAPIProvider apiProvider = new RedisAPIProvider(vertx, moduleConfiguration);
        apiProvider.redisAPI().onComplete(event -> initPromise.complete(new RedisStorage(vertx, moduleConfiguration, apiProvider)));
        return initPromise.future();
    }
}
