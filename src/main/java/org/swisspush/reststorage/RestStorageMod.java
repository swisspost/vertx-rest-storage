package org.swisspush.reststorage;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.swisspush.reststorage.util.ModuleConfiguration;

public class RestStorageMod extends AbstractVerticle {

    private Logger log = LoggerFactory.getLogger(RestStorageMod.class);

    @Override
    public void start(Promise<Void> promise) {
        ModuleConfiguration modConfig = ModuleConfiguration.fromJsonObject(config());
        log.info("Starting RestStorageMod with configuration: {}", modConfig);
        Storage storage;
        switch (modConfig.getStorageType()) {
            case filesystem:
                storage = new FileSystemStorage(vertx, modConfig.getRoot());
                break;
            case redis:
                storage = new RedisStorage(vertx, modConfig);
                break;
            default:
                throw new RuntimeException("Storage not supported: " + modConfig.getStorageType());
        }

        Handler<HttpServerRequest> handler = new RestStorageHandler(vertx, log, storage, modConfig);

        // in Vert.x 2x 100-continues was activated per default, in vert.x 3x it is off per default.
        HttpServerOptions options = new HttpServerOptions().setHandle100ContinueAutomatically(true);

        vertx.createHttpServer(options).requestHandler(handler).listen(modConfig.getPort(), result -> {
            if(result.succeeded()){
                new EventBusAdapter().init(vertx, modConfig.getStorageAddress(), handler);
                promise.complete();
            } else {
                promise.fail(result.cause());
            }
        });
    }
}
