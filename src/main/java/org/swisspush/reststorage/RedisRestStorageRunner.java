package org.swisspush.reststorage;

import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.swisspush.reststorage.util.ModuleConfiguration;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Runner class to start rest-storage in standalone mode with a redis based storage
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class RedisRestStorageRunner {

    public static void main(String[] args) {
        ModuleConfiguration modConfig = new ModuleConfiguration()
                .storageType(ModuleConfiguration.StorageType.redis)
                .redisReconnectAttempts(-1)
                .redisPoolRecycleTimeoutMs(-1)
                .resourceCleanupIntervalSec(10);

        Vertx.vertx().deployVerticle(new RestStorageMod(),
                new DeploymentOptions().setConfig(modConfig.asJsonObject()),
                RedisRestStorageRunner::onDeployComplete);
    }

    private static void onDeployComplete(AsyncResult<String> ev) {
        Logger log = getLogger(RedisRestStorageRunner.class);
        if( ev.failed() ){
            log.error("Failed to deploy RestStorageMod", ev.cause());
            return;
        }
        log.info("rest-storage started");
    }

}
