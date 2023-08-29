package org.swisspush.reststorage;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import org.slf4j.LoggerFactory;
import org.swisspush.reststorage.util.ModuleConfiguration;

/**
 * Runner class to start rest-storage in standalone mode with a redis based storage
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class RedisRestStorageRunner {

    public static void main(String[] args) {
        ModuleConfiguration modConfig = new ModuleConfiguration()
                .storageType(ModuleConfiguration.StorageType.redis)
                .resourceCleanupIntervalSec(10);

        Vertx.vertx().deployVerticle(new RestStorageMod(), new DeploymentOptions().setConfig(modConfig.asJsonObject()), event ->
                LoggerFactory.getLogger(RedisRestStorageRunner.class).info("rest-storage started"));
    }
}
