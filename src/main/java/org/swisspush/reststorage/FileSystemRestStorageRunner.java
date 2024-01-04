package org.swisspush.reststorage;

import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by florian kammermann on 23.05.2016.
 *
 * Deploys the rest-storage to vert.x.
 * Used in the standalone scenario.
 */
public class FileSystemRestStorageRunner {

    public static void main(String[] args) {
        Vertx.vertx().deployVerticle(new RestStorageMod(), FileSystemRestStorageRunner::onDeployDone);
    }

    private static void onDeployDone(AsyncResult<String> ev) {
        Logger log = getLogger(FileSystemRestStorageRunner.class);
        if( ev.failed() ){
            log.error("deployVerticle(new RestStorageMod())", ev.cause());
            return;
        }
        log.info("rest-storage started");
    }

}
