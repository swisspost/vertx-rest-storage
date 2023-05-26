package org.swisspush.reststorage;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.CopyOptions;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.UUID;


/**
 * Used by {@link FileSystemStorage} to execute file PUTs.
 */
public class FilePutter {

    private static final int MOVE_RETRY_TIMEOUT_MILLIS = 5_000;
    private static final int MOVE_RETRY_DELAY_MILLIS = 50;
    private static final Logger log = LoggerFactory.getLogger(FilePutter.class);
    private final CopyOptions moveOptions = new CopyOptions().setReplaceExisting(true);
    private final Vertx vertx;
    private final String root;
    private final String realPath;
    private final Handler<Resource> onCompleteHandler;
    private final FileCleanupManager fileCleanupManager = new FileCleanupManager();
    private String tmpFileVirtualPath;
    private String tmpFileRealPath;
    private String tmpFileParentRealPath;
    private volatile boolean executed = false;
    private long moveRetryExpirationTime = 0;
    private int moveToFinalDestinationAttemptCount = 0;

    /**
     * Package-private because currently only used internally.
     */
    FilePutter(Vertx vertx, String root, String realPath, Handler<Resource> onCompleteHandler) {
        this.vertx = vertx;
        this.root = root;
        this.realPath = canonicalizeRealPath(realPath);
        this.onCompleteHandler = onCompleteHandler;
    }

    /**
     * <p>Triggers the configured task. This method should only get called once per
     * instance!</p>
     *
     * @throws IllegalStateException Eg. in case method gets called more than one time.
     */
    public synchronized void execute() {
        if (executed) {
            throw new IllegalStateException("This putter already got executed.");
        }
        final FileSystem fileSystem = vertx.fileSystem();
        this.executed = true;
        // Setup required context.
        this.tmpFileVirtualPath = "/.tmp/uploads/" + new File(realPath).getName() + "-" + UUID.randomUUID() + ".part";
        this.tmpFileRealPath = canonicalizeVirtualPath(tmpFileVirtualPath);
        this.tmpFileParentRealPath = new File(tmpFileRealPath).getParent();
        // Prepare directory for temporary file.
        fileSystem.mkdirs(tmpFileParentRealPath, result -> {
            if (result.succeeded()) {
                openTmpFile();
            } else {
                log.warn("Failed to create directory '{}'.", tmpFileParentRealPath);
                resolveWithErroneousResource();
            }
        });
    }

    private void openTmpFile() {
        final FileSystem fileSystem = vertx.fileSystem();
        fileSystem.open(tmpFileRealPath, new OpenOptions(), result -> {
            if (result.succeeded()) {
                resolveWithTmpFileResource(tmpFileRealPath, result.result());
            } else {
                log.warn("Failed to open tmp file '{}'.", tmpFileRealPath);
                resolveWithErroneousResource();
            }
        });
    }

    private void resolveWithTmpFileResource(String realFilePath, final AsyncFile tmpFile) {
        final DocumentResource d = new DocumentResource();
        d.writeStream = tmpFile;
        d.closeHandler = v -> tmpFile.close(ev -> moveTmpFileToFinalDestination(d));
        d.addErrorHandler(err -> {
            log.error("Put file failed:", err);
            fileCleanupManager.cleanupFile(realFilePath, tmpFile, null);
        });
        // Resolve with ready-to-use resource.
        onCompleteHandler.handle(d);
    }

    private void moveTmpFileToFinalDestination(DocumentResource d) {
        final FileSystem fileSystem = vertx.fileSystem();
        if (moveRetryExpirationTime == 0) {
            // Evaluate expiration time of our retries.
            moveRetryExpirationTime = System.currentTimeMillis() + MOVE_RETRY_TIMEOUT_MILLIS;
        }
        // Move/rename our temporary file to its final destination.
        moveToFinalDestinationAttemptCount += 1;
        fileSystem.move(tmpFileRealPath, realPath, moveOptions, moveResult -> {
            if (moveResult.succeeded()) {
                log.debug("File stored successfully: {}", realPath);
                d.endHandler.handle(null);
            } else if (System.currentTimeMillis() < moveRetryExpirationTime) {
                // No timeout yet. Retry after some delay.
                vertx.setTimer(MOVE_RETRY_DELAY_MILLIS, aLong -> mkdirsAndTriggerMove(d));
            } else {
                log.error("Failed to move tmp file '{}' to its final destination '{}' even after trying {} times.", tmpFileRealPath, realPath, moveToFinalDestinationAttemptCount);
                d.errorHandler.handle(moveResult.cause());
            }
        });
    }

    private void mkdirsAndTriggerMove(DocumentResource d) {
        final FileSystem fileSystem = vertx.fileSystem();
        // Creating (possibly missing) parent dirs and try again.
        fileSystem.mkdirs(dirName(realPath), mkdirResult -> {
            if (mkdirResult.succeeded()) {
                // Trigger move
                moveTmpFileToFinalDestination(d);
            } else {
                log.error("Failed to create parent dirs of '{}'.", realPath);
                fileSystem.delete(tmpFileVirtualPath, deleteResult -> {
                    if (deleteResult.failed()) {
                        log.warn("Failed to delete tmp file '{}'.", tmpFileVirtualPath);
                    }
                    d.errorHandler.handle(mkdirResult.cause());
                });
            }
        });
    }

    private void resolveWithErroneousResource() {
        final Resource r = new Resource();
        r.error = true;
        r.exists = false;
        onCompleteHandler.handle(r);
    }

    private String dirName(String path) {
        return new File(path).getParent();
    }

    private String canonicalizeVirtualPath(String path) {
        return canonicalizeRealPath(root + path);
    }

    private static String canonicalizeRealPath(String path) {
        try {
            return new File(path).getCanonicalPath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    ///////////////////////////////////////////////////////////////////////////////
    // helper classes
    ///////////////////////////////////////////////////////////////////////////////

    /**
     * This is to cleanup a may open file by first close and then delete it.
     */
    private class FileCleanupManager {
        /**
         * @param realPath
         *      Absolute, real path of the file to delete. In case this is {@code null},
         *      no file will be deleted.
         * @param file
         *      The file to close. If this is null, no resource will be closed.
         */
        public void cleanupFile(String realPath, AsyncFile file, Handler<AsyncResult<Void>> handler) {
            if (file != null) {
                log.trace("A file got passed. Close it now.");
                try {
                    file.close(closeResult -> {
                        final Throwable cause = closeResult.cause();
                        if (closeResult.succeeded()) {
                            log.trace("File successfully closed.");
                        } else {
                            log.trace("Failed to close file:", cause);
                        }
                        deleteFile(realPath, handler);
                    });
                } catch (IllegalStateException e) {
                    if ("File handle is closed".equals(e.getMessage())) {
                        log.trace("We'll ignore that file already is closed.", e);
                        // Recover and continue with deletion because we're not interested when file
                        // already was closed.
                        deleteFile(realPath, handler);
                    } else {
                        throw e;
                    }
                }
            } else {
                log.trace("Nothing to close. Go directly to deletion step.");
                deleteFile(realPath, handler);
            }
        }

        /**
         * <p>This method should remain private within {@link FileCleanupManager} and
         * should NOT be called by {@link FilePutter}!</p>
         */
        private void deleteFile(String realPath, Handler<AsyncResult<Void>> handler) {
            final FileSystem fileSystem = vertx.fileSystem();
            if (realPath != null) {
                log.trace("Deleting file '{}'.", realPath);
                fileSystem.delete(realPath, handler);
            } else {
                log.trace("Nothing to delete. Skip.");
                if (handler != null) {
                    handler.handle(Future.succeededFuture());
                }
            }
        }
    }

}
