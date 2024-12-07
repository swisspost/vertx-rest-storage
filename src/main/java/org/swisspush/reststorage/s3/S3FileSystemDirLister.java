package org.swisspush.reststorage.s3;

import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.reststorage.CollectionResource;
import org.swisspush.reststorage.DocumentResource;
import org.swisspush.reststorage.Resource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.Stream;

import static org.swisspush.reststorage.s3.S3FileSystemStorage.S3_PATH_SEPARATOR;


/**
 * This type handles listing of directories in S3.
 *
 * <p>Internally it makes use of worker-threads to keep eventloop-thread
 * responsive.</p>
 */
public class S3FileSystemDirLister {

    private static final Logger log = LoggerFactory.getLogger(S3FileSystemDirLister.class);
    private final Vertx vertx;
    private final String root;


    public S3FileSystemDirLister(Vertx vertx, String root) {
        this.vertx = vertx;
        this.root = root;
    }

    public void handleListingRequest(Path path, final int offset, final int count, final Handler<Resource> handler) {
        // Delegate work to worker thread from thread pool.
        log.trace("Delegate to worker pool");
        final long startTimeMillis = System.currentTimeMillis();
        vertx.executeBlocking(promise -> {
            log.trace("Welcome on worker-thread.");
            listDirBlocking(path, offset, count, (Promise<CollectionResource>) (Promise<?>) promise);
            log.trace("worker-thread says bye.");
        }, event -> {
            log.trace("Welcome back on eventloop-thread.");
            if (log.isDebugEnabled()) {
                final long durationMillis = System.currentTimeMillis() - startTimeMillis;
                log.debug("List directory contents of '{}' took {}ms", path, durationMillis);
            }
            if (event.failed()) {
                log.error("Directory listing failed.", event.cause());
                final Resource erroneousResource = new Resource() {{
                    // Set fields according to documentation in Resource class.
                    name = path.getFileName().toString();
                    exists = false;
                    error = rejected = invalid = true;
                    errorMessage = invalidMessage = event.cause().getMessage();
                }};
                handler.handle(erroneousResource);
            } else {
                handler.handle((Resource) event.result());
            }
        });
        log.trace("Work delegated.");
    }

    private void listDirBlocking(Path path, int offset, int count, Promise<CollectionResource> promise) {
        // Prepare our result.
        final CollectionResource collection = new CollectionResource() {{
            items = new ArrayList<>(128);
        }};
        try (Stream<Path> source = Files.list(path)) {
            source.forEach(entry -> {
                String entryName = entry.getFileName().toString();
                log.trace("Processing entry '{}'", entryName);
                // Create resource representing currently processed directory entry.
                final Resource resource;
                if (entryName.endsWith(S3_PATH_SEPARATOR)) {
                    resource = new CollectionResource();
                    entryName = entryName.replace(S3_PATH_SEPARATOR, "");
                } else {
                    resource = new DocumentResource();
                }
                resource.name = entryName;
                collection.items.add(resource);
            });
        } catch (IOException e) {
            promise.fail(e);
            return;
        }
        Collections.sort(collection.items);
        // Don't know exactly what we do here now. Seems we check 'limit' for a range request.
        int n = count;
        if (n == -1) {
            n = collection.items.size();
        }
        // Don't know exactly what we do here. But it seems we evaluate 'start' of a range request.
        if (offset > -1) {
            if (offset >= collection.items.size() || (offset + n) >= collection.items.size() || (offset == 0 && n == -1)) {
                promise.complete(collection);
            } else {
                collection.items = collection.items.subList(offset, offset + n);
                promise.complete(collection);
            }
        } else {
            // TODO: Resolve future
            //       Previous implementation did nothing here. Why? Should we do something here?
            //       See: "https://github.com/hiddenalpha/vertx-rest-storage/blob/v2.5.2/src/main/java/org/swisspush/reststorage/FileSystemStorage.java#L77"
            log.warn("May we should do something here. I've no idea why old implementation did nothing.");
        }
    }
}
