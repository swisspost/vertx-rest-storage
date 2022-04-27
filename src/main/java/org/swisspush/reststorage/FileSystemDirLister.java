package org.swisspush.reststorage;

import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.Stream;


/**
 * This type handles listing of directories in filesystem.
 *
 * <p>Internally it makes use of worker-threads to keep eventloop-thread
 * responsive.</p>
 */
public class FileSystemDirLister {

    private static final Logger log = LoggerFactory.getLogger(FileSystemDirLister.class);
    private final Vertx vertx;
    private final String root;

    public FileSystemDirLister(Vertx vertx, String root) {
        this.vertx = vertx;
        this.root = root;
    }

    public void handleListingRequest(String path, final int offset, final int count, final Handler<Resource> handler) {
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
                    name = Paths.get(path).getFileName().toString();
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

    private void listDirBlocking(String path, int offset, int count, Promise<CollectionResource> promise) {
        //
        // HINT: This method gets executed on a worker thread!
        //
        // Convert String to Path
        final Path searchPath = Paths.get(canonicalizeVirtualPath(path));
        // Prepare our result.
        final CollectionResource collection = new CollectionResource() {{
            items = new ArrayList<>(128);
        }};
        final String fullPath = canonicalizeVirtualPath(path);
        try (Stream<Path> source = Files.list(searchPath)) {
            source.forEach(entry -> {
                final String entryName = entry.getFileName().toString();
                log.trace("Processing entry '{}'", entryName);
                if (".tmp".equals(entryName) && fullPath.length() == root.length()) {
                    // Ignore hidden '/.tmp/' directory.
                    return;
                }
                // Create resource representing currently processed directory entry.
                final Resource resource;
                if (Files.isDirectory(entry)) {
                    resource = new CollectionResource();
                } else if (Files.isRegularFile(entry)) {
                    resource = new DocumentResource();
                } else {
                    resource = new Resource();
                    resource.exists = false;
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

}
