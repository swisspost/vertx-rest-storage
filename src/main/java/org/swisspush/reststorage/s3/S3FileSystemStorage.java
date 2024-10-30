package org.swisspush.reststorage.s3;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.reststorage.CollectionResource;
import org.swisspush.reststorage.DocumentResource;
import org.swisspush.reststorage.Resource;
import org.swisspush.reststorage.Storage;
import org.swisspush.reststorage.exception.RestStorageExceptionFactory;
import org.swisspush.reststorage.util.LockMode;
import software.amazon.nio.spi.s3.S3FileSystem;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystemException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Objects;
import java.util.Optional;


public class S3FileSystemStorage implements Storage {

    private final Path root;
    private final Vertx vertx;
    private final RestStorageExceptionFactory exceptionFactory;
    private final int rootLen;
    private final S3FileSystemDirLister fileSystemDirLister;
    private static volatile S3FileSystem fileSystem;

    private final Logger log = LoggerFactory.getLogger(S3FileSystemStorage.class);

    // See: software.amazon.nio.spi.s3.Constants
    public static final String S3_PATH_SEPARATOR = "/";

    // use only one file system instance
    private static S3FileSystem getFileSystem(URI uri) {
        if (fileSystem == null) {
            synchronized (S3FileSystem.class) {
                if (fileSystem == null) {
                    fileSystem = (S3FileSystem) FileSystems.getFileSystem(uri);
                }
            }
        }
        return fileSystem;
    }

    public S3FileSystemStorage(Vertx vertx, RestStorageExceptionFactory exceptionFactory, String rootPath,
                               String awsS3Region, String awsS3BucketName, String awsS3AccessKeyId, String awsS3SecretAccessKey) {
        this.vertx = vertx;
        this.exceptionFactory = exceptionFactory;
        Objects.requireNonNull(awsS3Region, "Region must not be null");
        Objects.requireNonNull(awsS3BucketName, "BucketName must not be null");
        Objects.requireNonNull(awsS3AccessKeyId, "AccessKeyId must not be null");
        Objects.requireNonNull(awsS3SecretAccessKey, "SecretAccessKey must not be null");

        System.setProperty("aws.region",awsS3Region);
        System.setProperty("aws.accessKeyId", awsS3AccessKeyId);
        System.setProperty("aws.secretAccessKey", awsS3SecretAccessKey);

        if (!awsS3BucketName.startsWith("s3:") && !awsS3BucketName.startsWith("s3x:")) {
            awsS3BucketName = "s3://" + awsS3BucketName;
        }

        var uri = URI.create(awsS3BucketName);
        fileSystem = getFileSystem(uri);
        root = fileSystem.getPath(rootPath);

        try (var pathStream = Files.walk(root)) {
            pathStream.forEach(System.out::println);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        this.fileSystemDirLister = new S3FileSystemDirLister(vertx, root.toString());
        // Unify format for simpler work.
        String tmpRoot;
        tmpRoot = root.toString();

        // Cache string length of root without trailing slashes
        int rootLen;
        for (rootLen = tmpRoot.length() - 1; tmpRoot.charAt(rootLen) == '/'; --rootLen) ;
        this.rootLen = rootLen;
    }

    @Override
    public Optional<Float> getCurrentMemoryUsage() {
        throw new UnsupportedOperationException("Method 'getCurrentMemoryUsage' not supported in S3FileSystemStorage");
    }

    @Override
    public void get(String path, String etag, final int offset, final int count, final Handler<Resource> handler) {
        final Path fullDirPath = canonicalize(path, true);
        final Path fullFilePath = canonicalize(path, false);
        log.debug("GET {}", path);
        if (Files.isRegularFile(fullFilePath, LinkOption.NOFOLLOW_LINKS)) {
            log.debug("Open file '{}' ({})", path, fullFilePath);
            DocumentResource d = new DocumentResource();
            try {
                final InputStream inputStream = Files.newInputStream(fullFilePath);
                d.length = Files.size(fullFilePath);
                log.debug("Successfully opened '{}' which is {} bytes in size.", path, d.length);

                final FileReadStream<io.vertx.core.buffer.Buffer> readStream = new FileReadStream<>(vertx, d.length, path, inputStream);
                d.readStream = readStream;

                final Runnable cleanUp = () -> {
                    if (!readStream.isClosed()){
                    readStream.close();
                    }
                    try {
                        inputStream.close();
                    } catch (IOException e) {
                        log.debug("Failed to close input stream", e);
                    }
                };
                d.closeHandler = v -> {
                    log.debug("Resource got closed. Close file now '{}'", path);
                    cleanUp.run();
                };
                d.addErrorHandler(event -> {
                    log.error("Get resource failed.", exceptionFactory.newException("Close file now '" + path + "'", event));
                    cleanUp.run();
                });
            } catch (IOException e) {
                log.warn("Failed to open '{}' for read", path, e);
                d.error = true;
                d.errorMessage = e.getMessage();
            }
            handler.handle(d);
            return;
        }

        if (Files.isDirectory(fullDirPath)) {
            log.debug("Delegate directory listing of '{}'", path);
            fileSystemDirLister.handleListingRequest(fullDirPath, offset, count, handler);
        } else {
            log.debug("No such file or directory '{}' ({})", path, fullDirPath);
            Resource r = new Resource();
            r.exists = false;
            handler.handle(r);
        }
    }

    @Override
    public void put(String path, String etag, boolean merge, long expire, final Handler<Resource> handler) {
        put(path, etag, merge, expire, "", LockMode.SILENT, 0, handler);
    }

    @Override
    public void put(String path, String etag, boolean merge, long expire, String lockOwner, LockMode lockMode, long lockExpire, Handler<Resource> handler) {
        final Path fullPath = canonicalize(path, false);
        if (Files.exists(fullPath, LinkOption.NOFOLLOW_LINKS)) {
            if (Files.isDirectory(fullPath)) {
                CollectionResource c = new CollectionResource();
                handler.handle(c);
            } else if (Files.isRegularFile(fullPath, LinkOption.NOFOLLOW_LINKS)) {
                putFile(handler, fullPath);
            } else {
                Resource r = new Resource();
                r.exists = false;
                handler.handle(r);
            }
        } else {
            if (Files.exists(fullPath)) {
                putFile(handler, fullPath);
            } else {
                try {
                    Files.createDirectory(fullPath.getParent());
                    putFile(handler, fullPath);
                } catch (IOException e) {
                    log.error("Failed to create directory '{}'", path);
                }
            }
        }
    }

    @Override
    public void put(String path, String etag, boolean merge, long expire, String lockOwner, LockMode lockMode, long lockExpire, boolean storeCompressed, Handler<Resource> handler) {
        if (storeCompressed) {
            log.warn("PUT with storeCompressed option is not yet implemented in file system storage. Ignoring storeCompressed option value");
        }
        put(path, etag, merge, expire, "", LockMode.SILENT, 0, handler);
    }

    private void putFile(final Handler<Resource> handler, final Path fullPath) {
        try {
            // DON"T close it in finally block, async code
            final OutputStream outputStream = Files.newOutputStream(fullPath);
            final DocumentResource d = new DocumentResource();

            FileWriteStream fileWriteStream = new FileWriteStream(outputStream);
            d.writeStream = fileWriteStream;

            final Runnable cleanUp = () -> {

                try {
                    outputStream.close();
                } catch (IOException e) {
                    log.error("Failed to close output stream:", e);
                }
                fileWriteStream.end();
            };

            d.closeHandler = event -> {
                cleanUp.run();
                d.endHandler.handle(null);
            };
            d.addErrorHandler(err -> {
                log.error("Put file failed:", err);
                cleanUp.run();
            });
            handler.handle(d);
        } catch (IOException e) {
            log.error("Failed to put file:", e);
        }
    }

    @Override
    public void delete(String path, String lockOwner, LockMode lockMode, long lockExpire, boolean confirmCollectionDelete,
                       boolean deleteRecursive, final Handler<Resource> handler) {

        // at this stage I don't know what of this path point to
        final Path fullFilePath = canonicalize(path, false);
        final Path fullDirPath = canonicalize(path, true);

        Resource resource = new Resource();
        boolean deleteRecursiveInFileSystem = true;
        if (confirmCollectionDelete && !deleteRecursive) {
            deleteRecursiveInFileSystem = false;
        }
        boolean finalDeleteRecursiveInFileSystem = deleteRecursiveInFileSystem;

        // try to check as a file
        boolean exists = Files.exists(fullFilePath, LinkOption.NOFOLLOW_LINKS);
        Path fullPath = fullFilePath;

        if (!exists) {
            // not exists, maybe is a directory
            exists = Files.exists(fullDirPath, LinkOption.NOFOLLOW_LINKS);
            if (exists) {
                // so it is a directory
                fullPath = fullDirPath;
            }
        }

        if (exists) {
            try {
                deleteRecursive(fullPath, finalDeleteRecursiveInFileSystem);
                deleteEmptyParentDirs(fullPath.getParent());
            } catch (IOException e) {
                if (e.getCause() != null && e.getCause() instanceof DirectoryNotEmptyException) {
                    resource.error = true;
                    resource.errorMessage = "directory not empty. Use recursive=true parameter to delete";
                } else {
                    resource.exists = false;
                }
            }
            handler.handle(resource);
        } else {
            Resource r = new Resource();
            r.exists = false;
            handler.handle(r);
        }
    }

    @Override
    public void cleanup(Handler<DocumentResource> handler, String cleanupResourcesAmount) {
        // nothing to do here
    }

    @Override
    public void storageExpand(String path, String etag, List<String> subResources, Handler<Resource> handler) {
        throw new UnsupportedOperationException("Method 'storageExpand' not supported in S3FileSystemStorage");
    }

    /**
     * Deletes all empty parent directories starting at specified directory.
     *
     * @param path Most deep (virtual) directory to start bubbling up deletion of empty
     *             directories.
     */
    private void deleteEmptyParentDirs(Path path) {
        // Analyze if we reached root.
        int pathLen;
        // Evaluate length of current path excluding trailing slashes by searching
        // last non-slash (backslash of course on windows).
        for (pathLen = path.toString().length() - 1; path.toString().charAt(pathLen) == File.separatorChar; --pathLen) ;
        if (rootLen == pathLen) {
            // We do NOT want to delete our virtual root even it is empty :)
            log.debug("Stop deletion here to keep virtual root '{}'.", root);
            return;
        }
        log.debug("Delete directory if empty '{}'.", path);

        if (!isDirEmpty(path)) {
            log.debug("Directory is NOT empty '{}'.", path);
            return;
        }

        try {
            if (Files.deleteIfExists(path)) {
                // Bubbling up to parent.
                final Path parentPath = path.getParent();
                // HINT 1: We go recursive here!
                // HINT 2: When debugging stack traces keep in mind this recursion occurs
                //         asynchronous and therefore is not really a recursion :)
                deleteEmptyParentDirs(parentPath);
            } else {
                throw new FileNotFoundException();
            }
        } catch (IOException cause) {
            if (cause instanceof FileSystemException && cause.getCause() instanceof DirectoryNotEmptyException) {
                // Failed to delete directory because it's not empty. Therefore we must not
                // delete it at all and we're done now.
                log.debug("Directory '{}' not empty. Stop bubbling deleting dirs.", path);
            } else if (cause instanceof FileSystemException && cause.getCause() instanceof NoSuchFileException) {
                // Somehow a caller requested to delete a directory which seems not to exist.
                // This should never be the case theoretically. (except maybe some race
                // conditions?)
                log.warn("Ignored to delete non-existing dir '{}'.", path);
            } else {
                // This case should not happen. At least up to now i've no idea of a valid
                // scenario for this one.
                log.error("Unexpected error while deleting empty directories.", exceptionFactory.newException(cause));
            }
        }
    }

    public Path canonicalize(String path, boolean isDir) {
        if (path.startsWith(S3_PATH_SEPARATOR)) {
            path = path.substring(1);
        }

        if (!path.endsWith(S3_PATH_SEPARATOR) && isDir) {
            path = path + S3_PATH_SEPARATOR;
        }
        return root.resolve(path);
    }

    private void deleteRecursive(Path path, boolean recursive) throws IOException {
        if (recursive) {
            Files.walkFileTree(path, new SimpleFileVisitor<>() {
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
                    if (e == null) {
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    } else {
                        throw e;
                    }
                }
            });
        } else {
            Files.delete(path);
        }
    }

    private boolean isDirEmpty(final Path path) {
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(path)) {
            return !dirStream.iterator().hasNext();
        } catch (IOException e) {
            log.debug("Error to detects is directory empty or not '{}'.", path, exceptionFactory.newException(e));
            return false;
        }
    }
}
