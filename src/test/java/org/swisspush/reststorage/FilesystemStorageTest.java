package org.swisspush.reststorage;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.impl.BufferImpl;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.CopyOptions;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.swisspush.reststorage.mocks.*;
import org.swisspush.reststorage.util.LockMode;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.swisspush.reststorage.exception.RestStorageExceptionFactory.newRestStorageWastefulExceptionFactory;

/**
 * Tests for the {@link FileSystemStorage} class
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
@RunWith(VertxUnitRunner.class)
public class FilesystemStorageTest {

    private static final Logger logger = LoggerFactory.getLogger(FilesystemStorageTest.class);

    @Test(expected=UnsupportedOperationException.class)
    public void testGetMemoryUsageNotYetImplemented(TestContext testContext){
        FileSystemStorage storage = new FileSystemStorage(mock(Vertx.class), newRestStorageWastefulExceptionFactory(), "/root");
        storage.getCurrentMemoryUsage();
    }

    @Test
    public void removesFilesWhenPutGotInterrupted(TestContext testContext) {

        // Keep track of state during test
        final boolean[] handlerCalledPtr = new boolean[]{ false };
        final boolean[] tmpFileOpened = new boolean[]{ false };
        final Resource[] resourcePtr = new Resource[]{ null };
        final boolean[] errorTriggeredPtr = new boolean[]{ false };
        final boolean[] fileGotClosed = new boolean[]{ false };
        final boolean[] fileGotDeleted = new boolean[]{ false };

        // Setup victim
        final String path;
        FileSystemStorage victim;
        {
            final String root = createPseudoFileStorageRoot();
            final String base = "/path/to/my/fancy";
            path = base + "/file";
            final FileSystem fileSystem = new FailFastVertxFileSystem(){
                @Override public FileSystem exists(String s, Handler<AsyncResult<Boolean>> handler) {
                    s = s.replaceAll("\\\\","/"); // <-- Fix ugly operating systems.
                    if( (root+path).equals(s) ) {
                        // Report that file to store doesn't exists already.
                        handler.handle(Future.succeededFuture(false));
                    }else if( (root+base).equals(s) ){
                        // Report that directory already exists.
                        handler.handle(Future.succeededFuture(true));
                    }else if( s.matches(".*/\\.tmp/uploads/file.*\\.part") ){
                        handler.handle(Future.succeededFuture(tmpFileOpened[0]));
                    }else{
                        throw new UnsupportedOperationException( msg );
                    }
                    return this;
                }
                @Override public FileSystem open(String s, OpenOptions openOptions, Handler<AsyncResult<AsyncFile>> handler) {
                    logger.debug( "Open file '{}'", s );
                    tmpFileOpened[0] = true;
                    final AsyncFile file = new FailFastVertxAsyncFile(){
                        @Override public void close(Handler<AsyncResult<Void>> handler) {
                            logger.debug("Closing file '{}'", s);
                            synchronized (errorTriggeredPtr){
                                if( !errorTriggeredPtr[0]) testContext.fail("Must close file AFTER there was an error.");
                            }
                            synchronized (fileGotClosed){
                                fileGotClosed[0] = true;
                            }
                            handler.handle(Future.succeededFuture());
                        }
                    };
                    handler.handle(Future.succeededFuture(file));
                    return this;
                }
                @Override
                public FileSystem delete(String s, Handler<AsyncResult<Void>> handler) {
                    // This check may not suffice because it succeeds no matter which file gets
                    // deleted.
                    synchronized (fileGotDeleted){
                        logger.debug( "Delete recursive '{}'.", s);
                        fileGotDeleted[0] = true;
                    }
                    if (handler != null) handler.handle(Future.succeededFuture());
                    return this;
                }
                @Override public FileSystem mkdirs(String s, Handler<AsyncResult<Void>> handler) {
                    handler.handle(Future.succeededFuture(null));
                    return this;
                }
            };
            final Vertx mockedVertx = new FailFastVertx(){
                @Override public FileSystem fileSystem() {
                    return fileSystem;
                }
            };
            victim = new FileSystemStorage(mockedVertx, newRestStorageWastefulExceptionFactory(), root);
        }

        // Challenge victim
        {
            final String etag = null; // Not used by implementation.
            final boolean merge = false; // Not used by implementation.
            final long expire = 0; // Not used by implementation.
            final String lockOwner = null; // Not used by implementation.
            final LockMode lockMode = null; // Not used by implementation.
            final long lockExpire = 0; // Not used by implementation.
            final boolean storeCompressed = false; // Not used by implementation.

            final Handler<Resource> handler = resource -> {
                resourcePtr[0] = resource;
                synchronized (handlerCalledPtr){
                    logger.debug( "put() handler got called.");
                    handlerCalledPtr[0] = true;
                }
            };

            victim.put(path, etag, merge, expire, lockOwner, lockMode, lockExpire, storeCompressed, handler);
        }

        // If below assert fails. You may need to insert a Thread.sleep here to give
        // victim a chance to fulfill our query.

        synchronized (handlerCalledPtr){
            testContext.assertTrue(handlerCalledPtr[0],"Victim failed to call our handler.");
        }

        testContext.assertNotNull(resourcePtr[0], "Victim failed to deliver resource to us via callback.");

        logger.debug( "Trigger an error on the resource" );
        {
            final Handler<Throwable> errorHandler = resourcePtr[0].errorHandler;
            synchronized (errorTriggeredPtr){
                errorTriggeredPtr[0] = true;
            }
            if(errorHandler != null){
                errorHandler.handle(new Exception("My fancy mock exception :)"));
            }
        }

        synchronized (fileGotDeleted){
            testContext.assertTrue(fileGotDeleted[0],"Victim failed to delete file.");
        }
    }

    @Test
    public void cleanupEmptyParentDirsOnResourceDeletion(final TestContext testContext) {

        // Keep track of test state
        final boolean[] deleteHandlerGotCalled = new boolean[]{ false };
        final boolean[] leafFileDeleted = new boolean[]{ false };
        final boolean[] dir_to_deleted = new boolean[]{ false };
        final boolean[] dir_file_deleted = new boolean[]{ false };
        final boolean[] dir_pseudo_deleted = new boolean[]{ false };
        final boolean[] dir_my_deleted = new boolean[]{ false };

        // Setup victim
        final FileSystemStorage victim;
        {
            // Mock vertx filesystem
            final FileSystem fileSystem = new FailFastVertxFileSystem(){
                @Override public FileSystem exists(String path, Handler<AsyncResult<Boolean>> handler) {
                    path = path.replaceAll("\\\\", "/"); // Fix windows
                    if( path.endsWith("/my/pseudo/file/to/delete") ){
                        handler.handle(Future.succeededFuture(true));
                    }else{
                        throw new UnsupportedOperationException(msg);
                    }
                    return this;
                }
                @Override public FileSystem deleteRecursive(String path, boolean b, Handler<AsyncResult<Void>> handler) {
                    path = path.replaceAll("\\\\","/"); // Fix windows.
                    if( path.endsWith("/my/pseudo/file/to/delete") ){
                        leafFileDeleted[0] = true;
                    }else{
                        testContext.fail( "Got unexpected path '"+path+"'." );
                    }
                    handler.handle(Future.succeededFuture(null));
                    return this;
                }
                @Override public FileSystem delete(String path, Handler<AsyncResult<Void>> handler) {
                    path = path.replaceAll("\\\\","/"); // Fix windows.
                    if( path.endsWith("/my/pseudo/file/to") ) {
                        dir_to_deleted[0] = true;
                    }else if( path.endsWith("/my/pseudo/file") ){
                        dir_file_deleted[0] = true;
                    }else if( path.endsWith("/my/pseudo") ){
                        dir_pseudo_deleted[0] = true;
                    }else if( path.endsWith("/my") ){
                        dir_my_deleted[0] = true;
                    }else{
                        testContext.fail( "Got unexpected path '"+path+"'." );
                    }
                    handler.handle(Future.succeededFuture(null));
                    return this;
                }
            };
            // Mock vertx
            final Vertx mockedVertx = new FailFastVertx(){
                @Override public FileSystem fileSystem() {
                    return fileSystem;
                }
            };
            // Use pseudo root (could be anything because our test will never access real filesystem).
            final String root = createPseudoFileStorageRoot();
            // Wire up victim instance.
            victim = new FileSystemStorage(mockedVertx, newRestStorageWastefulExceptionFactory(), root);
        }

        // Challenge victim
        victim.delete( "/my/pseudo/file/to/delete", null, null, 0, false, false, resource -> deleteHandlerGotCalled[0] = true);

        // Assert
        testContext.assertTrue( deleteHandlerGotCalled[0] , "Victim failed to call handler after deletion.");
        testContext.assertTrue( leafFileDeleted[0] , "Victim failed to delete resource itself." );
        testContext.assertTrue( dir_to_deleted[0] , "Victim failed to delete one of the directories.");
        testContext.assertTrue( dir_file_deleted[0] , "Victim failed to delete one of the directories.");
        testContext.assertTrue( dir_pseudo_deleted[0] , "Victim failed to delete one of the directories.");
        testContext.assertTrue( dir_my_deleted[0] , "Victim failed to delete one of the directories.");
    }

    @Test
    public void surviveConcurrentDirectoryDeleteWhilePut(final TestContext testContext) {
        final Async async = testContext.async();
        final Vertx realVertx = Vertx.vertx();
        final FileSystem realFileSystem = realVertx.fileSystem();

        // State tracking:
        final int[] directoryGotConcurrentlyDeletedCount = {0};
        final boolean[] victimIsDoneWithItsWork = {false};

        final String root = createPseudoFileStorageRoot();
        // Prepare request
        // Parent directory of file we want to PUT.
        final String dirName = "/foo/bar";
        // File name of file we want to PUT.
        final String baseName = "my-file";
        // Virtual storage path of file we want to PUT.
        final String path = dirName + "/" + baseName;

        final FileSystemStorage victim;
        { // Setup victim
            // Intercept move call to "concurrently" delete the directory.
            final FileSystem mockFileSystem = new DelegatingVertxFileSystem(realFileSystem) {
                private static final int FOOL_COUNT = 3;
                @Override
                public FileSystem move(String src, String dst, CopyOptions options, Handler<AsyncResult<Void>> handler) {
                    dst = dst.replaceAll("\\\\", "/"); // <-- Fix ugly operating systems.
                    if (dst.equals(root + path)) {
                        // Victim requested to move to final file.
                        if (++directoryGotConcurrentlyDeletedCount[0] < FOOL_COUNT) {
                            // Fool victim because we as "someone else" concurrently deleted prepared
                            // directory.
                            super.deleteBlocking(new File(dst).getParent());
                        }
                    }
                    return super.move(src, dst, options, handler);
                }
            };
            final Vertx mockedVertx = new FailFastVertx() {
                private final Map<Long, Object> timers = new HashMap<>();
                @Override
                public FileSystem fileSystem() {
                    return mockFileSystem;
                }
                @Override
                public long setTimer(long delayMillis, Handler<Long> handler) {
                    return realVertx.setTimer(delayMillis, handler);
                }
            };
            victim = new FileSystemStorage(mockedVertx, newRestStorageWastefulExceptionFactory(), root);
        }

        // Trigger work
        victim.put(path, null, false, 0, null, null, 0, resource -> {
            testContext.assertFalse(resource.error);
            testContext.assertFalse(resource.invalid);
            testContext.assertFalse(resource.rejected);
            // We MUST receive a DocumentResource.
            final DocumentResource documentResource = (DocumentResource) resource;
            // Provide handler, first because victim tries to call this without a null
            // check and 2nd to get notified when victim completes.
            documentResource.endHandler = aVoid -> victimIsDoneWithItsWork[0] = true;
            documentResource.addErrorHandler(thr -> {
                logger.trace( "Victim reported a problem:", new Exception("Mocked error handler received a throwable.",thr) );
                victimIsDoneWithItsWork[0] = true;
            });
            documentResource.writeStream.write(new BufferImpl().appendString("My test files content."));
            // Trigger close handler to signalize we're done. Victim now has to finalize
            // upload.
            realVertx.setTimer(1, aLong -> documentResource.closeHandler.handle(null));
        });

        new Runnable() {
            private int awaitCount = 0;

            @Override
            public void run() {
                // Await victim to complete its work
                awaitCount += 1;
                if (victimIsDoneWithItsWork[0]) {
                    logger.debug( "Victim completed after polling state {} times." , awaitCount );
                    doAsserts();
                } else if (awaitCount < 100) {
                    // Not ready yet. Give victim some more time.
                    realVertx.setTimer(100, aLong -> run());
                } else {
                    logger.debug( "Polled completion {} times.", awaitCount );
                    testContext.fail("Victim took too long to do its job.");
                }
            }

            private void doAsserts() {
                testContext.assertTrue( directoryGotConcurrentlyDeletedCount[0] > 0 , "Test failed to fool victim.");
                testContext.assertTrue(realFileSystem.existsBlocking(root + path), "Victim failed to create file");
                async.complete();
            }
        }.run();
    }

    /**
     * @return
     *      A path denoting a directory inside 'target/' directory for usage as a
     *      temporary fileStorage. This returns only a path. It does not create or
     *      check if such a directory exists or someone has access to it.
     */
    private static String createPseudoFileStorageRoot() {
        return new File( "target/fileStorage-"+ UUID.randomUUID()).getAbsolutePath().replaceAll("\\\\","/"); // <-- Fix windows
    }
}
