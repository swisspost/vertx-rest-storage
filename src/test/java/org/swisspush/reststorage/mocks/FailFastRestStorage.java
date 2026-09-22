package org.swisspush.reststorage.mocks;

import io.vertx.core.Handler;
import org.swisspush.reststorage.DocumentResource;
import org.swisspush.reststorage.PathListResource;
import org.swisspush.reststorage.Resource;
import org.swisspush.reststorage.Storage;
import org.swisspush.reststorage.util.LockMode;

import java.util.List;
import java.util.Optional;


public class FailFastRestStorage implements Storage {

    protected final String msg;

    public FailFastRestStorage() {
        this("Method not implemented in mock. Override method to provide your behaviour.");
    }
    
    public FailFastRestStorage(String msg) {
        this.msg = msg;
    }

    @Override
    public Optional<Float> getCurrentMemoryUsage() {
        throw new UnsupportedOperationException(msg);    }

    @Override
    public void get(String path, String etag, int offset, int count, Handler<Resource> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void storageExpand(String path, String etag, List<String> subResources, Handler<Resource> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void list(String path, Handler<PathListResource> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void put(String path, String etag, boolean merge, long expire, Handler<Resource> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void put(String path, String etag, boolean merge, long expire, String lockOwner, LockMode lockMode, long lockExpire, Handler<Resource> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void put(String path, String etag, boolean merge, long expire, String lockOwner, LockMode lockMode, long lockExpire, boolean storeCompressed, Handler<Resource> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void delete(String path, String lockOwner, LockMode lockMode, long lockExpire, boolean confirmCollectionDelete, boolean deleteRecursive, Handler<Resource> handler) {
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void cleanup(Handler<DocumentResource> handler, String cleanupResourcesAmount) {
        throw new UnsupportedOperationException(msg);
    }
}
