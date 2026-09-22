package org.swisspush.reststorage;

import io.vertx.core.Handler;
import org.swisspush.reststorage.util.LockMode;

import java.util.List;
import java.util.Optional;

/**
 * Defines the CRUD-style contract for a storage backend used by the REST API.
 * Implementations expose resource access, mutation, locking, and cleanup operations through
 * asynchronous handlers that are notified with a {@link Resource} or {@link DocumentResource} result.
 */
public interface Storage {

    /**
     * Gets the current percentage of the actual memory usage. Possible values are in range 0.0 to 100.0.
     *
     * @return the current percentage of the actual memory usage, or an empty optional when the value is unavailable
     */
    Optional<Float> getCurrentMemoryUsage();

    /**
     * Reads a resource and optionally returns a partial view of it using an offset and count.
     *
     * @param path the resource path to read
     * @param etag the expected entity tag for conditional access
     * @param offset the starting byte offset within the resource
     * @param count the maximum number of bytes to return
     * @param handler the callback invoked with the result of the read operation
     */
    void get(String path, String etag, int offset, int count, Handler<Resource> handler);

    /**
     * Expands a path into a set of sub-resources and returns the aggregated representation.
     *
     * @param path the path to expand
     * @param etag the expected entity tag for conditional access
     * @param subResources the list of sub-resources to include in the expansion
     * @param handler the callback invoked with the expansion result
     */
    void storageExpand(String path, String etag, List<String> subResources, Handler<Resource> handler);

    /**
     * Lists all document resource paths below the provided path without loading the document bodies.
     * <p>
     * Redis implementation note: this operation is not Redis Cluster safe.
     *
     * @param path the base path whose descendant document paths should be listed
     * @param handler the callback invoked with the list result
     */
    void list(String path, Handler<PathListResource> handler);

    /**
     * Stores or updates a resource without explicit locking.
     *
     * @param path the resource path to write
     * @param etag the expected entity tag for optimistic concurrency control
     * @param merge whether the write should merge with an existing resource
     * @param expire the expiration time in milliseconds, or a non-expiring value if disabled
     * @param handler the callback invoked with the write result
     */
    void put(String path, String etag, boolean merge, long expire, Handler<Resource> handler);

    /**
     * Stores or updates a resource using a lock owner and lock metadata.
     *
     * @param path the resource path to write
     * @param etag the expected entity tag for optimistic concurrency control
     * @param merge whether the write should merge with an existing resource
     * @param expire the expiration time in milliseconds, or a non-expiring value if disabled
     * @param lockOwner identifies the owner of the lock
     * @param lockMode the locking mode to apply during the write
     * @param lockExpire the lock expiration time in milliseconds
     * @param handler the callback invoked with the write result
     */
    void put(String path, String etag, boolean merge, long expire, String lockOwner, LockMode lockMode, long lockExpire, Handler<Resource> handler);

    /**
     * Stores or updates a resource while optionally compressing the payload before persistence.
     *
     * @param path the resource path to write
     * @param etag the expected entity tag for optimistic concurrency control
     * @param merge whether the write should merge with an existing resource
     * @param expire the expiration time in milliseconds, or a non-expiring value if disabled
     * @param lockOwner identifies the owner of the lock
     * @param lockMode the locking mode to apply during the write
     * @param lockExpire the lock expiration time in milliseconds
     * @param storeCompressed whether the payload should be stored in compressed form
     * @param handler the callback invoked with the write result
     */
    void put(String path, String etag, boolean merge, long expire, String lockOwner, LockMode lockMode, long lockExpire, boolean storeCompressed, Handler<Resource> handler);

    /**
     * Deletes a resource and optionally handles recursive and collection-specific deletion semantics.
     *
     * @param path the resource path to delete
     * @param lockOwner identifies the owner of the lock, if any
     * @param lockMode the locking mode used to validate access
     * @param lockExpire the lock expiration time in milliseconds
     * @param confirmCollectionDelete whether a collection delete requires explicit confirmation
     * @param deleteRecursive whether the deletion should cascade to child resources
     * @param handler the callback invoked with the delete result
     */
    void delete(String path, String lockOwner, LockMode lockMode, long lockExpire, boolean confirmCollectionDelete, boolean deleteRecursive, Handler<Resource> handler);

    /**
     * Runs the storage cleanup process and returns the resources that were cleaned up.
     *
     * @param handler the callback invoked with the cleanup result
     * @param cleanupResourcesAmount the maximum number of resources to clean up
     */
    void cleanup(Handler<DocumentResource> handler, String cleanupResourcesAmount);

}
