package org.swisspush.reststorage.migration;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.redis.client.RedisAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.reststorage.migration.tasks.Task;
import org.swisspush.reststorage.redis.RedisProvider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.lang.System.currentTimeMillis;

/**
 * Tool for coordinating and executing data migration tasks when enabling Redis Cluster partitioning.
 * Ensures only one migration runs at a time across cluster nodes using a distributed lock in Redis.
 * 
 * <p>Usage example:
 * <pre>
 * MigrateTool migrateTool = new MigrateTool(vertx, redisProvider, "instance-1");
 * migrateTool
 *   .addTask(new ClusterPartitionMigrationTask(...))
 *   .start()
 *   .onComplete(ar -> {
 *     if (ar.succeeded()) {
 *       System.out.println("Migration completed");
 *     } else {
 *       System.out.println("Migration failed: " + ar.cause().getMessage());
 *     }
 *   });
 * </pre>
 */
public class MigrateTool {
    private static final Logger log = LoggerFactory.getLogger(MigrateTool.class);
    
    private static final int MIGRATION_LOCK_TIMEOUT_MS = 10_000;     // 10 seconds
    private static final int MIGRATION_LOCK_REFRESH_MS = 2_000;      // 2 seconds
    private static final String MIGRATION_LOCK_KEY = "rest-storage:migration:lock";
    
    private final String instanceId;
    private final RedisProvider redisProvider;
    private final Vertx vertx;
    private final List<Task> tasks = new ArrayList<>();
    private Long refreshTimerId = null;

    /**
     * Creates a new MigrateTool instance.
     * 
     * @param vertx the Vertx instance
     * @param redisProvider the Redis provider for accessing Redis
     * @param instanceId unique identifier for this instance (used for lock ownership)
     */
    public MigrateTool(Vertx vertx, RedisProvider redisProvider, String instanceId) {
        this.vertx = vertx;
        this.redisProvider = redisProvider;
        this.instanceId = instanceId;
    }

    /**
     * Adds a migration task to be executed.
     * 
     * @param task the task to add
     * @return this MigrateTool instance for method chaining
     */
    public MigrateTool addTask(Task task) {
        tasks.add(task);
        return this;
    }

    /**
     * Starts the migration process. If no tasks are registered, completes immediately.
     * Attempts to acquire a distributed lock; if successful, runs all tasks sequentially.
     * If the lock is already held by another instance, waits for that instance to complete.
     * 
     * @return a Future that completes when migration is done (whether locally or by another instance)
     */
    public Future<Void> start() {
        if (tasks.isEmpty()) {
            log.info("No migration tasks registered (instanceId:{})", instanceId);
            return Future.succeededFuture();
        }

        Promise<Void> migratePromise = Promise.promise();
        
        acquireLock().onComplete(ar -> {
            if (ar.failed()) {
                log.error("Failed to acquire migration lock (instanceId:{})", instanceId, ar.cause());
                migratePromise.fail(ar.cause());
                return;
            }
            
            if (ar.result()) {
                // Lock acquired - run migration tasks
                log.info("Migration lock acquired, starting tasks (instanceId:{})", instanceId);
                startRefreshTimer();
                runTasksSequentially()
                    .onComplete(runResult -> {
                        releaseLock().onComplete(releaseResult -> {
                            if (runResult.failed()) {
                                migratePromise.fail(runResult.cause());
                            } else {
                                migratePromise.complete();
                            }
                        });
                    });
            } else {
                // Lock held by another instance - wait for it to complete
                log.info("Migration already in progress on another instance, waiting (instanceId:{})", instanceId);
                waitForOtherMigrationCompletion().onComplete(ar2 -> {
                    log.info("Other instance migration completed (instanceId:{})", instanceId);
                    migratePromise.complete();
                });
            }
        });

        return migratePromise.future();
    }

    /**
     * Attempts to acquire the migration lock using SET NX PX.
     * 
     * @return Future completed with true if lock was acquired, false if already held
     */
    private Future<Boolean> acquireLock() {
        Promise<Boolean> promise = Promise.promise();
        
        redisProvider.redis().onComplete(ar -> {
            if (ar.failed()) {
                log.error("Redis connection failed (instanceId:{})", instanceId, ar.cause());
                promise.fail(ar.cause());
                return;
            }
            
            RedisAPI redisAPI = ar.result();
            String lockValue = String.valueOf(currentTimeMillis());
            
            redisAPI.set(
                Arrays.asList(
                    MIGRATION_LOCK_KEY,
                    lockValue,
                    "NX",
                    "PX",
                    String.valueOf(MIGRATION_LOCK_TIMEOUT_MS)
                ),
                setResult -> {
                    if (setResult.failed()) {
                        log.error("Failed to set migration lock (instanceId:{})", instanceId, setResult.cause());
                        promise.fail(setResult.cause());
                    } else {
                        boolean acquired = setResult.result() != null;
                        promise.complete(acquired);
                    }
                }
            );
        });
        
        return promise.future();
    }

    /**
     * Releases the migration lock by deleting it from Redis.
     */
    private Future<Void> releaseLock() {
        stopRefreshTimer();
        Promise<Void> promise = Promise.promise();
        
        redisProvider.redis().onComplete(ar -> {
            if (ar.failed()) {
                log.warn("Redis connection failed during lock release (instanceId:{})", instanceId, ar.cause());
                promise.fail(ar.cause());
                return;
            }
            
            RedisAPI redisAPI = ar.result();
            redisAPI.del(Collections.singletonList(MIGRATION_LOCK_KEY), delResult -> {
                if (delResult.failed()) {
                    log.warn("Failed to delete migration lock (instanceId:{})", instanceId, delResult.cause());
                    promise.fail(delResult.cause());
                } else {
                    log.info("Migration lock released (instanceId:{})", instanceId);
                    promise.complete();
                }
            });
        });
        
        return promise.future();
    }

    /**
     * Runs all registered tasks sequentially.
     */
    private Future<Void> runTasksSequentially() {
        Future<Void> chain = Future.succeededFuture();
        
        for (Task task : tasks) {
            chain = chain.compose(v -> {
                log.info("Starting migration task: {} (instanceId:{})", task.getTaskKey(), instanceId);
                return task.run()
                    .map(success -> {
                        if (!success) {
                            throw new RuntimeException("Task failed: " + task.getTaskKey());
                        }
                        log.info("Migration task completed: {} (instanceId:{})", task.getTaskKey(), instanceId);
                        return null;
                    });
            });
        }
        
        return chain;
    }

    /**
     * Waits for another instance's migration to complete by polling the lock key.
     */
    private Future<Void> waitForOtherMigrationCompletion() {
        Promise<Void> promise = Promise.promise();
        pollLockKey(promise);
        return promise.future();
    }

    /**
     * Polls the lock key to check if migration is still in progress.
     */
    private void pollLockKey(Promise<Void> promise) {
        redisProvider.redis().onComplete(ar -> {
            if (ar.failed()) {
                log.error("Redis connection failed during lock polling (instanceId:{})", instanceId, ar.cause());
                promise.fail(ar.cause());
                return;
            }
            
            RedisAPI redisAPI = ar.result();
            redisAPI.exists(Collections.singletonList(MIGRATION_LOCK_KEY), existsResult -> {
                if (existsResult.failed()) {
                    log.error("Failed to check lock key (instanceId:{})", instanceId, existsResult.cause());
                    promise.fail(existsResult.cause());
                    return;
                }
                
                if (existsResult.result().toInteger() == 0) {
                    // Lock released, migration complete
                    log.debug("Lock key no longer exists, migration complete (instanceId:{})", instanceId);
                    promise.complete();
                } else {
                    // Lock still held, check again later
                    log.debug("Lock key still exists, checking again in {}ms (instanceId:{})", 
                        MIGRATION_LOCK_REFRESH_MS, instanceId);
                    vertx.setTimer(MIGRATION_LOCK_REFRESH_MS, tid -> pollLockKey(promise));
                }
            });
        });
    }

    /**
     * Starts a periodic timer to refresh the migration lock.
     */
    private void startRefreshTimer() {
        refreshTimerId = vertx.setPeriodic(MIGRATION_LOCK_REFRESH_MS, tid -> {
            redisProvider.redis().onComplete(ar -> {
                if (ar.failed()) {
                    log.error("Redis connection failed during lock refresh (instanceId:{})", instanceId, ar.cause());
                    return;
                }
                
                RedisAPI redisAPI = ar.result();
                redisAPI.pexpire(
                    Arrays.asList(
                        MIGRATION_LOCK_KEY,
                        String.valueOf(MIGRATION_LOCK_TIMEOUT_MS)
                    ),
                    pexpireResult -> {
                        if (pexpireResult.failed()) {
                            log.warn("Failed to refresh migration lock (instanceId:{})", instanceId, pexpireResult.cause());
                        } else {
                            log.debug("Migration lock TTL refreshed (instanceId:{})", instanceId);
                        }
                    }
                );
            });
        });
        log.debug("Migration lock refresh timer started (instanceId:{})", instanceId);
    }

    /**
     * Stops the periodic lock refresh timer.
     */
    private void stopRefreshTimer() {
        if (refreshTimerId != null) {
            vertx.cancelTimer(refreshTimerId);
            refreshTimerId = null;
            log.debug("Migration lock refresh timer stopped (instanceId:{})", instanceId);
        }
    }
}
