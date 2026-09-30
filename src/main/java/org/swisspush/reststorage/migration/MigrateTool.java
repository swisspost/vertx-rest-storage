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
import java.util.UUID;

/**
 * Tool for coordinating and executing data migration tasks when enabling Redis Cluster partitioning.
 * Ensures only one migration runs at a time across cluster nodes using a distributed lock in Redis.
 *
 * <p><b>Lock safety:</b> the lock value is a random token generated on each acquisition, not just a
 * timestamp. Both the periodic TTL refresh and the final release use a Lua compare-and-swap script
 * that only mutates the lock if it still holds this instance's token. This prevents a stalled
 * instance (e.g. after a long GC pause during which the lock's TTL expired and another instance
 * acquired it) from refreshing or deleting a lock that has since legitimately become owned by a
 * different instance. If the refresh timer ever detects lost ownership, it stops itself and
 * {@link #start()}'s future fails, since the migration's exclusivity guarantee can no longer be
 * trusted for the remainder of that run.
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

    // Compare-and-swap scripts so a refresh/release only ever affects the lock this instance itself
    // acquired (identified by ARGV[1], a per-acquisition random token) - never a lock some other
    // instance has since acquired after ours expired (e.g. after a long GC pause).
    private static final String REFRESH_LOCK_SCRIPT =
            "if redis.call('GET', KEYS[1]) == ARGV[1] then " +
            "  return redis.call('PEXPIRE', KEYS[1], ARGV[2]) " +
            "else " +
            "  return 0 " +
            "end";
    private static final String RELEASE_LOCK_SCRIPT =
            "if redis.call('GET', KEYS[1]) == ARGV[1] then " +
            "  return redis.call('DEL', KEYS[1]) " +
            "else " +
            "  return 0 " +
            "end";

    private final String instanceId;
    private final RedisProvider redisProvider;
    private final Vertx vertx;
    private final List<Task> tasks = new ArrayList<>();
    private Long refreshTimerId = null;
    // Random per-acquisition token (not just instanceId) so even two acquisitions by the very same
    // instance (e.g. after a lost-and-reacquired lock) are never mistaken for one another.
    private volatile String lockToken = null;
    // Set by the refresh timer if it ever discovers the lock is no longer ours (lost ownership,
    // e.g. TTL expired before a refresh could run). Once true, release must not touch the lock -
    // it may already belong to another instance - and the migration result should be treated as
    // unsafe/failed rather than successful.
    private volatile boolean lockOwnershipLost = false;

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
                lockOwnershipLost = false;
                startRefreshTimer();
                runTasksSequentially()
                    .onComplete(runResult -> {
                        releaseLock().onComplete(releaseResult -> {
                            if (lockOwnershipLost) {
                                // The lock (and thus our exclusive right to run tasks) may have been
                                // held by another instance for part of the run - the result cannot be
                                // trusted as a safe, exclusive migration outcome.
                                migratePromise.fail("Migration lock ownership was lost during task execution "
                                        + "(instanceId:" + instanceId + "); result is not trustworthy");
                            } else if (runResult.failed()) {
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
     * Attempts to acquire the migration lock using SET NX PX. The lock value is a random,
     * per-acquisition token (see {@link #lockToken}) rather than just a timestamp, so later
     * refresh/release calls can verify (via a Lua compare-and-swap) that they still own the lock
     * before mutating it.
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
            String token = instanceId + ":" + UUID.randomUUID();
            
            redisAPI.set(
                Arrays.asList(
                    MIGRATION_LOCK_KEY,
                    token,
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
                        if (acquired) {
                            lockToken = token;
                        }
                        promise.complete(acquired);
                    }
                }
            );
        });
        
        return promise.future();
    }

    /**
     * Releases the migration lock, but only if it still holds the token this instance set when it
     * acquired it (compare-and-swap via {@link #RELEASE_LOCK_SCRIPT}). This prevents deleting a lock
     * that another instance has since legitimately acquired (e.g. after this instance's TTL expired
     * during a long stall and {@link #lockOwnershipLost} was set by the refresh timer).
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
            String token = lockToken;
            if (token == null) {
                // Nothing to release (we never held a token, e.g. acquireLock failed before setting it).
                log.debug("No lock token to release (instanceId:{})", instanceId);
                promise.complete();
                return;
            }
            redisAPI.eval(Arrays.asList(RELEASE_LOCK_SCRIPT, "1", MIGRATION_LOCK_KEY, token), evalResult -> {
                if (evalResult.failed()) {
                    log.warn("Failed to delete migration lock (instanceId:{})", instanceId, evalResult.cause());
                    promise.fail(evalResult.cause());
                } else {
                    boolean released = evalResult.result() != null && evalResult.result().toInteger() != 0;
                    if (released) {
                        log.info("Migration lock released (instanceId:{})", instanceId);
                    } else {
                        log.warn("Migration lock was not released because it no longer holds our token "
                                + "(instanceId:{}) - another instance may already own it", instanceId);
                    }
                    lockToken = null;
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
     * Starts a periodic timer to refresh the migration lock, using a Lua compare-and-swap so the TTL
     * is only extended while the lock still holds this instance's token (see {@link #lockToken}). If
     * the lock was lost (e.g. TTL expired during a stall before a refresh tick could run, and another
     * instance has since acquired it), {@link #lockOwnershipLost} is set and the timer stops itself -
     * further ticks would otherwise refresh a different instance's lock.
     */
    private void startRefreshTimer() {
        refreshTimerId = vertx.setPeriodic(MIGRATION_LOCK_REFRESH_MS, tid -> {
            redisProvider.redis().onComplete(ar -> {
                if (ar.failed()) {
                    log.error("Redis connection failed during lock refresh (instanceId:{})", instanceId, ar.cause());
                    return;
                }
                
                RedisAPI redisAPI = ar.result();
                String token = lockToken;
                if (token == null) {
                    return;
                }
                redisAPI.eval(
                    Arrays.asList(REFRESH_LOCK_SCRIPT, "1", MIGRATION_LOCK_KEY, token,
                            String.valueOf(MIGRATION_LOCK_TIMEOUT_MS)),
                    evalResult -> {
                        if (evalResult.failed()) {
                            log.warn("Failed to refresh migration lock (instanceId:{})", instanceId, evalResult.cause());
                            return;
                        }
                        boolean refreshed = evalResult.result() != null && evalResult.result().toInteger() != 0;
                        if (refreshed) {
                            log.debug("Migration lock TTL refreshed (instanceId:{})", instanceId);
                        } else {
                            log.error("Migration lock ownership lost (instanceId:{}); another instance may now "
                                    + "hold it. Stopping refresh timer.", instanceId);
                            lockOwnershipLost = true;
                            stopRefreshTimer();
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
