package org.swisspush.reststorage.migration.tasks;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.reststorage.redis.PartitionContext;
import org.swisspush.reststorage.redis.RedisProvider;
import org.swisspush.reststorage.util.ModuleConfiguration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.lang.Math.min;

/**
 * Migrates an existing rest-storage keyspace from the plain (non-cluster) key layout to the
 * Redis Cluster hash-tagged layout used when {@code redisClusterPartitioningEnabled} is {@code true}.
 *
 * <p>Run this <b>once, while no rest-storage instance is writing</b>, and only switch
 * {@code redisClusterPartitioningEnabled} to {@code true} afterwards.</p>
 *
 * <h2>What it changes</h2>
 * <table>
 *   <caption>Key layout before/after</caption>
 *   <tr><th>Key</th><th>Before</th><th>After</th></tr>
 *   <tr><td>resource</td><td>{@code rest-storage:resources:project:a}</td><td>{@code rest-storage:resources:{project}:a}</td></tr>
 *   <tr><td>collection</td><td>{@code rest-storage:collections:project:a}</td><td>{@code rest-storage:collections:{project}:a}</td></tr>
 *   <tr><td>lock</td><td>{@code rest-storage:locks:project:a}</td><td>{@code rest-storage:locks:{project}:a}</td></tr>
 *   <tr><td>delta</td><td>{@code delta:resources:project:a}</td><td>{@code delta:resources:{project}:a}</td></tr>
 *   <tr><td>expirable</td><td>one global ZSET {@code rest-storage:expirable}</td>
 *       <td>one ZSET per partition {@code rest-storage:expirable:{project}}, members rewritten</td></tr>
 *   <tr><td>root collection</td><td>ZSET {@code rest-storage:collections}</td>
 *       <td><i>deleted</i>; its members seed the partition registry</td></tr>
 *   <tr><td>partition registry</td><td>&mdash;</td><td>SET {@code rest-storage:locks-partitions}</td></tr>
 * </table>
 *
 * <p>The partition registry is what cluster-mode root {@code GET}/{@code DELETE} and the periodic
 * {@code cleanup} iterate over (see {@code RedisStorage}); without it, migrated data would be
 * invisible at the root even though the individual resource keys exist.</p>
 *
 * <h2>How keys are moved</h2>
 * <p>Keys are moved with {@code RENAME}: it is executed server-side, so resource values (which may be
 * gzip-compressed binary) are never round-tripped through the String-based {@code RedisAPI} and cannot
 * be corrupted, and TTLs are preserved automatically.</p>
 *
 * <p><b>Consequence:</b> {@code RENAME} cannot move a key across hash slots, so this task must be run
 * against the <i>pre-cluster</i> Redis (standalone or Sentinel). Running it against a real Redis Cluster
 * that still holds untagged keys will fail with {@code CROSSSLOT}; in that case migrate with an external
 * tool that can move keys between nodes.</p>
 *
 * <p>The task is <b>idempotent</b>: {@link PartitionContext#forPath} maps an already-tagged path to
 * itself, so re-running it over a migrated keyspace is a no-op.</p>
 *
 * <h2>Batching</h2>
 * <p>Both key spaces and the expirable set are migrated page-by-page as they are discovered via
 * {@code SCAN}/{@code ZSCAN} (bounded by {@link #SCAN_COUNT} keys/entries per page) instead of first
 * collecting the entire data set into memory. Within a page, up to {@link #BATCH_SIZE} operations
 * ({@code RENAME}/{@code ZADD}) are issued concurrently and awaited together before the next batch
 * starts, bounding both memory usage and in-flight Redis commands regardless of data set size.</p>
 *
 * <h2>Completion flag</h2>
 * <p>Once the migration finishes successfully, a permanent completion flag is set at
 * {@value #DONE_KEY}. On every subsequent {@link #run()} (e.g. after an application restart with
 * {@code redisClusterPartitioningEnabled} still {@code true}), that flag is checked first and, if
 * present, the whole migration is skipped - the flag is only ever checked/set here, never by
 * {@code MigrateTool} (which stays task-agnostic). A failed run leaves no flag behind, so it is
 * retried on the next {@link #run()}.</p>
 */
public class ClusterPartitionMigrationTask implements Task {

    private static final Logger log = LoggerFactory.getLogger(ClusterPartitionMigrationTask.class);

    private static final String TASK_KEY = "cluster-partition-migration";
    /** Permanent completion flag set in Redis once this task has run through successfully. */
    private static final String DONE_KEY = "rest-storage:migration:tasks:" + TASK_KEY + ":done";
    private static final int SCAN_COUNT = 1000;
    /** Max number of RENAME/ZADD operations issued concurrently for one SCAN/ZSCAN page. */
    private static final int BATCH_SIZE = 50;
    /** Largest magnitude a {@code double} can still represent every integer up to, i.e. 2^53. */
    private static final double MAX_EXACT_INTEGRAL_DOUBLE = 9007199254740992.0;

    private final RedisProvider redisProvider;
    private final String resourcesPrefix;
    private final String collectionsPrefix;
    private final String deltaResourcesPrefix;
    private final String deltaEtagsPrefix;
    private final String expirablePrefix;
    private final String lockPrefix;
    private final String partitionRegistryKey;

    /**
     * Convenience constructor deriving every prefix from the same {@link ModuleConfiguration} the
     * {@code RedisStorage} instance is (or will be) configured with.
     */
    public ClusterPartitionMigrationTask(RedisProvider redisProvider, ModuleConfiguration config) {
        this(redisProvider,
                config.getResourcesPrefix(),
                config.getCollectionsPrefix(),
                config.getDeltaResourcesPrefix(),
                config.getDeltaEtagsPrefix(),
                config.getExpirablePrefix(),
                config.getLockPrefix());
    }

    public ClusterPartitionMigrationTask(
            RedisProvider redisProvider,
            String resourcesPrefix,
            String collectionsPrefix,
            String deltaResourcesPrefix,
            String deltaEtagsPrefix,
            String expirablePrefix,
            String lockPrefix) {
        this.redisProvider = redisProvider;
        this.resourcesPrefix = resourcesPrefix;
        this.collectionsPrefix = collectionsPrefix;
        this.deltaResourcesPrefix = deltaResourcesPrefix;
        this.deltaEtagsPrefix = deltaEtagsPrefix;
        this.expirablePrefix = expirablePrefix;
        this.lockPrefix = lockPrefix;
        // Must match RedisStorage's registry key: config.getLockPrefix() + "-partitions"
        this.partitionRegistryKey = lockPrefix + "-partitions";
    }

    @Override
    public String getTaskKey() {
        return TASK_KEY;
    }

    @Override
    public Future<Boolean> run() {
        log.info("Starting cluster partition migration");
        final Set<String> tags = new LinkedHashSet<>();
        final Stats stats = new Stats();

        return redisProvider.redis().compose(redisAPI ->
                isAlreadyDone(redisAPI).compose(done -> {
                    if (done) {
                        log.info("Cluster partition migration already marked done ('{}' exists), skipping", DONE_KEY);
                        return Future.succeededFuture(true);
                    }
                    return migrateKeySpace(redisAPI, resourcesPrefix, tags, stats)
                            .compose(v -> migrateKeySpace(redisAPI, collectionsPrefix, tags, stats))
                            .compose(v -> migrateKeySpace(redisAPI, deltaResourcesPrefix, tags, stats))
                            .compose(v -> migrateKeySpace(redisAPI, deltaEtagsPrefix, tags, stats))
                            .compose(v -> migrateKeySpace(redisAPI, lockPrefix, tags, stats))
                            .compose(v -> migrateExpirableSet(redisAPI, tags, stats))
                            .compose(v -> migrateRootCollection(redisAPI, tags))
                            .compose(v -> writePartitionRegistry(redisAPI, tags))
                            .compose(v -> markDone(redisAPI))
                            .map(v -> {
                                log.info("Cluster partition migration finished: {} key(s) renamed, {} already tagged, "
                                                + "{} expirable entry/entries redistributed, {} partition(s): {}",
                                        stats.renamed, stats.skipped, stats.expirableEntries, tags.size(), tags);
                                return true;
                            });
                })
        ).otherwise(err -> {
            log.error("Cluster partition migration failed", err);
            return false;
        });
    }

    /**
     * Checks whether {@link #DONE_KEY} is already set, i.e. whether a previous run of this task
     * already completed successfully.
     */
    private Future<Boolean> isAlreadyDone(RedisAPI redisAPI) {
        return redisAPI.exists(Collections.singletonList(DONE_KEY))
                .map(resp -> resp != null && resp.toInteger() > 0);
    }

    /**
     * Permanently marks this task as done so future runs (see {@link #isAlreadyDone}) skip it.
     */
    private Future<Void> markDone(RedisAPI redisAPI) {
        return redisAPI.set(Arrays.asList(DONE_KEY, String.valueOf(System.currentTimeMillis()))).mapEmpty();
    }

    // ------------------------------------------------------------------
    // key spaces (resources / collections / delta / locks)
    // ------------------------------------------------------------------

    /**
     * Renames every {@code <prefix>:<untagged path>} key to {@code <prefix>:<tagged path>}, processing
     * one {@code SCAN} page (bounded by {@link #SCAN_COUNT}) at a time instead of collecting the whole
     * key space in memory first. Renaming interleaved with an ongoing {@code SCAN} is safe here: a
     * rename only ever changes the (still matching) prefix, and {@link #migrateKey} is idempotent, so a
     * renamed key resurfacing later in the same scan (per SCAN's at-least-once guarantee) is a no-op.
     */
    private Future<Void> migrateKeySpace(RedisAPI redisAPI, String prefix, Set<String> tags, Stats stats) {
        return scanAndProcess(redisAPI, prefix + ":*",
                keys -> forEachBatched(keys, BATCH_SIZE, key -> migrateKey(redisAPI, prefix, key, tags, stats)));
    }

    private Future<Void> migrateKey(RedisAPI redisAPI, String prefix, String key, Set<String> tags, Stats stats) {
        if (!key.startsWith(prefix)) {
            return Future.succeededFuture();
        }
        // The partition registry lives next to the lock prefix ("<lockPrefix>-partitions"); it is not a
        // resource path and must never be tagged. The ":*" pattern does not match it, but guard anyway.
        if (key.equals(partitionRegistryKey)) {
            return Future.succeededFuture();
        }
        String encodedPath = key.substring(prefix.length());
        PartitionContext ctx = PartitionContext.forPath(encodedPath, true, expirablePrefix);
        if (ctx.getTag() == null) {
            return Future.succeededFuture();
        }
        tags.add(ctx.getTag());
        String newKey = prefix + ctx.getKey();
        if (newKey.equals(key)) {
            stats.skipped++;
            return Future.succeededFuture();
        }
        return redisAPI.rename(key, newKey)
                .<Void>mapEmpty()
                .recover(err -> {
                    // The key may have been deleted (e.g. expired) between SCAN and RENAME - harmless.
                    if (isNoSuchKey(err)) {
                        log.debug("Key '{}' vanished before it could be renamed, skipping", key);
                        return Future.succeededFuture();
                    }
                    return Future.failedFuture(err);
                })
                .map(v -> {
                    stats.renamed++;
                    return null;
                });
    }

    // ------------------------------------------------------------------
    // expirable set
    // ------------------------------------------------------------------

    /**
     * Splits the single global expirable ZSET into one ZSET per partition. Its members are resource
     * <i>key names</i>, so they have to be rewritten to their tagged form as they are redistributed.
     * Entries are read and redistributed page-by-page via {@code ZSCAN} (bounded by {@link #SCAN_COUNT})
     * instead of loading the whole set into memory with a single {@code ZRANGE 0 -1}.
     */
    private Future<Void> migrateExpirableSet(RedisAPI redisAPI, Set<String> tags, Stats stats) {
        return redisAPI.exists(Collections.singletonList(expirablePrefix)).compose(exists -> {
            if (exists == null || exists.toInteger() == 0) {
                log.debug("Global expirable set '{}' is empty or absent, nothing to redistribute", expirablePrefix);
                return Future.succeededFuture();
            }
            return genericScan(redisAPI, (cursor, count) -> redisAPI.zscan(Arrays.asList(expirablePrefix, cursor, "COUNT", String.valueOf(count))),
                    entries -> forEachBatched(readScoredEntries(entries), BATCH_SIZE,
                            entry -> migrateExpirableEntry(redisAPI, entry[0], entry[1], tags, stats)))
                    .compose(v -> redisAPI.del(Collections.singletonList(expirablePrefix)).mapEmpty());
        });
    }

    /**
     * Reads a flat {@code ZSCAN} page reply ({@code [member1, score1, member2, score2, ...]}) into
     * {@code [member, score]} pairs.
     */
    private static List<String[]> readScoredEntries(List<Response> page) {
        List<String[]> entries = new ArrayList<>();
        for (int i = 0; i + 1 < page.size(); i += 2) {
            entries.add(new String[]{page.get(i).toString(), formatScore(page.get(i + 1))});
        }
        return entries;
    }

    /**
     * Renders a score so it can be fed straight back into {@code ZADD}. RESP3 delivers scores as
     * doubles, whose {@code toString()} uses scientific notation (e.g. {@code 1.893456E12}) for the
     * large epoch-millis values this storage uses; integral scores are therefore rendered as plain
     * integers to keep the migrated set byte-identical to what {@code put.lua} would have written.
     */
    private static String formatScore(Response score) {
        if (score == null) {
            return "0";
        }
        Double value;
        try {
            value = score.toDouble();
        } catch (RuntimeException e) {
            return score.toString();
        }
        if (value == null || value.isNaN() || value.isInfinite()) {
            return score.toString();
        }
        if (value == Math.rint(value) && Math.abs(value) <= MAX_EXACT_INTEGRAL_DOUBLE) {
            return Long.toString(value.longValue());
        }
        return value.toString();
    }

    private Future<Void> migrateExpirableEntry(RedisAPI redisAPI, String member, String score,
                                               Set<String> tags, Stats stats) {
        if (!member.startsWith(resourcesPrefix)) {
            log.warn("Expirable set member '{}' does not start with resources prefix '{}', skipping",
                    member, resourcesPrefix);
            return Future.succeededFuture();
        }
        String encodedPath = member.substring(resourcesPrefix.length());
        PartitionContext ctx = PartitionContext.forPath(encodedPath, true, expirablePrefix);
        if (ctx.getTag() == null) {
            return Future.succeededFuture();
        }
        tags.add(ctx.getTag());
        String taggedMember = resourcesPrefix + ctx.getKey();
        return redisAPI.zadd(Arrays.asList(ctx.getExpirableSetKey(), score, taggedMember))
                .<Void>mapEmpty()
                .map(v -> {
                    stats.expirableEntries++;
                    return null;
                });
    }

    // ------------------------------------------------------------------
    // root collection -> partition registry
    // ------------------------------------------------------------------

    /**
     * In cluster mode the shared, untagged root collection key is never written (see put-cluster.lua),
     * and root listings are served from the partition registry instead. Its current members are exactly
     * the top-level path segments, i.e. the partition tags, so they seed the registry before it is dropped.
     */
    private Future<Void> migrateRootCollection(RedisAPI redisAPI, Set<String> tags) {
        return redisAPI.zrange(Arrays.asList(collectionsPrefix, "0", "-1")).compose(response -> {
            if (response != null) {
                for (Response member : response) {
                    String tag = PartitionContext.derivePartitionTag(PartitionContext.PATH_SEP + member.toString());
                    if (tag != null) {
                        tags.add(tag);
                    }
                }
            }
            return redisAPI.del(Collections.singletonList(collectionsPrefix)).mapEmpty();
        });
    }

    private Future<Void> writePartitionRegistry(RedisAPI redisAPI, Set<String> tags) {
        if (tags.isEmpty()) {
            log.info("No partitions discovered, partition registry '{}' left untouched", partitionRegistryKey);
            return Future.succeededFuture();
        }
        List<String> args = new ArrayList<>();
        args.add(partitionRegistryKey);
        args.addAll(tags);
        return redisAPI.sadd(args).mapEmpty();
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    /**
     * Runs a {@code SCAN}-family cursor loop, invoking {@code scanCall} for each page (starting at
     * cursor {@code "0"}) and {@code pageHandler} with that page's raw items (member/score pairs for
     * {@code ZSCAN}, or plain keys for {@code SCAN}), advancing to the next cursor only once the
     * current page has been fully processed. Bounds memory/in-flight commands to one page at a time,
     * regardless of how large the scanned data set is.
     */
    private Future<Void> genericScan(RedisAPI redisAPI,
                                      BiFunction<String, Integer, Future<Response>> scanCall,
                                      Function<List<Response>, Future<Void>> pageHandler) {
        Promise<Void> promise = Promise.promise();
        genericScanPage(redisAPI, "0", scanCall, pageHandler, promise);
        return promise.future();
    }

    private void genericScanPage(RedisAPI redisAPI, String cursor,
                                  BiFunction<String, Integer, Future<Response>> scanCall,
                                  Function<List<Response>, Future<Void>> pageHandler,
                                  Promise<Void> promise) {
        scanCall.apply(cursor, SCAN_COUNT).onComplete(ar -> {
            if (ar.failed()) {
                promise.fail(ar.cause());
                return;
            }
            Response response = ar.result();
            String nextCursor = response.get(0).toString();
            Response items = response.get(1);
            List<Response> page = new ArrayList<>();
            if (items != null) {
                for (Response item : items) {
                    page.add(item);
                }
            }
            Future<Void> pageFuture = page.isEmpty() ? Future.succeededFuture() : pageHandler.apply(page);
            pageFuture.onComplete(pageResult -> {
                if (pageResult.failed()) {
                    promise.fail(pageResult.cause());
                } else if ("0".equals(nextCursor)) {
                    promise.complete();
                } else {
                    genericScanPage(redisAPI, nextCursor, scanCall, pageHandler, promise);
                }
            });
        });
    }

    /**
     * {@code SCAN ... MATCH <pattern>} variant of {@link #genericScan}: pages are delivered as plain
     * key name strings.
     */
    private Future<Void> scanAndProcess(RedisAPI redisAPI, String pattern, Function<List<String>, Future<Void>> pageHandler) {
        return genericScan(redisAPI,
                (cursor, count) -> redisAPI.scan(Arrays.asList(cursor, "MATCH", pattern, "COUNT", String.valueOf(count))),
                page -> {
                    List<String> keys = new ArrayList<>(page.size());
                    for (Response key : page) {
                        keys.add(key.toString());
                    }
                    return pageHandler.apply(keys);
                });
    }

    /**
     * Applies {@code action} to each item in bounded-concurrency batches of {@code batchSize}: all
     * actions within one batch are started concurrently, awaited together, and only then does the next
     * batch start. This bounds the number of in-flight Redis commands while still overlapping their
     * network round-trips, instead of paying one round-trip per item strictly sequentially.
     */
    private static <T> Future<Void> forEachBatched(List<T> items, int batchSize, Function<T, Future<Void>> action) {
        Promise<Void> promise = Promise.promise();
        advanceBatched(items, 0, batchSize, action, promise);
        return promise.future();
    }

    private static <T> void advanceBatched(List<T> items, int index, int batchSize,
                                           Function<T, Future<Void>> action, Promise<Void> promise) {
        if (index >= items.size()) {
            promise.complete();
            return;
        }
        int end = min(index + batchSize, items.size());
        List<Future<?>> batch = new ArrayList<>(end - index);
        for (int i = index; i < end; i++) {
            batch.add(action.apply(items.get(i)));
        }
        Future.all(batch).onComplete(ar -> {
            if (ar.failed()) {
                promise.fail(ar.cause());
            } else {
                advanceBatched(items, end, batchSize, action, promise);
            }
        });
    }

    private static boolean isNoSuchKey(Throwable err) {
        String message = err == null ? null : err.getMessage();
        return message != null && message.toLowerCase().contains("no such key");
    }

    private static final class Stats {
        private int renamed;
        private int skipped;
        private int expirableEntries;
    }
}
