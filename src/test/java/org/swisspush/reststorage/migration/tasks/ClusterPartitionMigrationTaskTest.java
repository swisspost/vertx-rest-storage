package org.swisspush.reststorage.migration.tasks;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.swisspush.reststorage.CollectionResource;
import org.swisspush.reststorage.DocumentResource;
import org.swisspush.reststorage.JedisFactory;
import org.swisspush.reststorage.Resource;
import org.swisspush.reststorage.exception.RestStorageExceptionFactory;
import org.swisspush.reststorage.redis.DefaultRedisProvider;
import org.swisspush.reststorage.redis.RedisProvider;
import org.swisspush.reststorage.redis.RedisStorage;
import org.swisspush.reststorage.util.ModuleConfiguration;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.nio.charset.StandardCharsets;
import java.util.Set;

/**
 * Integration tests for {@link ClusterPartitionMigrationTask}, covering every key space it rewrites
 * plus an end-to-end acceptance test proving that data written in non-cluster mode is actually
 * readable through a cluster-mode {@code RedisStorage} after the migration.
 *
 * <p>Requires a Redis listening on localhost:6379 (same prerequisite as the other integration tests).
 * The test class is deliberately <i>not</i> named {@code *IntegrationTest}, because it drives the
 * {@code redisClusterPartitioningEnabled} flag itself and must not be re-run by the surefire execution
 * that forces that flag globally.</p>
 */
@RunWith(VertxUnitRunner.class)
public class ClusterPartitionMigrationTaskTest {

    private static final String RESOURCES = "rest-storage:resources";
    private static final String COLLECTIONS = "rest-storage:collections";
    private static final String EXPIRABLE = "rest-storage:expirable";
    private static final String LOCKS = "rest-storage:locks";
    private static final String DELTA_RESOURCES = "delta:resources";
    private static final String DELTA_ETAGS = "delta:etags";
    /** Must match {@code RedisStorage}: {@code config.getLockPrefix() + "-partitions"}. */
    private static final String REGISTRY = "rest-storage:locks-partitions";
    /** Must match {@link ClusterPartitionMigrationTask#DONE_KEY}. */
    private static final String DONE_KEY = "rest-storage:migration:tasks:cluster-partition-migration:done";

    private Vertx vertx;
    private Jedis jedis;
    private RedisProvider redisProvider;

    @Before
    public void setUp() {
        vertx = Vertx.vertx();
        jedis = JedisFactory.createJedis();
        jedis.flushAll();
        redisProvider = new DefaultRedisProvider(vertx, baseConfig(),
                RestStorageExceptionFactory.newRestStorageThriftyExceptionFactory());
    }

    @After
    public void tearDown(TestContext context) {
        jedis.flushAll();
        jedis.close();
        vertx.close(context.asyncAssertSuccess());
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private ModuleConfiguration baseConfig() {
        return new ModuleConfiguration()
                .storageType(ModuleConfiguration.StorageType.redis)
                .redisHost("localhost")
                .redisPort(6379);
    }

    /** Runs the migration on the test thread and asserts it reported success. */
    private void migrate(TestContext context) {
        Async async = context.async();
        Boolean[] result = new Boolean[1];
        Throwable[] failure = new Throwable[1];
        new ClusterPartitionMigrationTask(redisProvider, baseConfig()).run().onComplete(ar -> {
            if (ar.succeeded()) {
                result[0] = ar.result();
            } else {
                failure[0] = ar.cause();
            }
            async.complete();
        });
        async.awaitSuccess(30_000);
        if (failure[0] != null) {
            context.fail(failure[0]);
        }
        context.assertEquals(Boolean.TRUE, result[0], "migration task must report success");
    }

    private RedisStorage newStorage(TestContext context, boolean partitioningEnabled) {
        RedisStorage storage = new RedisStorage(vertx,
                baseConfig().redisClusterPartitioningEnabled(partitioningEnabled),
                redisProvider,
                RestStorageExceptionFactory.newRestStorageThriftyExceptionFactory());
        // RedisStorage loads its Lua scripts asynchronously from its constructor; give that a moment
        // so the first put/get does not race the SCRIPT LOAD round-trips.
        pause(context, 1_000);
        return storage;
    }

    private void pause(TestContext context, long millis) {
        Async async = context.async();
        vertx.setTimer(millis, id -> async.complete());
        async.awaitSuccess(millis + 10_000);
    }

    private void putDocument(TestContext context, RedisStorage storage, String path, String content, String etag) {
        Async async = context.async();
        storage.put(path, etag, false, -1, resource -> {
            DocumentResource d = (DocumentResource) resource;
            d.endHandler = event -> async.complete();
            d.addErrorHandler(context::fail);
            d.writeStream.write(Buffer.buffer(content));
            d.closeHandler.handle(null);
        });
        async.awaitSuccess(30_000);
    }

    private Resource getResource(TestContext context, RedisStorage storage, String path) {
        Async async = context.async();
        Resource[] ref = new Resource[1];
        storage.get(path, null, 0, -1, resource -> {
            ref[0] = resource;
            async.complete();
        });
        async.awaitSuccess(30_000);
        return ref[0];
    }

    // ------------------------------------------------------------------
    // resources
    // ------------------------------------------------------------------

    @Test
    public void resourceKeyIsTaggedAndFieldsArePreserved(TestContext context) {
        jedis.hset(RESOURCES + ":project:server:test", "resource", "{\"foo\":\"bar\"}");
        jedis.hset(RESOURCES + ":project:server:test", "etag", "etag-1");

        migrate(context);

        context.assertFalse(jedis.exists(RESOURCES + ":project:server:test"), "untagged key must be gone");
        context.assertEquals("{\"foo\":\"bar\"}", jedis.hget(RESOURCES + ":{project}:server:test", "resource"));
        context.assertEquals("etag-1", jedis.hget(RESOURCES + ":{project}:server:test", "etag"));
    }

    /**
     * Resource payloads are stored as ISO-8859-1 strings and may be gzip-compressed, i.e. arbitrary
     * bytes. Moving keys with RENAME keeps the value server-side; this test would fail if the payload
     * were ever round-tripped through the String-based RedisAPI (which would transcode it as UTF-8).
     */
    @Test
    public void binaryResourceValueSurvivesMigrationByteForByte(TestContext context) {
        byte[] key = (RESOURCES + ":project:binary").getBytes(StandardCharsets.UTF_8);
        byte[] field = "resource".getBytes(StandardCharsets.UTF_8);
        // gzip magic bytes plus a deliberately invalid UTF-8 sequence
        byte[] value = {(byte) 0x1f, (byte) 0x8b, 0x08, 0x00, (byte) 0xC3, (byte) 0x28, (byte) 0xFF, (byte) 0xFE};
        jedis.hset(key, field, value);

        migrate(context);

        byte[] migrated = jedis.hget((RESOURCES + ":{project}:binary").getBytes(StandardCharsets.UTF_8), field);
        context.assertNotNull(migrated, "migrated binary resource must exist");
        context.assertTrue(java.util.Arrays.equals(value, migrated),
                "binary payload must survive the migration unchanged");
    }

    // ------------------------------------------------------------------
    // collections + partition registry
    // ------------------------------------------------------------------

    @Test
    public void collectionKeyIsTagged(TestContext context) {
        jedis.zadd(COLLECTIONS + ":project:server", 99_999d, "test");

        migrate(context);

        context.assertFalse(jedis.exists(COLLECTIONS + ":project:server"));
        context.assertEquals(1L, jedis.zcard(COLLECTIONS + ":{project}:server"));
        context.assertEquals(99_999d, jedis.zscore(COLLECTIONS + ":{project}:server", "test"));
    }

    @Test
    public void rootCollectionIsRemovedAndSeedsPartitionRegistry(TestContext context) {
        jedis.zadd(COLLECTIONS, 1_234d, "project");
        jedis.zadd(COLLECTIONS, 1_234d, "other");

        migrate(context);

        context.assertFalse(jedis.exists(COLLECTIONS),
                "the shared untagged root collection is never written in cluster mode and must be dropped");
        Set<String> registry = jedis.smembers(REGISTRY);
        context.assertEquals(2, registry.size());
        context.assertTrue(registry.contains("project"));
        context.assertTrue(registry.contains("other"));
    }

    @Test
    public void partitionRegistryIsMergedAndNeverItselfTagged(TestContext context) {
        jedis.sadd(REGISTRY, "preexisting");
        jedis.hset(RESOURCES + ":project:a", "resource", "x");

        migrate(context);

        Set<String> registry = jedis.smembers(REGISTRY);
        context.assertTrue(registry.contains("preexisting"), "pre-existing registry entries must be kept");
        context.assertTrue(registry.contains("project"));
        context.assertEquals("set", jedis.type(REGISTRY));
    }

    // ------------------------------------------------------------------
    // expirable set
    // ------------------------------------------------------------------

    @Test
    public void globalExpirableSetIsSplitPerPartitionWithRewrittenMembers(TestContext context) {
        long scoreA = 1_893_456_000_000L;
        long scoreB = 1_893_456_999_000L;
        jedis.zadd(EXPIRABLE, scoreA, RESOURCES + ":project:a");
        jedis.zadd(EXPIRABLE, scoreB, RESOURCES + ":other:b");

        migrate(context);

        context.assertFalse(jedis.exists(EXPIRABLE), "the global expirable set must be replaced");
        context.assertEquals(1L, jedis.zcard(EXPIRABLE + ":{project}"));
        context.assertEquals(1L, jedis.zcard(EXPIRABLE + ":{other}"));
        context.assertEquals((double) scoreA, jedis.zscore(EXPIRABLE + ":{project}", RESOURCES + ":{project}:a"),
                "member must be rewritten to its tagged form and keep its score");
        context.assertEquals((double) scoreB, jedis.zscore(EXPIRABLE + ":{other}", RESOURCES + ":{other}:b"));
    }

    // ------------------------------------------------------------------
    // locks + delta
    // ------------------------------------------------------------------

    @Test
    public void lockKeyIsTaggedAndTtlIsPreserved(TestContext context) {
        jedis.hset(LOCKS + ":project:server:test", "owner", "someowner");
        jedis.hset(LOCKS + ":project:server:test", "mode", "silent");
        jedis.pexpire(LOCKS + ":project:server:test", 60_000L);

        migrate(context);

        context.assertFalse(jedis.exists(LOCKS + ":project:server:test"));
        context.assertEquals("someowner", jedis.hget(LOCKS + ":{project}:server:test", "owner"));
        Long pttl = jedis.pttl(LOCKS + ":{project}:server:test");
        context.assertTrue(pttl > 0L && pttl <= 60_000L, "TTL must survive the rename, was " + pttl);
    }

    @Test
    public void deltaKeysAreTagged(TestContext context) {
        jedis.set(DELTA_RESOURCES + ":project:server:test", "42");
        jedis.set(DELTA_ETAGS + ":project:server:test", "etag-7");

        migrate(context);

        context.assertFalse(jedis.exists(DELTA_RESOURCES + ":project:server:test"));
        context.assertFalse(jedis.exists(DELTA_ETAGS + ":project:server:test"));
        context.assertEquals("42", jedis.get(DELTA_RESOURCES + ":{project}:server:test"));
        context.assertEquals("etag-7", jedis.get(DELTA_ETAGS + ":{project}:server:test"));
    }

    // ------------------------------------------------------------------
    // whole-keyspace behaviour
    // ------------------------------------------------------------------

    @Test
    public void allPartitionsAndNestingLevelsAreMigrated(TestContext context) {
        jedis.hset(RESOURCES + ":alpha:one", "resource", "1");
        jedis.hset(RESOURCES + ":alpha:two:three", "resource", "2");
        jedis.hset(RESOURCES + ":beta:x", "resource", "3");
        jedis.zadd(COLLECTIONS + ":alpha", 1d, "one");
        jedis.zadd(COLLECTIONS + ":alpha:two", 1d, "three");
        jedis.zadd(COLLECTIONS + ":beta", 1d, "x");
        jedis.zadd(COLLECTIONS, 1d, "alpha");
        jedis.zadd(COLLECTIONS, 1d, "beta");

        migrate(context);

        context.assertTrue(jedis.exists(RESOURCES + ":{alpha}:one"));
        context.assertTrue(jedis.exists(RESOURCES + ":{alpha}:two:three"));
        context.assertTrue(jedis.exists(RESOURCES + ":{beta}:x"));
        context.assertTrue(jedis.exists(COLLECTIONS + ":{alpha}"));
        context.assertTrue(jedis.exists(COLLECTIONS + ":{alpha}:two"));
        context.assertTrue(jedis.exists(COLLECTIONS + ":{beta}"));
        context.assertTrue(jedis.keys(RESOURCES + ":alpha*").isEmpty(), "no untagged resource key may remain");
        context.assertTrue(jedis.keys(RESOURCES + ":beta*").isEmpty(), "no untagged resource key may remain");

        Set<String> registry = jedis.smembers(REGISTRY);
        context.assertEquals(2, registry.size());
        context.assertTrue(registry.contains("alpha"));
        context.assertTrue(registry.contains("beta"));
    }

    @Test
    public void migrationIsIdempotent(TestContext context) {
        jedis.hset(RESOURCES + ":project:server:test", "resource", "{\"foo\":\"bar\"}");
        jedis.zadd(COLLECTIONS + ":project:server", 1d, "test");
        jedis.zadd(COLLECTIONS, 1d, "project");
        jedis.zadd(EXPIRABLE, 1_893_456_000_000d, RESOURCES + ":project:server:test");

        migrate(context);
        Set<String> afterFirstRun = jedis.keys("*");

        migrate(context);
        Set<String> afterSecondRun = jedis.keys("*");

        context.assertEquals(afterFirstRun, afterSecondRun, "re-running the migration must be a no-op");
        context.assertEquals("{\"foo\":\"bar\"}", jedis.hget(RESOURCES + ":{project}:server:test", "resource"));
    }

    @Test
    public void emptyKeyspaceSucceedsAndCreatesNothing(TestContext context) {
        migrate(context);

        context.assertTrue(jedis.keys("*").stream().allMatch(key -> key.equals(DONE_KEY)),
                "migrating an empty keyspace must not create a partition registry, "
                        + "the only key allowed to exist is the task's own completion flag");
    }

    // ------------------------------------------------------------------
    // completion flag (see ClusterPartitionMigrationTask#DONE_KEY)
    // ------------------------------------------------------------------

    @Test
    public void doneFlagIsSetAfterSuccessfulRun(TestContext context) {
        jedis.hset(RESOURCES + ":project:server:test", "resource", "v1");

        migrate(context);

        context.assertTrue(jedis.exists(DONE_KEY), "the completion flag must be set after a successful run");
    }

    /**
     * Distinguishes "skips entirely" from "is merely idempotent": data written to an untagged key
     * *after* the first (successful) run is left completely untouched by a second run, proving the
     * second run short-circuited on the completion flag rather than re-scanning and re-processing the
     * (now larger) key space.
     */
    @Test
    public void secondRunSkipsEntirelyOnceDoneFlagIsSet(TestContext context) {
        jedis.hset(RESOURCES + ":project:server:test", "resource", "v1");
        migrate(context);
        context.assertTrue(jedis.exists(DONE_KEY), "done flag must be set after the first successful run");

        jedis.hset(RESOURCES + ":other:server:test", "resource", "v2");

        migrate(context);

        context.assertTrue(jedis.exists(RESOURCES + ":other:server:test"),
                "a run skipped due to the completion flag must not touch data written afterwards");
        context.assertFalse(jedis.exists(RESOURCES + ":{other}:server:test"),
                "the untagged key must remain untouched, proving the second run was skipped, not merely idempotent");
    }

    // ------------------------------------------------------------------
    // batching / multi-page SCAN + ZSCAN (see ClusterPartitionMigrationTask#genericScan,
    // #scanAndProcess, #forEachBatched)
    // ------------------------------------------------------------------

    /**
     * Resource keys are migrated via a {@code SCAN} cursor loop that processes one page (bounded by
     * {@code SCAN_COUNT = 1000}) at a time, renaming keys within a page in concurrent batches of
     * {@code BATCH_SIZE = 50}. This writes enough keys to force at least three {@code SCAN} pages and
     * several dozen rename batches per page, and asserts that every single key still survives exactly
     * once - a bug in the cursor loop (e.g. an off-by-one on the "0" terminal cursor) or in the batch
     * runner (e.g. dropping/duplicating an item at a batch boundary) would surface as a missing or
     * duplicated key here.
     */
    @Test
    public void largeResourceKeyspaceSpanningMultipleScanPagesIsFullyMigrated(TestContext context) {
        int totalKeys = 2_500; // > SCAN_COUNT (1000): forces multiple SCAN pages
        String[] partitions = {"alpha", "beta", "gamma"};
        Pipeline pipeline = jedis.pipelined();
        for (int i = 0; i < totalKeys; i++) {
            String partition = partitions[i % partitions.length];
            pipeline.hset(RESOURCES + ":" + partition + ":item" + i, "resource", "v" + i);
        }
        pipeline.sync();

        migrate(context);

        long taggedCount = 0;
        for (String partition : partitions) {
            context.assertTrue(jedis.keys(RESOURCES + ":" + partition + ":*").isEmpty(),
                    "no untagged key may remain for partition '" + partition + "'");
            taggedCount += jedis.keys(RESOURCES + ":{" + partition + "}:*").size();
        }
        context.assertEquals((long) totalKeys, taggedCount,
                "every key must survive the multi-page, batched migration exactly once");

        Set<String> registry = jedis.smembers(REGISTRY);
        context.assertEquals(partitions.length, registry.size());
        for (String partition : partitions) {
            context.assertTrue(registry.contains(partition));
        }
    }

    /**
     * Same concern as {@link #largeResourceKeyspaceSpanningMultipleScanPagesIsFullyMigrated}, but for
     * the expirable set, which used to be read in one go via {@code ZRANGE 0 -1 WITHSCORES} and is now
     * paged via {@code ZSCAN}. Writes enough members to force several {@code ZSCAN} pages and asserts
     * every entry is redistributed to its per-partition ZSET exactly once.
     */
    @Test
    public void largeExpirableSetSpanningMultipleZscanPagesIsFullyRedistributed(TestContext context) {
        int totalEntries = 2_500; // > SCAN_COUNT (1000): forces multiple ZSCAN pages
        String[] partitions = {"alpha", "beta", "gamma"};
        Pipeline pipeline = jedis.pipelined();
        for (int i = 0; i < totalEntries; i++) {
            String partition = partitions[i % partitions.length];
            pipeline.zadd(EXPIRABLE, (double) i, RESOURCES + ":" + partition + ":item" + i);
        }
        pipeline.sync();

        migrate(context);

        context.assertFalse(jedis.exists(EXPIRABLE), "the global expirable set must be replaced");
        long total = 0;
        for (String partition : partitions) {
            total += jedis.zcard(EXPIRABLE + ":{" + partition + "}");
        }
        context.assertEquals((long) totalEntries, total,
                "every expirable entry must survive the multi-page, batched ZSCAN redistribution exactly once");
    }

    // ------------------------------------------------------------------
    // end-to-end acceptance
    // ------------------------------------------------------------------

    /**
     * The test that actually matters: write through a non-cluster {@code RedisStorage}, migrate, then
     * read the very same paths back through a cluster-mode {@code RedisStorage}. This covers both the
     * per-resource key tagging and the partition registry that root listings depend on.
     */
    @Test
    public void dataWrittenInNonClusterModeIsReadableInClusterModeAfterMigration(TestContext context) {
        String body = "{\"hello\":\"world\"}";
        RedisStorage nonClusterStorage = newStorage(context, false);
        putDocument(context, nonClusterStorage, "/project/server/test", body, "etag-42");

        // sanity: the plain, untagged layout was used
        context.assertTrue(jedis.exists(RESOURCES + ":project:server:test"));
        context.assertTrue(jedis.exists(COLLECTIONS));

        migrate(context);

        context.assertTrue(jedis.exists(RESOURCES + ":{project}:server:test"));
        context.assertFalse(jedis.exists(RESOURCES + ":project:server:test"));

        RedisStorage clusterStorage = newStorage(context, true);

        Resource document = getResource(context, clusterStorage, "/project/server/test");
        context.assertTrue(document.exists, "migrated document must be found in cluster mode");
        context.assertTrue(document instanceof DocumentResource);
        context.assertEquals("etag-42", ((DocumentResource) document).etag);
        context.assertEquals((long) body.length(), ((DocumentResource) document).length);

        Resource root = getResource(context, clusterStorage, "/");
        context.assertTrue(root instanceof CollectionResource,
                "root listing must be served from the migrated partition registry");
        CollectionResource rootCollection = (CollectionResource) root;
        context.assertEquals(1, rootCollection.items.size());
        context.assertEquals("project", rootCollection.items.get(0).name);
    }
}
