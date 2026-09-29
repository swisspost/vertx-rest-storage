package org.swisspush.reststorage.migration;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.swisspush.reststorage.JedisFactory;
import org.swisspush.reststorage.exception.RestStorageExceptionFactory;
import org.swisspush.reststorage.migration.tasks.Task;
import org.swisspush.reststorage.redis.DefaultRedisProvider;
import org.swisspush.reststorage.redis.RedisProvider;
import org.swisspush.reststorage.util.ModuleConfiguration;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Integration tests for {@link MigrateTool}'s task orchestration and distributed lock.
 * Requires a Redis listening on localhost:6379 (same prerequisite as the other integration tests).
 */
@RunWith(VertxUnitRunner.class)
public class MigrateToolTest {

    private static final String MIGRATION_LOCK_KEY = "rest-storage:migration:lock";

    private Vertx vertx;
    private Jedis jedis;
    private RedisProvider redisProvider;

    @Before
    public void setUp() {
        vertx = Vertx.vertx();
        jedis = JedisFactory.createJedis();
        jedis.flushAll();
        ModuleConfiguration config = new ModuleConfiguration()
                .storageType(ModuleConfiguration.StorageType.redis)
                .redisHost("localhost")
                .redisPort(6379);
        redisProvider = new DefaultRedisProvider(vertx, config,
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

    /** A task that records its execution order into {@code log} and then succeeds. */
    private static Task recordingTask(String key, List<String> log) {
        return new Task() {
            @Override
            public String getTaskKey() {
                return key;
            }

            @Override
            public Future<Boolean> run() {
                log.add(key);
                return Future.succeededFuture(true);
            }
        };
    }

    private static Task failingTask(String key) {
        return new Task() {
            @Override
            public String getTaskKey() {
                return key;
            }

            @Override
            public Future<Boolean> run() {
                return Future.succeededFuture(false);
            }
        };
    }

    /** Blocks the test thread until {@code future} settles, returning whether it succeeded. */
    private boolean await(TestContext context, Future<Void> future) {
        Async async = context.async();
        boolean[] succeeded = new boolean[1];
        future.onComplete(ar -> {
            succeeded[0] = ar.succeeded();
            async.complete();
        });
        async.awaitSuccess(30_000);
        return succeeded[0];
    }

    // ------------------------------------------------------------------
    // tests
    // ------------------------------------------------------------------

    @Test
    public void completesImmediatelyWhenNoTasksRegistered(TestContext context) {
        MigrateTool tool = new MigrateTool(vertx, redisProvider, "instance-1");

        context.assertTrue(await(context, tool.start()));
        // No lock must be taken when there is nothing to do.
        context.assertFalse(jedis.exists(MIGRATION_LOCK_KEY));
    }

    @Test
    public void runsTasksSequentiallyInRegistrationOrder(TestContext context) {
        List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());
        MigrateTool tool = new MigrateTool(vertx, redisProvider, "instance-1")
                .addTask(recordingTask("first", executionOrder))
                .addTask(recordingTask("second", executionOrder))
                .addTask(recordingTask("third", executionOrder));

        context.assertTrue(await(context, tool.start()));
        context.assertEquals(Arrays.asList("first", "second", "third"), new ArrayList<>(executionOrder));
    }

    @Test
    public void releasesLockAfterSuccessfulRun(TestContext context) {
        MigrateTool tool = new MigrateTool(vertx, redisProvider, "instance-1")
                .addTask(recordingTask("only", Collections.synchronizedList(new ArrayList<>())));

        context.assertTrue(await(context, tool.start()));
        context.assertFalse(jedis.exists(MIGRATION_LOCK_KEY), "lock must be released after a successful run");
    }

    @Test
    public void failingTaskFailsMigrationAndStillReleasesLock(TestContext context) {
        List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());
        MigrateTool tool = new MigrateTool(vertx, redisProvider, "instance-1")
                .addTask(recordingTask("before", executionOrder))
                .addTask(failingTask("boom"))
                .addTask(recordingTask("after", executionOrder));

        context.assertFalse(await(context, tool.start()), "a task returning false must fail the migration");
        // The task after the failing one must not have run.
        context.assertEquals(Collections.singletonList("before"), new ArrayList<>(executionOrder));
        context.assertFalse(jedis.exists(MIGRATION_LOCK_KEY), "lock must be released even when a task fails");
    }

    @Test
    public void secondInstanceWaitsInsteadOfRerunningTasks(TestContext context) {
        AtomicInteger executions = new AtomicInteger();
        Task slowTask = new Task() {
            @Override
            public String getTaskKey() {
                return "slow";
            }

            @Override
            public Future<Boolean> run() {
                executions.incrementAndGet();
                Promise<Boolean> promise = Promise.promise();
                vertx.setTimer(1_000, id -> promise.complete(true));
                return promise.future();
            }
        };

        MigrateTool toolA = new MigrateTool(vertx, redisProvider, "instance-a").addTask(slowTask);
        MigrateTool toolB = new MigrateTool(vertx, redisProvider, "instance-b").addTask(slowTask);

        Async async = context.async(2);
        toolA.start().onComplete(ar -> {
            context.assertTrue(ar.succeeded());
            async.countDown();
        });
        toolB.start().onComplete(ar -> {
            context.assertTrue(ar.succeeded());
            async.countDown();
        });
        async.awaitSuccess(30_000);

        context.assertEquals(1, executions.get(),
                "only the lock holder may run the task; the other instance must just wait");
        context.assertFalse(jedis.exists(MIGRATION_LOCK_KEY));
    }
}
