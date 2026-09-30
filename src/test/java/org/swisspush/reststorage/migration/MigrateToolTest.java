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

    @Test
    public void refreshAndReleaseNeverTouchALockTakenOverByAnotherInstance(TestContext context) {
        // Simulates a stalled instance whose lock TTL expired and was legitimately re-acquired by a
        // different instance (different token) while our task was still running. Neither the periodic
        // refresh nor the final release must ever mutate that other instance's lock.
        String foreignToken = "instance-foreign:takeover-token";
        Task longRunningTask = new Task() {
            @Override
            public String getTaskKey() {
                return "long-running";
            }

            @Override
            public Future<Boolean> run() {
                Promise<Boolean> promise = Promise.promise();
                // Simulate the takeover shortly after our own lock was acquired, well before our
                // first refresh tick (refresh interval is 2s), and keep running past it so the
                // refresh timer has a chance to observe the mismatch.
                vertx.setTimer(300, id -> {
                    jedis.set(MIGRATION_LOCK_KEY, foreignToken);
                    jedis.pexpire(MIGRATION_LOCK_KEY, 30_000);
                });
                vertx.setTimer(3_500, id -> promise.complete(true));
                return promise.future();
            }
        };

        MigrateTool tool = new MigrateTool(vertx, redisProvider, "instance-a").addTask(longRunningTask);

        context.assertFalse(await(context, tool.start()),
                "migration result must not be trusted once lock ownership was lost mid-run");
        context.assertEquals(foreignToken, jedis.get(MIGRATION_LOCK_KEY),
                "the other instance's lock (different token) must survive both our refresh and release");
    }

    /** Wraps a real {@link RedisProvider}, letting the first {@code allowedCalls} calls through and failing every
     *  call after that - used to simulate a Redis connection failure kicking in partway through a flow. */
    private static RedisProvider failingAfter(RedisProvider delegate, int allowedCalls) {
        AtomicInteger callCount = new AtomicInteger();
        return () -> {
            if (callCount.getAndIncrement() >= allowedCalls) {
                return Future.failedFuture(new RuntimeException("simulated redis connection failure"));
            }
            return delegate.redis();
        };
    }

    @Test
    public void failsWhenPollingForOtherInstancesMigrationCompletionFails(TestContext context) {
        Task slowTask = new Task() {
            @Override
            public String getTaskKey() {
                return "slow";
            }

            @Override
            public Future<Boolean> run() {
                Promise<Boolean> promise = Promise.promise();
                vertx.setTimer(3_000, id -> promise.complete(true));
                return promise.future();
            }
        };

        // instance-a genuinely acquires and holds the lock for a while.
        MigrateTool toolA = new MigrateTool(vertx, redisProvider, "instance-a").addTask(slowTask);
        // instance-b's provider allows exactly one call (its own, failed, acquireLock attempt) and then
        // fails every subsequent call - i.e. every call made while polling for instance-a's completion.
        RedisProvider flakyProvider = failingAfter(redisProvider, 1);
        MigrateTool toolBFlaky = new MigrateTool(vertx, flakyProvider, "instance-b").addTask(slowTask);

        Async async = context.async(2);
        toolA.start().onComplete(ar -> {
            context.assertTrue(ar.succeeded());
            async.countDown();
        });
        toolBFlaky.start().onComplete(ar -> {
            context.assertTrue(ar.failed(),
                    "a Redis failure while waiting for another instance's migration must not be reported as success");
            async.countDown();
        });
        async.awaitSuccess(30_000);
    }
}
