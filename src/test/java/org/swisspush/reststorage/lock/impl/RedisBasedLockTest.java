package org.swisspush.reststorage.lock.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.redis.client.*;
import io.vertx.redis.client.impl.RedisClient;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.swisspush.reststorage.exception.RestStorageExceptionFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.time.Duration;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.awaitility.Awaitility.await;
import static org.swisspush.reststorage.exception.RestStorageExceptionFactory.newRestStorageWastefulExceptionFactory;

/**
 * Tests for the {@link RedisBasedLock} class
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
@RunWith(VertxUnitRunner.class)
public class RedisBasedLockTest {

    private static Vertx vertx;
    private Jedis jedis;
    private static RedisBasedLock redisBasedLock;

    String lock_1 = "lock_1";
    String token_1 = "token_1";

    @org.junit.Rule
    public Timeout rule = Timeout.seconds(5);

    @BeforeClass
    public static void setupLock(){
        vertx = Vertx.vertx();
        RedisAPI redisAPI = RedisAPI.api(new RedisClient(vertx, new NetClientOptions(), new PoolOptions(), new RedisStandaloneConnectOptions(), TracingPolicy.IGNORE));
        redisBasedLock = new RedisBasedLock(() -> Future.succeededFuture(redisAPI), newRestStorageWastefulExceptionFactory());
    }

    @Before
    public void setUp(){
        jedis = new Jedis("localhost");
        try {
            jedis.flushAll();
        } catch (JedisConnectionException e){
            org.junit.Assume.assumeNoException("Ignoring this test because no running redis is available. This is the case during release", e);
        }
    }

    @After
    public void tearDown(){
        if(jedis != null){
            jedis.close();
        }
    }

    private String lockKey(String lock){
        return RedisBasedLock.STORAGE_PREFIX + lock;
    }

    @Test
    public void testAcquireLock(TestContext context){
        Async async = context.async();
        context.assertFalse(jedis.exists(lockKey(lock_1)));
        redisBasedLock.acquireLock(lock_1, token_1, 500).onComplete(event -> {
            context.assertTrue(event.succeeded());
            context.assertTrue(event.result());
            context.assertTrue(jedis.exists(lockKey(lock_1)));
            context.assertEquals(token_1, jedis.get(lockKey(lock_1)));
            async.complete();
        });
    }

    @Test
    public void testAcquireMultipleLocks(TestContext context){
        Async async = context.async();
        context.assertFalse(jedis.exists(lockKey(lock_1)));
        redisBasedLock.acquireLock(lock_1, token_1, 500).onComplete(event -> {
            context.assertTrue(event.succeeded());
            context.assertTrue(event.result());
            context.assertTrue(jedis.exists(lockKey(lock_1)));
            context.assertEquals(token_1, jedis.get(lockKey(lock_1)));
            redisBasedLock.acquireLock("lock_2", "token_X", 300).onComplete(event1 -> {
                context.assertTrue(event1.succeeded());
                context.assertTrue(event1.result());
                context.assertTrue(jedis.exists(lockKey("lock_2")));
                context.assertEquals("token_X", jedis.get(lockKey("lock_2")));
                redisBasedLock.acquireLock("lock_3", "token_Z", 300).onComplete(event2 -> {
                    context.assertTrue(event2.succeeded());
                    context.assertTrue(event2.result());
                    context.assertTrue(jedis.exists(lockKey("lock_3")));
                    context.assertEquals("token_Z", jedis.get(lockKey("lock_3")));
                    async.complete();
                });
            });
        });
    }

    @Test
    public void testAcquireLockAgain(TestContext context){
        Async async = context.async();
        context.assertFalse(jedis.exists(lockKey(lock_1)));
        redisBasedLock.acquireLock(lock_1, token_1, 300).onComplete(event -> {
            context.assertTrue(event.succeeded());
            context.assertTrue(event.result());
            context.assertTrue(jedis.exists(lockKey(lock_1)));
            context.assertEquals(token_1, jedis.get(lockKey(lock_1)));
            redisBasedLock.acquireLock(lock_1, "token_X", 350).onComplete(event1 -> {
                context.assertTrue(event1.succeeded());
                context.assertFalse(event1.result());
                context.assertTrue(jedis.exists(lockKey(lock_1)));
                context.assertEquals(token_1, jedis.get(lockKey(lock_1)));
                async.complete();
            });
        });
    }

    @Test
    public void testAcquireLockAfterExpired(TestContext context){
        Async async = context.async();
        context.assertFalse(jedis.exists(lockKey(lock_1)));
        redisBasedLock.acquireLock(lock_1, token_1, 300).onComplete(event -> {
            context.assertTrue(event.succeeded());
            context.assertTrue(event.result());
            context.assertTrue(jedis.exists(lockKey(lock_1)));
            context.assertEquals(token_1, jedis.get(lockKey(lock_1)));
            waitMaxUntilExpired(lockKey(lock_1), 350);
            redisBasedLock.acquireLock(lock_1, "token_X", 500).onComplete(event1 -> {
                context.assertTrue(event1.succeeded());
                context.assertTrue(event1.result());
                context.assertTrue(jedis.exists(lockKey(lock_1)));
                context.assertEquals("token_X", jedis.get(lockKey(lock_1)));
                async.complete();
            });
        });
    }

    @Test
    public void testReleaseNonExistingLock(TestContext context){
        Async async = context.async();
        context.assertFalse(jedis.exists(lockKey(lock_1)));
        redisBasedLock.releaseLock(lock_1, token_1).onComplete(event -> {
            context.assertTrue(event.succeeded());
            context.assertFalse(event.result());
            context.assertFalse(jedis.exists(lockKey(lock_1)));
            async.complete();
        });
    }

    @Test
    public void testReleaseExpiredLock(TestContext context){
        Async async = context.async();
        context.assertFalse(jedis.exists(lockKey(lock_1)));
        redisBasedLock.acquireLock(lock_1, token_1, 500).onComplete(event -> {
            context.assertTrue(event.succeeded());
            context.assertTrue(event.result());
            context.assertTrue(jedis.exists(lockKey(lock_1)));
            context.assertEquals(token_1, jedis.get(lockKey(lock_1)));
            waitMaxUntilExpired(lockKey(lock_1), 600);
            redisBasedLock.releaseLock(lock_1, token_1).onComplete(event1 -> {
                context.assertTrue(event1.succeeded());
                context.assertFalse(event1.result());
                context.assertFalse(jedis.exists(lockKey(lock_1)));
                async.complete();
            });
        });
    }

    @Test
    public void testReleaseExistingLockWithCorrectToken(TestContext context){
        Async async = context.async();
        context.assertFalse(jedis.exists(lockKey(lock_1)));
        redisBasedLock.acquireLock(lock_1, token_1, 500).onComplete(event -> {
            context.assertTrue(event.succeeded());
            context.assertTrue(event.result());
            context.assertTrue(jedis.exists(lockKey(lock_1)));
            context.assertEquals(token_1, jedis.get(lockKey(lock_1)));
            redisBasedLock.releaseLock(lock_1, token_1).onComplete(event1 -> {
                context.assertTrue(event1.succeeded());
                context.assertTrue(event1.result());
                context.assertFalse(jedis.exists(lockKey(lock_1)));
                async.complete();
            });
        });
    }

    @Test
    public void testReleaseExistingLockWithWrongToken(TestContext context){
        Async async = context.async();
        context.assertFalse(jedis.exists(lockKey(lock_1)));
        redisBasedLock.acquireLock(lock_1, token_1, 500).onComplete(event -> {
            context.assertTrue(event.succeeded());
            context.assertTrue(event.result());
            context.assertTrue(jedis.exists(lockKey(lock_1)));
            context.assertEquals(token_1, jedis.get(lockKey(lock_1)));
            redisBasedLock.releaseLock(lock_1, "token_X").onComplete(event1 -> {
                context.assertTrue(event1.succeeded());
                context.assertFalse(event1.result());
                context.assertTrue(jedis.exists(lockKey(lock_1)));
                context.assertEquals(token_1, jedis.get(lockKey(lock_1)));
                async.complete();
            });
        });
    }

    @Test
    public void testReleaseLockRespectingOwnership(TestContext context){
        Async async = context.async();
        context.assertFalse(jedis.exists(lockKey(lock_1)));
        redisBasedLock.acquireLock(lock_1, token_1, 500).onComplete(event -> {
            context.assertTrue(event.succeeded());
            context.assertTrue(event.result());
            context.assertTrue(jedis.exists(lockKey(lock_1)));
            context.assertEquals(token_1, jedis.get(lockKey(lock_1)));
            waitMaxUntilExpired(lockKey(lock_1), 600);
            context.assertFalse(jedis.exists(lockKey(lock_1)));
            redisBasedLock.acquireLock(lock_1, "token_X", 350).onComplete(event1 -> {
                context.assertTrue(event1.succeeded());
                context.assertTrue(event1.result());
                context.assertTrue(jedis.exists(lockKey(lock_1)));
                context.assertEquals("token_X", jedis.get(lockKey(lock_1)));
                redisBasedLock.releaseLock(lock_1, token_1).onComplete(event2 -> {
                    context.assertTrue(event2.succeeded());
                    context.assertFalse(event2.result());
                    context.assertTrue(jedis.exists(lockKey(lock_1)));
                    context.assertEquals("token_X", jedis.get(lockKey(lock_1)));
                    redisBasedLock.releaseLock(lock_1, "token_X").onComplete(event3 -> {
                        context.assertTrue(event3.succeeded());
                        context.assertTrue(event3.result());
                        context.assertFalse(jedis.exists(lockKey(lock_1)));
                        async.complete();
                    });
                });
            });
        });
    }

    private void waitMaxUntilExpired(String key, long expireMs){
        await().pollInterval(50, MILLISECONDS).atMost(Duration.ofMillis(expireMs)).until(() -> !jedis.exists(key));
    }
}
