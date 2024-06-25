package org.swisspush.reststorage.redis;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.impl.types.BulkType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.swisspush.reststorage.util.ResourcesUtils;

import static org.mockito.Mockito.*;

/**
 * Tests for the {@link DefaultRedisReadyProvider} class
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
@RunWith(VertxUnitRunner.class)
public class DefaultRedisReadyProviderTest {

    private final String REDIS_INFO_LOADING = ResourcesUtils.loadResource("redis_info_persistance_loading_1", true);
    private final String REDIS_INFO_NOT_LOADING = ResourcesUtils.loadResource("redis_info_persistance_loading_0", true);

    private Vertx vertx;
    private RedisAPI redisAPI;
    private DefaultRedisReadyProvider readyProvider;

    @Before
    public void setUp() {
        this.vertx = Vertx.vertx();
        redisAPI = Mockito.mock(RedisAPI.class);
        readyProvider = new DefaultRedisReadyProvider(vertx, 1000);
    }

    private void assertReadiness(TestContext testContext, AsyncResult<Boolean> event, Boolean expectedReadiness) {
        testContext.assertTrue(event.succeeded());
        testContext.assertEquals(expectedReadiness, event.result());
    }

    @Test
    public void testRedisReady(TestContext testContext) {
        Async async = testContext.async();
        Mockito.when(redisAPI.info(any())).thenReturn(Future.succeededFuture(BulkType.create(Buffer.buffer(REDIS_INFO_NOT_LOADING), false)));

        readyProvider.ready(redisAPI).onComplete(event -> {
            assertReadiness(testContext, event, true);
            async.complete();
        });
    }

    @Test
    public void testRedisReadyMultipleCalls(TestContext testContext) {
        Async async = testContext.async();
        Mockito.when(redisAPI.info(any())).thenReturn(Future.succeededFuture(BulkType.create(Buffer.buffer(REDIS_INFO_NOT_LOADING), false)));

        readyProvider.ready(redisAPI).onComplete(event -> {
            assertReadiness(testContext, event, true);
            readyProvider.ready(redisAPI).onComplete(event2 -> {
                assertReadiness(testContext, event2, true);
                async.complete();
            });
        });

        verify(redisAPI, times(1)).info(any());
    }

    @Test
    public void testRedisNotReady(TestContext testContext) {
        Async async = testContext.async();
        Mockito.when(redisAPI.info(any())).thenReturn(Future.succeededFuture(BulkType.create(Buffer.buffer(REDIS_INFO_LOADING), false)));

        readyProvider.ready(redisAPI).onComplete(event -> {
            assertReadiness(testContext, event, false);
            async.complete();
        });
    }

    @Test
    public void testRedisNotReadyInvalidInfoResponse(TestContext testContext) {
        Async async = testContext.async();
        Mockito.when(redisAPI.info(any())).thenReturn(Future.succeededFuture(BulkType.create(Buffer.buffer("some invalid info response"), false)));

        readyProvider.ready(redisAPI).onComplete(event -> {
            assertReadiness(testContext, event, false);
            async.complete();
        });
    }

    @Test
    public void testRedisNotReadyExceptionWhenAccessingRedisAPI(TestContext testContext) {
        Async async = testContext.async();
        Mockito.when(redisAPI.info(any())).thenReturn(Future.failedFuture("Boooom"));

        readyProvider.ready(redisAPI).onComplete(event -> {
            assertReadiness(testContext, event, false);
            async.complete();
        });
    }
}
