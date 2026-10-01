package org.swisspush.reststorage.redis;

import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.buffer.impl.BufferImpl;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import io.vertx.redis.client.impl.types.BulkType;
import io.vertx.redis.client.impl.types.MultiType;
import io.vertx.redis.client.impl.types.NumberType;
import io.vertx.redis.client.impl.types.SimpleStringType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.swisspush.reststorage.exception.RestStorageExceptionFactory;
import org.swisspush.reststorage.util.LockMode;
import org.swisspush.reststorage.util.ModuleConfiguration;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.swisspush.reststorage.exception.RestStorageExceptionFactory.newRestStorageWastefulExceptionFactory;

/**
 * Tests for the {@link RedisStorage} class
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
@RunWith(VertxUnitRunner.class)
public class RedisStorageTest {

    private RedisAPI redisAPI;
    private RedisProvider redisProvider;
    private RedisStorage storage;
    private RestStorageExceptionFactory exceptionFactory;

    @Before
    public void setUp(TestContext context) {
        redisAPI = Mockito.mock(RedisAPI.class);
        redisProvider = Mockito.mock(RedisProvider.class);
        when(redisProvider.redis()).thenReturn(Future.succeededFuture(redisAPI));
        exceptionFactory = Mockito.spy(newRestStorageWastefulExceptionFactory());

        storage = new RedisStorage(mock(Vertx.class), new ModuleConfiguration(), redisProvider, exceptionFactory);
    }

    @Test
    public void testStorageGetWithRedisErrorCallsHandler(TestContext testContext) {
        Async async = testContext.async();

        when(redisProvider.redis()).thenReturn(Future.failedFuture("Booooom"));

        storage.get("/some/path/resource", "", 0, 100, event -> {
            String msg = "redisProvider.redis() failed";
            testContext.assertTrue(event.error);
            testContext.assertEquals(msg, event.errorMessage);

            ArgumentCaptor<Throwable> throwableArgument = ArgumentCaptor.forClass(Throwable.class);

            verify(exceptionFactory, times(1)).newException(eq(msg), throwableArgument.capture());
            testContext.assertTrue(throwableArgument.getValue().getMessage().contains("Booooom"));
            async.complete();
        });
    }

    @Test
    public void testStorageDeleteWithRedisErrorCallsHandler(TestContext testContext) {
        Async async = testContext.async();

        when(redisProvider.redis()).thenReturn(Future.failedFuture("Booooom"));

        storage.delete("/some/path/resource", "", LockMode.SILENT, 300L, true, true, event -> {
            String msg = "redisProvider.redis() failed";
            testContext.assertTrue(event.error);
            testContext.assertEquals(msg, event.errorMessage);

            ArgumentCaptor<Throwable> throwableArgument = ArgumentCaptor.forClass(Throwable.class);

            verify(exceptionFactory, times(1)).newException(eq(msg), throwableArgument.capture());
            testContext.assertTrue(throwableArgument.getValue().getMessage().contains("Booooom"));
            async.complete();
        });
    }

    @Test
    public void testStorageExpandWithRedisErrorCallsHandler(TestContext testContext) {
        Async async = testContext.async();

        when(redisProvider.redis()).thenReturn(Future.failedFuture("Booooom"));

        storage.storageExpand("/some/path/resource", "", List.of("res1", "res2", "res3"), event -> {
            String msg = "redisProvider.redis() failed";
            testContext.assertTrue(event.error);
            testContext.assertEquals(msg, event.errorMessage);

            ArgumentCaptor<Throwable> throwableArgument = ArgumentCaptor.forClass(Throwable.class);

            verify(exceptionFactory, times(1)).newException(eq(msg), throwableArgument.capture());
            testContext.assertTrue(throwableArgument.getValue().getMessage().contains("Booooom"));
            async.complete();
        });
    }

    @Test
    public void testStorageListWithRedisErrorCallsHandler(TestContext testContext) {
        Async async = testContext.async();

        when(redisProvider.redis()).thenReturn(Future.failedFuture("Booooom"));

        storage.list("/some/path", 1000, null, 0, event -> {
            String msg = "redisProvider.redis() failed";
            testContext.assertTrue(event.error);
            testContext.assertEquals(msg, event.errorMessage);
            testContext.assertEquals(Collections.emptyList(), event.paths);

            ArgumentCaptor<Throwable> throwableArgument = ArgumentCaptor.forClass(Throwable.class);

            verify(exceptionFactory, times(1)).newException(eq(msg), throwableArgument.capture());
            testContext.assertTrue(throwableArgument.getValue().getMessage().contains("Booooom"));
            async.complete();
        });
    }

    @Test
    public void testStorageListReturnsPathsWithoutLoadingResourceBodies(TestContext testContext) {
        Async async = testContext.async();

        when(redisAPI.scan(eq(Arrays.asList("0", "MATCH", "rest-storage:resources:some:path:*", "COUNT", "1000")), any(Handler.class))).thenAnswer(invocation -> {
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                @Override
                public Response result() {
                    return scanResponse("0",
                            "rest-storage:resources:some:path:b:c",
                            "rest-storage:resources:some:path:a");
                }
            });
            return null;
        });
        stubZmscoreAllActive();

        storage.list("/some/path", 1000, null, 0, event -> {
            testContext.assertFalse(event.error);
            testContext.assertTrue(event.exists);
            testContext.assertEquals(Arrays.asList("/some/path/a", "/some/path/b/c"), event.paths);
            verify(redisAPI, never()).hmget(anyList(), any(Handler.class));
            async.complete();
        });
    }

    @Test
    public void testStorageListReturnsNestedFileNamesAsDocumentPaths(TestContext testContext) {
        Async async = testContext.async();

        when(redisAPI.scan(eq(Arrays.asList("0", "MATCH", "rest-storage:resources:data:myService:vehicles:*", "COUNT", "1000")), any(Handler.class))).thenAnswer(invocation -> {
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                @Override
                public Response result() {
                    return scanResponse("0",
                            "rest-storage:resources:data:myService:vehicles:vehicle-1:components:component-1:stuff",
                            "rest-storage:resources:data:myService:vehicles:vehicle-1:components:component-1:more:more-1:a",
                            "rest-storage:resources:data:myService:vehicles:vehicle-1:components:component-1:more:more-1:b",
                            "rest-storage:resources:data:myService:vehicles:vehicle-2:components:component-2:more:more-2:a");
                }
            });
            return null;
        });
        stubZmscoreAllActive();

        storage.list("/data/myService/vehicles", 1000, null, 0, event -> {
            testContext.assertFalse(event.error);
            testContext.assertEquals(Arrays.asList(
                    "/data/myService/vehicles/vehicle-1/components/component-1/more/more-1/a",
                    "/data/myService/vehicles/vehicle-1/components/component-1/more/more-1/b",
                    "/data/myService/vehicles/vehicle-1/components/component-1/stuff",
                    "/data/myService/vehicles/vehicle-2/components/component-2/more/more-2/a"), event.paths);
            verify(redisAPI, never()).hmget(anyList(), any(Handler.class));
            async.complete();
        });
    }

    @Test
    public void testStorageListPassesClientCursorToScanAndReturnsNextCursor(TestContext testContext) {
        Async async = testContext.async();

        when(redisAPI.scan(eq(Arrays.asList("42", "MATCH", "rest-storage:resources:some:path:*", "COUNT", "1000")), any(Handler.class))).thenAnswer(invocation -> {
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                @Override
                public Response result() {
                    return scanResponse("99", "rest-storage:resources:some:path:a");
                }
            });
            return null;
        });
        stubZmscoreAllActive();

        storage.list("/some/path", 1000, null, 42, event -> {
            testContext.assertFalse(event.error);
            testContext.assertEquals(Arrays.asList("/some/path/a"), event.paths);
            testContext.assertEquals(99, event.nextCursor);
            async.complete();
        });
    }

    @Test
    public void testStorageListWithoutCursorScansFromZeroAndReportsCompletionCursor(TestContext testContext) {
        Async async = testContext.async();

        when(redisAPI.scan(eq(Arrays.asList("0", "MATCH", "rest-storage:resources:some:path:*", "COUNT", "1000")), any(Handler.class))).thenAnswer(invocation -> {
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                @Override
                public Response result() {
                    return scanResponse("0", "rest-storage:resources:some:path:a");
                }
            });
            return null;
        });
        stubZmscoreAllActive();

        storage.list("/some/path", 1000, null, 0, event -> {
            testContext.assertFalse(event.error);
            testContext.assertEquals(Arrays.asList("/some/path/a"), event.paths);
            testContext.assertEquals(0, event.nextCursor);
            async.complete();
        });
    }

    @Test
    public void testStorageListFilterIsIncludedInScanMatchPattern(TestContext testContext) {
        Async async = testContext.async();

        when(redisAPI.scan(eq(Arrays.asList("0", "MATCH", "rest-storage:resources:some:path:*more*", "COUNT", "1000")), any(Handler.class))).thenAnswer(invocation -> {
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                @Override
                public Response result() {
                    return scanResponse("0", "rest-storage:resources:some:path:more-1:a");
                }
            });
            return null;
        });
        stubZmscoreAllActive();

        storage.list("/some/path", 1000, "more", 0, event -> {
            testContext.assertFalse(event.error);
            testContext.assertEquals(Arrays.asList("/some/path/more-1/a"), event.paths);
            async.complete();
        });
    }

    @Test
    public void testStorageListDoesNotDropMatchesExceedingLimitWithinASingleScanRound(TestContext testContext) {
        // Regression test: SCAN's COUNT is only a hint to Redis - a single round can legitimately
        // return more matches than the requested limit. Since the underlying SCAN cursor has already
        // moved past those keys, truncating them here would silently and permanently drop paths -
        // including the case below where the scan reports completion (cursor "0") in the very same
        // round that exceeded the limit.
        Async async = testContext.async();

        when(redisAPI.scan(eq(Arrays.asList("0", "MATCH", "rest-storage:resources:some:path:*", "COUNT", "2")), any(Handler.class))).thenAnswer(invocation -> {
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                @Override
                public Response result() {
                    return scanResponse("0",
                            "rest-storage:resources:some:path:a",
                            "rest-storage:resources:some:path:b",
                            "rest-storage:resources:some:path:c");
                }
            });
            return null;
        });
        stubZmscoreAllActive();

        storage.list("/some/path", 2, null, 0, event -> {
            testContext.assertFalse(event.error);
            testContext.assertEquals(Arrays.asList("/some/path/a", "/some/path/b", "/some/path/c"), event.paths,
                    "all matches of this SCAN round must be reported, even though there are more than the requested limit");
            testContext.assertEquals(0, event.nextCursor);
            async.complete();
        });
    }

    @Test
    public void testStorageListChecksExpiryWithASingleBatchedZmscoreCallInsteadOfOnePerKey(TestContext testContext) {
        Async async = testContext.async();

        when(redisAPI.scan(eq(Arrays.asList("0", "MATCH", "rest-storage:resources:some:path:*", "COUNT", "1000")), any(Handler.class))).thenAnswer(invocation -> {
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                @Override
                public Response result() {
                    return scanResponse("0",
                            "rest-storage:resources:some:path:a",
                            "rest-storage:resources:some:path:b");
                }
            });
            return null;
        });
        long past = System.currentTimeMillis() - 10_000;
        when(redisAPI.zmscore(eq(Arrays.asList("rest-storage:expirable",
                "rest-storage:resources:some:path:a", "rest-storage:resources:some:path:b")), any(Handler.class)))
                .thenAnswer(invocation -> {
                    MultiType scores = MultiType.create(2, false);
                    scores.add(null);
                    scores.add(BulkType.create(BufferImpl.buffer(String.valueOf(past)), false));
                    ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                        @Override
                        public Response result() {
                            return scores;
                        }
                    });
                    return null;
                });

        storage.list("/some/path", 1000, null, 0, event -> {
            testContext.assertFalse(event.error);
            testContext.assertEquals(Arrays.asList("/some/path/a"), event.paths,
                    "path b expired in the past and must be filtered out, path a has no expiry and must be kept");
            verify(redisAPI, never()).zscore(anyString(), anyString(), any(Handler.class));
            verify(redisAPI, times(1)).zmscore(anyList(), any(Handler.class));
            async.complete();
        });
    }

    @Test
    public void testCalculateCurrentMemoryUsageRedisClientFail(TestContext testContext) {
        Async async = testContext.async();

        when(redisAPI.info(eq(Collections.singletonList("memory")), any(Handler.class))).thenAnswer(invocation -> {
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new FailAsyncResult() {
                @Override
                public Throwable cause() {
                    return new RuntimeException("Booom");
                }
            });
            return null;
        });

        storage.calculateCurrentMemoryUsage().onComplete(optionalAsyncResult -> {
            testContext.assertTrue(optionalAsyncResult.succeeded());
            testContext.assertFalse(optionalAsyncResult.result().isPresent());
            async.complete();
        });
    }

    @Test
    public void testCalculateCurrentMemoryUsageMissingMemorySection(TestContext testContext) {
        Async async = testContext.async();

        when(redisAPI.info(eq(Collections.singletonList("memory")), any(Handler.class))).thenAnswer(invocation -> {
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                @Override
                public Response result() {
                    MultiType response = MultiType.create(1, true);
                    response.add(SimpleStringType.create("data"));

                    MultiType data1 = MultiType.create(1, true);
                    data1.add(SimpleStringType.create("some_property"));
                    data1.add(SimpleStringType.create("some_value"));
                    response.add(data1);
                    return response;
                }
            });
            return null;
        });

        storage.calculateCurrentMemoryUsage().onComplete(optionalAsyncResult -> {
            testContext.assertTrue(optionalAsyncResult.succeeded());
            testContext.assertFalse(optionalAsyncResult.result().isPresent());
            async.complete();
        });
    }

    @Test
    public void testCalculateCurrentMemoryUsageMissingTotalSystemMemory(TestContext testContext) {
        Async async = testContext.async();

        when(redisAPI.info(eq(Collections.singletonList("memory")), any(Handler.class))).thenAnswer(invocation -> {
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                @Override
                public Response result() {
                    MultiType response = MultiType.create(1, true);
                    response.add(SimpleStringType.create("memory"));

                    MultiType data1 = MultiType.create(1, true);
                    data1.add(SimpleStringType.create("some_property"));
                    data1.add(SimpleStringType.create("some_value"));
                    response.add(data1);
                    return response;
                }
            });
            return null;
        });

        storage.calculateCurrentMemoryUsage().onComplete(optionalAsyncResult -> {
            testContext.assertTrue(optionalAsyncResult.succeeded());
            testContext.assertFalse(optionalAsyncResult.result().isPresent());
            async.complete();
        });
    }

    @Test
    public void testCalculateCurrentMemoryUsageTotalSystemMemoryZero(TestContext testContext) {
        Async async = testContext.async();

        when(redisAPI.info(eq(Collections.singletonList("memory")), any(Handler.class))).thenAnswer(invocation -> {
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                @Override
                public Response result() {
                    MultiType response = MultiType.create(1, true);
                    response.add(SimpleStringType.create("memory"));

                    MultiType data1 = MultiType.create(1, true);
                    data1.add(SimpleStringType.create("total_system_memory"));
                    data1.add(SimpleStringType.create("0"));
                    response.add(data1);
                    return response;
                }
            });
            return null;
        });

        storage.calculateCurrentMemoryUsage().onComplete(optionalAsyncResult -> {
            testContext.assertTrue(optionalAsyncResult.succeeded());
            testContext.assertFalse(optionalAsyncResult.result().isPresent());
            async.complete();
        });
    }

    @Test
    public void testCalculateCurrentMemoryUsageTotalSystemMemoryWrongType(TestContext testContext) {
        Async async = testContext.async();

        when(redisAPI.info(eq(Collections.singletonList("memory")), any(Handler.class))).thenAnswer(invocation -> {
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                @Override
                public Response result() {
                    MultiType response = MultiType.create(1, true);
                    response.add(SimpleStringType.create("memory"));

                    MultiType data1 = MultiType.create(1, true);
                    data1.add(SimpleStringType.create("total_system_memory"));
                    data1.add(NumberType.create(12345));
                    response.add(data1);
                    return response;
                }
            });
            return null;
        });

        storage.calculateCurrentMemoryUsage().onComplete(optionalAsyncResult -> {
            testContext.assertTrue(optionalAsyncResult.succeeded());
            testContext.assertFalse(optionalAsyncResult.result().isPresent());
            async.complete();
        });
    }

    @Test
    public void testCalculateCurrentMemoryUsageMissingUsedMemory(TestContext testContext) {
        Async async = testContext.async();

        when(redisAPI.info(eq(Collections.singletonList("memory")), any(Handler.class))).thenAnswer(invocation -> {
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                @Override
                public Response result() {
                    MultiType response = MultiType.create(2, true);
                    response.add(SimpleStringType.create("memory"));

                    MultiType data1 = MultiType.create(2, true);
                    data1.add(SimpleStringType.create("total_system_memory"));
                    data1.add(SimpleStringType.create("1000"));

                    data1.add(SimpleStringType.create("total_system_memory"));
                    data1.add(SimpleStringType.create("a_value"));
                    response.add(data1);
                    return response;
                }
            });
            return null;
        });

        storage.calculateCurrentMemoryUsage().onComplete(optionalAsyncResult -> {
            testContext.assertTrue(optionalAsyncResult.succeeded());
            testContext.assertFalse(optionalAsyncResult.result().isPresent());
            async.complete();
        });
    }

    @Test
    public void testCalculateCurrentMemoryUsageUsedMemoryWrongType(TestContext testContext) {
        Async async = testContext.async();

        when(redisAPI.info(eq(Collections.singletonList("memory")), any(Handler.class))).thenAnswer(invocation -> {
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                @Override
                public Response result() {
                    MultiType response = MultiType.create(1, true);
                    response.add(SimpleStringType.create("memory"));

                    MultiType data1 = MultiType.create(2, true);
                    data1.add(SimpleStringType.create("total_system_memory"));
                    data1.add(SimpleStringType.create("12345"));
                    data1.add(SimpleStringType.create("total_system_memory"));
                    data1.add(NumberType.create(123));
                    response.add(data1);
                    return response;
                }
            });
            return null;
        });

        storage.calculateCurrentMemoryUsage().onComplete(optionalAsyncResult -> {
            testContext.assertTrue(optionalAsyncResult.succeeded());
            testContext.assertFalse(optionalAsyncResult.result().isPresent());
            async.complete();
        });
    }

    @Test
    public void testCalculateCurrentMemoryUsageMaxmemoryFallbackWhenTotalSystemMemoryMissing(TestContext testContext) {
        Async async = testContext.async();

        when(redisAPI.info(eq(Collections.singletonList("memory")), any(Handler.class))).thenAnswer(invocation -> {
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                @Override
                public Response result() {
                    Buffer buffer = new BufferImpl();
                    buffer.appendString("used_memory:75");
                    buffer.appendString(System.lineSeparator());
                    buffer.appendString("maxmemory:100");
                    return BulkType.create(buffer, false);
                }
            });
            return null;
        });

        storage.calculateCurrentMemoryUsage().onComplete(optionalAsyncResult -> {
            testContext.assertTrue(optionalAsyncResult.succeeded());
            testContext.assertTrue(optionalAsyncResult.result().isPresent());
            testContext.assertEquals(75.0f, optionalAsyncResult.result().get());
            async.complete();
        });
    }

    @Test
    public void testCalculateCurrentMemoryUsageMaxmemoryFallbackWhenTotalSystemMemoryZero(TestContext testContext) {
        Async async = testContext.async();

        when(redisAPI.info(eq(Collections.singletonList("memory")), any(Handler.class))).thenAnswer(invocation -> {
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                @Override
                public Response result() {
                    Buffer buffer = new BufferImpl();
                    buffer.appendString("used_memory:50");
                    buffer.appendString(System.lineSeparator());
                    buffer.appendString("total_system_memory:0");
                    buffer.appendString(System.lineSeparator());
                    buffer.appendString("maxmemory:200");
                    return BulkType.create(buffer, false);
                }
            });
            return null;
        });

        storage.calculateCurrentMemoryUsage().onComplete(optionalAsyncResult -> {
            testContext.assertTrue(optionalAsyncResult.succeeded());
            testContext.assertTrue(optionalAsyncResult.result().isPresent());
            testContext.assertEquals(25.0f, optionalAsyncResult.result().get());
            async.complete();
        });
    }

    @Test
    public void testCalculateCurrentMemoryUsageBothTotalSystemMemoryAndMaxmemoryMissing(TestContext testContext) {
        Async async = testContext.async();

        when(redisAPI.info(eq(Collections.singletonList("memory")), any(Handler.class))).thenAnswer(invocation -> {
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                @Override
                public Response result() {
                    Buffer buffer = new BufferImpl();
                    buffer.appendString("used_memory:75");
                    return BulkType.create(buffer, false);
                }
            });
            return null;
        });

        storage.calculateCurrentMemoryUsage().onComplete(optionalAsyncResult -> {
            testContext.assertTrue(optionalAsyncResult.succeeded());
            testContext.assertFalse(optionalAsyncResult.result().isPresent());
            async.complete();
        });
    }

    @Test
    public void testCalculateCurrentMemoryUsageBothTotalSystemMemoryAndMaxmemoryZero(TestContext testContext) {
        Async async = testContext.async();

        when(redisAPI.info(eq(Collections.singletonList("memory")), any(Handler.class))).thenAnswer(invocation -> {
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                @Override
                public Response result() {
                    Buffer buffer = new BufferImpl();
                    buffer.appendString("used_memory:75");
                    buffer.appendString(System.lineSeparator());
                    buffer.appendString("total_system_memory:0");
                    buffer.appendString(System.lineSeparator());
                    buffer.appendString("maxmemory:0");
                    return BulkType.create(buffer, false);
                }
            });
            return null;
        });

        storage.calculateCurrentMemoryUsage().onComplete(optionalAsyncResult -> {
            testContext.assertTrue(optionalAsyncResult.succeeded());
            testContext.assertFalse(optionalAsyncResult.result().isPresent());
            async.complete();
        });
    }

    @Test
    public void testCalculateCurrentMemoryUsageTotalSystemMemoryPreferredOverMaxmemory(TestContext testContext) {
        Async async = testContext.async();

        when(redisAPI.info(eq(Collections.singletonList("memory")), any(Handler.class))).thenAnswer(invocation -> {
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                @Override
                public Response result() {
                    Buffer buffer = new BufferImpl();
                    buffer.appendString("used_memory:50");
                    buffer.appendString(System.lineSeparator());
                    buffer.appendString("total_system_memory:100");
                    buffer.appendString(System.lineSeparator());
                    buffer.appendString("maxmemory:200");
                    return BulkType.create(buffer, false);
                }
            });
            return null;
        });

        storage.calculateCurrentMemoryUsage().onComplete(optionalAsyncResult -> {
            testContext.assertTrue(optionalAsyncResult.succeeded());
            testContext.assertTrue(optionalAsyncResult.result().isPresent());
            testContext.assertEquals(50.0f, optionalAsyncResult.result().get());
            async.complete();
        });
    }

    @Test
    public void testCalculateCurrentMemoryUsageMaxmemoryEmptyValue(TestContext testContext) {
        Async async = testContext.async();

        when(redisAPI.info(eq(Collections.singletonList("memory")), any(Handler.class))).thenAnswer(invocation -> {
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                @Override
                public Response result() {
                    Buffer buffer = new BufferImpl();
                    buffer.appendString("used_memory:75");
                    buffer.appendString(System.lineSeparator());
                    buffer.appendString("maxmemory:");
                    return BulkType.create(buffer, false);
                }
            });
            return null;
        });

        storage.calculateCurrentMemoryUsage().onComplete(optionalAsyncResult -> {
            testContext.assertTrue(optionalAsyncResult.succeeded());
            testContext.assertFalse(optionalAsyncResult.result().isPresent());
            async.complete();
        });
    }

    @Test
    public void testCalculateCurrentMemoryUsageMaxmemoryNonNumeric(TestContext testContext) {
        Async async = testContext.async();

        when(redisAPI.info(eq(Collections.singletonList("memory")), any(Handler.class))).thenAnswer(invocation -> {
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                @Override
                public Response result() {
                    Buffer buffer = new BufferImpl();
                    buffer.appendString("used_memory:75");
                    buffer.appendString(System.lineSeparator());
                    buffer.appendString("maxmemory:abc");
                    return BulkType.create(buffer, false);
                }
            });
            return null;
        });

        storage.calculateCurrentMemoryUsage().onComplete(optionalAsyncResult -> {
            testContext.assertTrue(optionalAsyncResult.succeeded());
            testContext.assertFalse(optionalAsyncResult.result().isPresent());
            async.complete();
        });
    }

    @Test
    public void testCalculateCurrentMemoryUsage(TestContext testContext) {
        Async async = testContext.async(4);

        when(redisAPI.info(eq(Collections.singletonList("memory")), any(Handler.class))).thenAnswer(invocation -> {
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                @Override
                public Response result() {
                    Buffer buffer = new BufferImpl();
                    buffer.appendString("used_memory:75");
                    buffer.appendString(System.lineSeparator());
                    buffer.appendString("total_system_memory:100");
                    return BulkType.create(buffer, false);
                }
            });
            return null;
        });

        storage.calculateCurrentMemoryUsage().onComplete(optionalAsyncResult -> {
            testContext.assertTrue(optionalAsyncResult.succeeded());
            testContext.assertTrue(optionalAsyncResult.result().isPresent());
            testContext.assertEquals(75.0f, optionalAsyncResult.result().get());
            async.countDown();
        });

        when(redisAPI.info(eq(Collections.singletonList("memory")), any(Handler.class))).thenAnswer(invocation -> {
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                @Override
                public Response result() {
                    Buffer buffer = new BufferImpl();
                    buffer.appendString("used_memory:0");
                    buffer.appendString(System.lineSeparator());
                    buffer.appendString("total_system_memory:100");
                    return BulkType.create(buffer, false);
                }
            });
            return null;
        });

        storage.calculateCurrentMemoryUsage().onComplete(optionalAsyncResult -> {
            testContext.assertTrue(optionalAsyncResult.succeeded());
            testContext.assertTrue(optionalAsyncResult.result().isPresent());
            testContext.assertEquals(0.0f, optionalAsyncResult.result().get());
            async.countDown();
        });

        when(redisAPI.info(eq(Collections.singletonList("memory")), any(Handler.class))).thenAnswer(invocation -> {
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                @Override
                public Response result() {
                    Buffer buffer = new BufferImpl();
                    buffer.appendString("used_memory:100");
                    buffer.appendString(System.lineSeparator());
                    buffer.appendString("total_system_memory:100");
                    return BulkType.create(buffer, false);
                }
            });
            return null;
        });

        storage.calculateCurrentMemoryUsage().onComplete(optionalAsyncResult -> {
            testContext.assertTrue(optionalAsyncResult.succeeded());
            testContext.assertTrue(optionalAsyncResult.result().isPresent());
            testContext.assertEquals(100.0f, optionalAsyncResult.result().get());
            async.countDown();
        });

        when(redisAPI.info(eq(Collections.singletonList("memory")), any(Handler.class))).thenAnswer(invocation -> {
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                @Override
                public Response result() {
                    Buffer buffer = new BufferImpl();
                    buffer.appendString("used_memory:-20");
                    buffer.appendString(System.lineSeparator());
                    buffer.appendString("total_system_memory:100");
                    return BulkType.create(buffer, false);
                }
            });
            return null;
        });

        storage.calculateCurrentMemoryUsage().onComplete(optionalAsyncResult -> {
            testContext.assertTrue(optionalAsyncResult.succeeded());
            testContext.assertTrue(optionalAsyncResult.result().isPresent());
            testContext.assertEquals(0.0f, optionalAsyncResult.result().get());
            async.countDown();
        });

        async.awaitSuccess();
    }

    private static class SuccessAsyncResult implements AsyncResult<Response> {

        @Override
        public Response result() {
            return null;
        }

        @Override
        public Throwable cause() {
            return null;
        }

        @Override
        public boolean succeeded() {
            return true;
        }

        @Override
        public boolean failed() {
            return false;
        }
    }

    private static class FailAsyncResult implements AsyncResult<Response> {

        @Override
        public Response result() {
            return null;
        }

        @Override
        public Throwable cause() {
            return null;
        }

        @Override
        public boolean succeeded() {
            return false;
        }

        @Override
        public boolean failed() {
            return true;
        }
    }

    private static Response scanResponse(String cursor, String... keys) {
        MultiType keyResponse = MultiType.create(keys.length, false);
        for (String key : keys) {
            keyResponse.add(SimpleStringType.create(key));
        }
        MultiType response = MultiType.create(2, false);
        response.add(SimpleStringType.create(cursor));
        response.add(keyResponse);
        return response;
    }

    /**
     * Stubs {@code redisAPI.zmscore(...)} to report every requested key as not expired (null score),
     * matching the default fixture behaviour previously provided by per-key {@code zscore} stubs.
     */
    private void stubZmscoreAllActive() {
        when(redisAPI.zmscore(anyList(), any(Handler.class))).thenAnswer(invocation -> {
            List<String> args = (List<String>) invocation.getArguments()[0];
            MultiType scores = MultiType.create(args.size() - 1, false);
            for (int i = 1; i < args.size(); i++) {
                scores.add(null);
            }
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                @Override
                public Response result() {
                    return scores;
                }
            });
            return null;
        });
    }
}
