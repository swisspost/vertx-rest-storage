package org.swisspush.reststorage;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import io.vertx.redis.client.impl.RedisClient;
import io.vertx.redis.client.impl.types.MultiType;
import io.vertx.redis.client.impl.types.NumberType;
import io.vertx.redis.client.impl.types.SimpleStringType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.swisspush.reststorage.util.ModuleConfiguration;

import java.util.Collections;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link RedisStorage} class
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
@RunWith(VertxUnitRunner.class)
public class RedisStorageTest {

    private RedisClient redisClient;
    private RedisAPI redisAPI;
    private RedisStorage storage;

    @Before
    public void setUp(TestContext context) {
        redisClient = Mockito.mock(RedisClient.class);
        redisAPI = Mockito.mock(RedisAPI.class);
        storage = new RedisStorage(mock(Vertx.class), new ModuleConfiguration(), redisClient, redisAPI);
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
                    MultiType response = MultiType.create(1,true);
                    response.add(SimpleStringType.create("data"));

                    MultiType data1 = MultiType.create(1,true);
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
                    MultiType response = MultiType.create(1,true);
                    response.add(SimpleStringType.create("memory"));

                    MultiType data1 = MultiType.create(1,true);
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
                    MultiType response = MultiType.create(1,true);
                    response.add(SimpleStringType.create("memory"));

                    MultiType data1 = MultiType.create(1,true);
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
                    MultiType response = MultiType.create(1,true);
                    response.add(SimpleStringType.create("memory"));

                    MultiType data1 = MultiType.create(1,true);
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
                    MultiType response = MultiType.create(2,true);
                    response.add(SimpleStringType.create("memory"));

                    MultiType data1 = MultiType.create(2,true);
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
                    MultiType response = MultiType.create(1,true);
                    response.add(SimpleStringType.create("memory"));

                    MultiType data1 = MultiType.create(2,true);
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
    public void testCalculateCurrentMemoryUsage(TestContext testContext) {
        Async async = testContext.async(4);

        when(redisAPI.info(eq(Collections.singletonList("memory")), any(Handler.class))).thenAnswer(invocation -> {
            ((Handler<AsyncResult<Response>>) invocation.getArguments()[1]).handle(new SuccessAsyncResult() {
                @Override
                public Response result() {
                    MultiType response = MultiType.create(1,true);
                    response.add(SimpleStringType.create("memory"));

                    MultiType data1 = MultiType.create(2,true);
                    data1.add(SimpleStringType.create("total_system_memory"));
                    data1.add(SimpleStringType.create("100"));
                    data1.add(SimpleStringType.create("used_memory"));
                    data1.add(SimpleStringType.create("75"));
                    response.add(data1);
                    return response;
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
                    MultiType response = MultiType.create(1,true);
                    response.add(SimpleStringType.create("memory"));

                    MultiType data1 = MultiType.create(2,true);
                    data1.add(SimpleStringType.create("total_system_memory"));
                    data1.add(SimpleStringType.create("100"));
                    data1.add(SimpleStringType.create("used_memory"));
                    data1.add(SimpleStringType.create("0"));
                    response.add(data1);
                    return response;
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
                    MultiType response = MultiType.create(1,true);
                    response.add(SimpleStringType.create("memory"));

                    MultiType data1 = MultiType.create(2,true);
                    data1.add(SimpleStringType.create("total_system_memory"));
                    data1.add(SimpleStringType.create("100"));
                    data1.add(SimpleStringType.create("used_memory"));
                    data1.add(SimpleStringType.create("150"));
                    response.add(data1);
                    return response;
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
                    MultiType response = MultiType.create(1,true);
                    response.add(SimpleStringType.create("memory"));

                    MultiType data1 = MultiType.create(2,true);
                    data1.add(SimpleStringType.create("total_system_memory"));
                    data1.add(SimpleStringType.create("100"));
                    data1.add(SimpleStringType.create("used_memory"));
                    data1.add(SimpleStringType.create("-20"));
                    response.add(data1);
                    return response;
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
}
