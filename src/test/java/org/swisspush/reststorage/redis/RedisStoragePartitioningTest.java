package org.swisspush.reststorage.redis;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import io.vertx.redis.client.impl.types.BulkType;
import io.vertx.redis.client.impl.types.MultiType;
import io.vertx.redis.client.impl.types.NumberType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.swisspush.reststorage.DocumentResource;
import org.swisspush.reststorage.Resource;
import org.swisspush.reststorage.exception.RestStorageExceptionFactory;
import org.swisspush.reststorage.util.ModuleConfiguration;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.*;

/**
 * Tests for the Redis Cluster path-based partitioning feature ({@code redisClusterPartitioningEnabled}).
 *
 * <p>These tests use a hand-written {@link RedisAPI} fake (built directly on the {@code send(Command, String...)}
 * method that every other {@code RedisAPI} command defaults onto) instead of Mockito, because the Mockito version
 * pinned in this project (1.10.19 / cglib) is incompatible with the JDK used to run these tests in this
 * environment (see {@code RedisStorageTest} - unrelated, pre-existing issue).</p>
 */
@RunWith(VertxUnitRunner.class)
public class RedisStoragePartitioningTest {

    private Vertx vertx;
    private RestStorageExceptionFactory exceptionFactory;

    @Before
    public void setUp() {
        vertx = Vertx.vertx();
        exceptionFactory = RestStorageExceptionFactory.newRestStorageThriftyExceptionFactory();
    }

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private static Response bulk(String s) {
        return BulkType.create(Buffer.buffer(s), false);
    }

    private static Response number(long n) {
        return NumberType.create(n);
    }

    private static Response multiResponse(Response... items) {
        MultiType m = MultiType.create(items.length, false);
        for (Response r : items) {
            m.add(r);
        }
        return m;
    }

    /**
     * Minimal, dependency-free fake of {@link RedisAPI}. Every RedisAPI command (evalsha, sadd, smembers,
     * zcount, script, ...) is a default method that ends up calling the single abstract
     * {@link #send(Command, String...)} method, so overriding just that (plus {@link #close()}) is enough
     * to intercept every command used by {@link RedisStorage}.
     */
    private static class FakeRedisAPI implements RedisAPI {
        final List<Invocation> invocations = new ArrayList<>();
        private final Function<Invocation, Response> handler;

        FakeRedisAPI(Function<Invocation, Response> handler) {
            this.handler = handler;
        }

        @Override
        public void close() {
            // no-op
        }

        @Override
        public Future<Response> send(Command command, String... args) {
            Invocation invocation = new Invocation(command, Arrays.asList(args));
            invocations.add(invocation);
            // "script exists" is always answered positively so RedisStorage's constructor-time
            // script preload never tries to actually load a script body through this fake.
            if (command == Command.SCRIPT && !invocation.args.isEmpty() && "exists".equals(invocation.args.get(0))) {
                return Future.succeededFuture(multiResponse(number(1)));
            }
            return Future.succeededFuture(handler.apply(invocation));
        }

        List<Invocation> byCommand(Command command) {
            List<Invocation> result = new ArrayList<>();
            for (Invocation invocation : invocations) {
                if (invocation.command == command) {
                    result.add(invocation);
                }
            }
            return result;
        }
    }

    private static class Invocation {
        final Command command;
        final List<String> args;

        Invocation(Command command, List<String> args) {
            this.command = command;
            this.args = args;
        }
    }

    private RedisStorage newStorage(boolean partitioningEnabled, FakeRedisAPI api) {
        ModuleConfiguration config = new ModuleConfiguration().redisClusterPartitioningEnabled(partitioningEnabled);
        RedisProvider provider = () -> Future.succeededFuture(api);
        return new RedisStorage(vertx, config, provider, exceptionFactory);
    }

    @SuppressWarnings("unchecked")
    private static <T> T invokePrivate(Object target, String methodName, Class<?>[] paramTypes, Object... args) throws Exception {
        Method m = RedisStorage.class.getDeclaredMethod(methodName, paramTypes);
        m.setAccessible(true);
        return (T) m.invoke(target, args);
    }

    // ------------------------------------------------------------------
    // PartitionContext (public class) - derivePartitionTag() / forPath()
    // ------------------------------------------------------------------

    @Test
    public void derivePartitionTagReturnsFirstSegment() {
        assertEquals("project", PartitionContext.derivePartitionTag(":project:server:test"));
        assertEquals("project", PartitionContext.derivePartitionTag("project:server:test"));
        assertEquals("a", PartitionContext.derivePartitionTag(":a"));
    }

    @Test
    public void derivePartitionTagReturnsNullForEmptyOrRootPath() {
        assertNull(PartitionContext.derivePartitionTag(""));
        assertNull(PartitionContext.derivePartitionTag(":"));
        assertNull(PartitionContext.derivePartitionTag("::"));
    }

    @Test
    public void derivePartitionTagStripsLiteralBraces() {
        assertEquals("weird", PartitionContext.derivePartitionTag(":{weird}:server"));
        // a segment consisting only of braces has nothing left -> null
        assertNull(PartitionContext.derivePartitionTag(":{}:server"));
    }

    @Test
    public void forPathDisabledLeavesKeyAndExpirableSetUnchanged() {
        PartitionContext ctx = PartitionContext.forPath(":project:server:test", false, "rest-storage:expirable");
        assertEquals(":project:server:test", ctx.getKey());
        assertEquals("rest-storage:expirable", ctx.getExpirableSetKey());
        assertNull(ctx.getTag());
    }

    @Test
    public void forPathEnabledTagsKeyAndExpirableSet() {
        PartitionContext ctx = PartitionContext.forPath(":project:server:test", true, "rest-storage:expirable");
        assertEquals(":{project}:server:test", ctx.getKey());
        assertEquals("rest-storage:expirable:{project}", ctx.getExpirableSetKey());
        assertEquals("project", ctx.getTag());
    }

    @Test
    public void forPathEnabledButRootPathFallsBackToUntagged() {
        PartitionContext ctx = PartitionContext.forPath("", true, "rest-storage:expirable");
        assertEquals("", ctx.getKey());
        assertEquals("rest-storage:expirable", ctx.getExpirableSetKey());
        assertNull(ctx.getTag());
    }

    @Test
    public void forPathEnabledPreservesLeadingSeparatorsAndHierarchy() {
        PartitionContext ctx = PartitionContext.forPath(":invoices:2024:01:doc1", true, "rest-storage:expirable");
        // Same number of ':' separated segments as before, only the first real segment is wrapped.
        assertEquals(":{invoices}:2024:01:doc1", ctx.getKey());
        assertEquals(
                countOccurrences(":invoices:2024:01:doc1", ':'),
                countOccurrences(ctx.getKey(), ':')
        );
    }

    private static long countOccurrences(String s, char c) {
        return s.chars().filter(ch -> ch == c).count();
    }

    // ------------------------------------------------------------------
    // registerPartitionTag()
    // ------------------------------------------------------------------

    @Test
    public void registerPartitionTagNoOpWhenDisabled() throws Exception {
        FakeRedisAPI api = new FakeRedisAPI(inv -> number(1));
        RedisStorage storage = newStorage(false, api);
        int before = api.invocations.size();
        invokePrivate(storage, "registerPartitionTag", new Class<?>[]{RedisAPI.class, String.class}, api, "project");
        assertEquals("no additional redis calls expected", before, api.invocations.size());
    }

    @Test
    public void registerPartitionTagNoOpWhenTagIsNull() throws Exception {
        FakeRedisAPI api = new FakeRedisAPI(inv -> number(1));
        RedisStorage storage = newStorage(true, api);
        int before = api.invocations.size();
        invokePrivate(storage, "registerPartitionTag", new Class<?>[]{RedisAPI.class, String.class}, api, null);
        assertEquals(before, api.invocations.size());
    }

    @Test
    public void registerPartitionTagSaddsTagIntoRegistryWhenEnabled() throws Exception {
        FakeRedisAPI api = new FakeRedisAPI(inv -> number(1));
        RedisStorage storage = newStorage(true, api);
        invokePrivate(storage, "registerPartitionTag", new Class<?>[]{RedisAPI.class, String.class}, api, "project");

        List<Invocation> saddCalls = api.byCommand(Command.SADD);
        assertEquals(1, saddCalls.size());
        assertEquals(Arrays.asList("rest-storage:locks-partitions", "project"), saddCalls.get(0).args);
    }

    // ------------------------------------------------------------------
    // end-to-end: get()/put()/delete()/storageExpand() key tagging
    // ------------------------------------------------------------------

    @Test
    public void getUsesTaggedKeyAndExpirableSetWhenPartitioningEnabled(TestContext context) {
        Async async = context.async();
        FakeRedisAPI api = new FakeRedisAPI(inv -> bulk("notFound"));
        RedisStorage storage = newStorage(true, api);

        storage.get("/project/server/test", null, 0, -1, resource -> {
            List<Invocation> evalshaCalls = api.byCommand(Command.EVALSHA);
            context.assertEquals(1, evalshaCalls.size());
            List<String> args = evalshaCalls.get(0).args;
            // args layout: [sha, numkeys, key1, resourcesPrefix, collectionsPrefix, expirableSet, ...]
            context.assertEquals("1", args.get(1));
            context.assertEquals(":{project}:server:test", args.get(2));
            context.assertEquals("rest-storage:expirable:{project}", args.get(5));
            async.complete();
        });
    }

    @Test
    public void getUsesPlainKeyWhenPartitioningDisabled(TestContext context) {
        Async async = context.async();
        FakeRedisAPI api = new FakeRedisAPI(inv -> bulk("notFound"));
        RedisStorage storage = newStorage(false, api);

        storage.get("/project/server/test", null, 0, -1, resource -> {
            List<Invocation> evalshaCalls = api.byCommand(Command.EVALSHA);
            context.assertEquals(1, evalshaCalls.size());
            List<String> args = evalshaCalls.get(0).args;
            context.assertEquals(":project:server:test", args.get(2));
            context.assertEquals("rest-storage:expirable", args.get(5));
            async.complete();
        });
    }

    @Test
    public void storageExpandUsesTaggedKeyWhenPartitioningEnabled(TestContext context) {
        Async async = context.async();
        FakeRedisAPI api = new FakeRedisAPI(inv -> bulk("notFound"));
        RedisStorage storage = newStorage(true, api);

        storage.storageExpand("/invoices/2024", null, List.of("doc1"), resource -> {
            List<Invocation> evalshaCalls = api.byCommand(Command.EVALSHA);
            context.assertEquals(1, evalshaCalls.size());
            List<String> args = evalshaCalls.get(0).args;
            context.assertEquals(":{invoices}:2024", args.get(2));
            async.complete();
        });
    }

    @Test
    public void deleteUsesTaggedKeyAndExpirableSetWhenPartitioningEnabled(TestContext context) {
        Async async = context.async();
        FakeRedisAPI api = new FakeRedisAPI(inv -> bulk("ok"));
        RedisStorage storage = newStorage(true, api);

        storage.delete("/project/server/test", "", org.swisspush.reststorage.util.LockMode.SILENT, 0,
                false, false, resource -> {
            List<Invocation> evalshaCalls = api.byCommand(Command.EVALSHA);
            context.assertEquals(1, evalshaCalls.size());
            List<String> args = evalshaCalls.get(0).args;
            context.assertEquals(":{project}:server:test", args.get(2));
            // expirableSet is argument index 3 (0-based) within the DELETE argument list, i.e. index 7 overall
            // (sha, numkeys, key, resourcesPrefix, collectionsPrefix, deltaResourcesPrefix, deltaEtagsPrefix, expirableSet)
            context.assertEquals("rest-storage:expirable:{project}", args.get(7));
            async.complete();
        });
    }

    @Test
    public void putRegistersPartitionTagOnSuccessWhenPartitioningEnabled(TestContext context) {
        Async async = context.async();
        FakeRedisAPI api = new FakeRedisAPI(inv -> {
            if (inv.command == Command.EVALSHA) {
                return bulk("OK");
            }
            return number(1);
        });
        RedisStorage storage = newStorage(true, api);

        storage.put("/project/server/test", "someetag", false, -1, resource -> {
            DocumentResource d = (DocumentResource) resource;
            d.endHandler = event -> {
                List<Invocation> evalshaCalls = api.byCommand(Command.EVALSHA);
                context.assertEquals(1, evalshaCalls.size());
                context.assertEquals(":{project}:server:test", evalshaCalls.get(0).args.get(2));

                List<Invocation> saddCalls = api.byCommand(Command.SADD);
                context.assertEquals(1, saddCalls.size());
                context.assertEquals(Arrays.asList("rest-storage:locks-partitions", "project"), saddCalls.get(0).args);
                async.complete();
            };
            d.closeHandler.handle(null);
        });
    }

    @Test
    public void putDoesNotRegisterPartitionTagWhenPartitioningDisabled(TestContext context) {
        Async async = context.async();
        FakeRedisAPI api = new FakeRedisAPI(inv -> {
            if (inv.command == Command.EVALSHA) {
                return bulk("OK");
            }
            return number(1);
        });
        RedisStorage storage = newStorage(false, api);

        storage.put("/project/server/test", "someetag", false, -1, resource -> {
            DocumentResource d = (DocumentResource) resource;
            d.endHandler = event -> {
                context.assertEquals(":project:server:test", api.byCommand(Command.EVALSHA).get(0).args.get(2));
                context.assertTrue(api.byCommand(Command.SADD).isEmpty());
                async.complete();
            };
            d.closeHandler.handle(null);
        });
    }

    // ------------------------------------------------------------------
    // cleanup() / cleanupAllPartitions()
    // ------------------------------------------------------------------

    @Test
    public void cleanupUsesSingleGlobalExpirableSetWhenPartitioningDisabled(TestContext context) {
        Async async = context.async();
        FakeRedisAPI api = new FakeRedisAPI(inv -> {
            if (inv.command == Command.EVALSHA) {
                return number(0); // nothing cleaned this run -> triggers zcount and completion
            }
            if (inv.command == Command.ZCOUNT) {
                return number(0);
            }
            return bulk("");
        });
        RedisStorage storage = newStorage(false, api);

        storage.cleanup(resource -> {
            // disabled path must never touch the partition registry
            context.assertTrue(api.byCommand(Command.SMEMBERS).isEmpty());
            List<Invocation> zcountCalls = api.byCommand(Command.ZCOUNT);
            context.assertEquals(1, zcountCalls.size());
            context.assertEquals("rest-storage:expirable", zcountCalls.get(0).args.get(0));
            async.complete();
        }, "100");
    }

    @Test
    public void cleanupIteratesAllRegisteredPartitionsWhenEnabled(TestContext context) {
        Async async = context.async();
        FakeRedisAPI api = new FakeRedisAPI(inv -> {
            if (inv.command == Command.SMEMBERS) {
                return multiResponse(bulk("project"), bulk("invoices"));
            }
            if (inv.command == Command.EVALSHA) {
                return number(0); // nothing cleaned this run for either partition
            }
            if (inv.command == Command.ZCOUNT) {
                String expirableSetArg = inv.args.get(0);
                if ("rest-storage:expirable:{project}".equals(expirableSetArg)) {
                    return number(3);
                } else if ("rest-storage:expirable:{invoices}".equals(expirableSetArg)) {
                    return number(2);
                }
                return number(0);
            }
            return bulk("");
        });
        RedisStorage storage = newStorage(true, api);

        storage.cleanup(resource -> {
            context.assertEquals(1, api.byCommand(Command.SMEMBERS).size());

            List<Invocation> zcountCalls = api.byCommand(Command.ZCOUNT);
            context.assertEquals(2, zcountCalls.size());

            DocumentResource d = resource;
            Buffer buf = Buffer.buffer();
            d.readStream.endHandler(nothing -> {
                JsonObject json = new JsonObject(buf.toString());
                context.assertEquals(0L, json.getLong("cleanedResources"));
                context.assertEquals(5, json.getInteger("expiredResourcesLeft"));
                async.complete();
            });
            d.readStream.handler(buf::appendBuffer);
        }, "100");
    }

    @Test
    public void cleanupWithNoRegisteredPartitionsCompletesWithZeroResult(TestContext context) {
        Async async = context.async();
        FakeRedisAPI api = new FakeRedisAPI(inv -> {
            if (inv.command == Command.SMEMBERS) {
                return multiResponse();
            }
            return bulk("");
        });
        RedisStorage storage = newStorage(true, api);

        storage.cleanup(resource -> {
            DocumentResource d = resource;
            Buffer buf = Buffer.buffer();
            d.readStream.endHandler(nothing -> {
                JsonObject json = new JsonObject(buf.toString());
                context.assertEquals(0L, json.getLong("cleanedResources"));
                context.assertEquals(0, json.getInteger("expiredResourcesLeft"));
                async.complete();
            });
            d.readStream.handler(buf::appendBuffer);
        }, "100");
    }
}
