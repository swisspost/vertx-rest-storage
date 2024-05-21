package org.swisspush.reststorage.lock.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.swisspush.reststorage.exception.RestStorageExceptionFactory;
import org.swisspush.reststorage.redis.RedisProvider;
import org.swisspush.reststorage.lock.Lock;
import org.swisspush.reststorage.lock.lua.LockLuaScripts;
import org.swisspush.reststorage.lock.lua.LuaScriptState;
import org.swisspush.reststorage.lock.lua.ReleaseLockRedisCommand;
import org.swisspush.reststorage.redis.RedisUtils;
import org.swisspush.reststorage.util.FailedAsyncResult;

import java.util.Collections;
import java.util.List;

/**
 * Implementation of the {@link Lock} interface based on a redis database.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class RedisBasedLock implements Lock {

    private final Logger log = LoggerFactory.getLogger(RedisBasedLock.class);

    public static final String STORAGE_PREFIX = "rest-storage-lock:";
    private static final String[] EMPTY_STRING_ARRAY = new String[0];

    private final LuaScriptState releaseLockLuaScriptState;
    private final RedisProvider redisProvider;
    private final RestStorageExceptionFactory exceptionFactory;

    public RedisBasedLock(
        RedisProvider redisProvider,
        RestStorageExceptionFactory exceptionFactory
    ) {
        this.redisProvider = redisProvider;
        this.exceptionFactory = exceptionFactory;
        this.releaseLockLuaScriptState = new LuaScriptState(LockLuaScripts.LOCK_RELEASE, redisProvider, false);
    }

    private void redisSetWithOptions(String key, String value, boolean nx, long px, Handler<AsyncResult<Response>> handler) {
        JsonArray options = new JsonArray();
        options.add("PX").add(px);
        if (nx) {
            options.add("NX");
        }
        redisProvider.redis().onComplete( redisEv -> {
            if( redisEv.failed() ){
                handler.handle(new FailedAsyncResult<>(new Exception("redisProvider.redis()", redisEv.cause())));
                return;
            }
            var redisAPI = redisEv.result();
            String[] payload = RedisUtils.toPayload(key, value, options).toArray(EMPTY_STRING_ARRAY);
            redisAPI.send(Command.SET, payload).onComplete( ev -> {
                if( ev.failed() ){
                    handler.handle(new FailedAsyncResult<>(new Exception("redisAPI.send(SET, ...)", ev.cause())));
                }else{
                    handler.handle(ev);
                }
            });
        });
    }

    @Override
    public Future<Boolean> acquireLock(String lock, String token, long lockExpiryMs) {
        Promise<Boolean> promise = Promise.promise();
        redisSetWithOptions(buildLockKey(lock), token, true, lockExpiryMs, event -> {
            if (event.succeeded()) {
                if (event.result() != null) {
                    promise.complete("OK".equalsIgnoreCase(event.result().toString()));
                } else {
                    promise.complete(false);
                }
            } else {
                promise.fail(exceptionFactory.newException("redisSetWithOptions() failed", event.cause()));
            }
        });
        return promise.future();
    }

    @Override
    public Future<Boolean> releaseLock(String lock, String token) {
        Promise<Boolean> promise = Promise.promise();
        List<String> keys = Collections.singletonList(buildLockKey(lock));
        List<String> arguments = Collections.singletonList(token);
        ReleaseLockRedisCommand cmd = new ReleaseLockRedisCommand(releaseLockLuaScriptState,
                keys, arguments, redisProvider, log, promise);
        cmd.exec(0);
        return promise.future();
    }

    private String buildLockKey(String lock) {
        return STORAGE_PREFIX + lock;
    }
}
