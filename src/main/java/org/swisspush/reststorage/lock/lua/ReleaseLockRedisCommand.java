package org.swisspush.reststorage.lock.lua;

import io.vertx.core.Promise;
import org.slf4j.Logger;
import org.swisspush.reststorage.exception.RestStorageExceptionFactory;
import org.swisspush.reststorage.redis.RedisProvider;

import java.util.ArrayList;
import java.util.List;

/**
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class ReleaseLockRedisCommand implements RedisCommand {

    private final LuaScriptState luaScriptState;
    private final List<String> keys;
    private final List<String> arguments;
    private final Promise<Boolean> promise;
    private final RedisProvider redisProvider;
    private final RestStorageExceptionFactory exceptionFactory;
    private final Logger log;

    public ReleaseLockRedisCommand(
        LuaScriptState luaScriptState,
        List<String> keys,
        List<String> arguments,
        RedisProvider redisProvider,
        RestStorageExceptionFactory exceptionFactory,
        Logger log,
        final Promise<Boolean> promise
    ) {
        this.luaScriptState = luaScriptState;
        this.keys = keys;
        this.arguments = arguments;
        this.redisProvider = redisProvider;
        this.exceptionFactory = exceptionFactory;
        this.log = log;
        this.promise = promise;
    }

    @Override
    public void exec(int executionCounter) {
        List<String> args = new ArrayList<>();
        args.add(luaScriptState.getSha());
        args.add(String.valueOf(keys.size()));
        args.addAll(keys);
        args.addAll(arguments);

        redisProvider.redis().onComplete( redisEv -> {
            if( redisEv.failed() ){
                promise.fail(exceptionFactory.newException("redisProvider.redis() failed", redisEv.cause()));
                return;
            }
            var redisAPI = redisEv.result();
            redisAPI.evalsha(args, shaEv -> {
                if( shaEv.failed() ){
                    Throwable ex = shaEv.cause();
                    String message = ex.getMessage();
                    if (message != null && message.startsWith("NOSCRIPT")) {
                        log.warn("ReleaseLockRedisCommand script couldn't be found, reload it", ex);
                        log.warn("amount the script got loaded: " + executionCounter);
                        if (executionCounter > 10) {
                            promise.fail("amount the script got loaded is higher than 10, we abort");
                        } else {
                            luaScriptState.loadLuaScript(new ReleaseLockRedisCommand(luaScriptState, keys,
                                    arguments, redisProvider, exceptionFactory, log, promise), executionCounter);
                        }
                    } else {
                        promise.fail(exceptionFactory.newException("redisAPI.evalsha() failed", ex));
                    }
                    return;
                }
                Long unlocked = shaEv.result().toLong();
                promise.complete(unlocked > 0);
            });
        });
    }
}
