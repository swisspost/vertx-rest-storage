package org.swisspush.reststorage.lock.lua;

/**
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public interface RedisCommand {
    void exec(int executionCounter);
}
