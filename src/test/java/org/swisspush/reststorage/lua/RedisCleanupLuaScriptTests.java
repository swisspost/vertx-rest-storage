package org.swisspush.reststorage.lua;

import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class RedisCleanupLuaScriptTests extends AbstractLuaScriptTest {

    private static final double MAX_EXPIRE_IN_MILLIS = 99999999999999d;

    @Test
    public void cleanupAllExpiredAmount2() throws InterruptedException {

        // ARRANGE
        String now = String.valueOf(System.currentTimeMillis());
        evalScriptPutNoReturn(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}", now);
        evalScriptPutNoReturn(":project:server:test:test11:test22", "{\"content\": \"test/test1/test2\"}", now);
        Thread.sleep(10);

        // ACT
        Long count = (Long) evalScriptCleanup(0, System.currentTimeMillis());

        // ASSERT
        assertThat(count, equalTo(2L));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1:test2"), equalTo(false));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test11", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test11:test22"), equalTo(false));
    }

    @Test
    public void cleanupOneExpiredAmount2() throws InterruptedException {

        // ARRANGE
        String now = String.valueOf(System.currentTimeMillis());
        String nowPlus1000sec = String.valueOf((System.currentTimeMillis() + 1000000));
        evalScriptPutNoReturn(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}", now);
        evalScriptPutNoReturn(":project:server:test:test11:test22", "{\"content\": \"test/test1/test2\"}", nowPlus1000sec);
        Thread.sleep(1000);

        // ACT

        // evalScriptDel(":project:server:test:test1:test2", MAX_EXPIRE_IN_MILLIS);
        Long count = (Long) evalScriptCleanup(0, System.currentTimeMillis());

        // ASSERT
        assertThat(count, equalTo(1L));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1:test2"), equalTo(false));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test11", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test11:test22"), equalTo(true));
    }

    @Test
    public void cleanup15ExpiredAmount30Bulksize10() throws InterruptedException {

        // ARRANGE
        String now = String.valueOf(System.currentTimeMillis());
        String maxExpire = String.valueOf(MAX_EXPIRE_IN_MILLIS);
        for (int i = 1; i <= 30; i++) {
            evalScriptPutNoReturn(":project:server:test:test1:test" + i, "{\"content\": \"test" + i + "\"}", i % 2 == 0 ? now : maxExpire);
        }
        Thread.sleep(10);

        // ACT
        Long count1round = (Long) evalScriptCleanup(0, System.currentTimeMillis(), 10);
        Long count2round = (Long) evalScriptCleanup(0, System.currentTimeMillis(), 10);

        // ASSERT
        assertThat(count1round, equalTo(10L));
        assertThat(count2round, equalTo(5L));
        assertThat(jedis.zcount("rest-storage:collections:project:server:test:test1", 0d, MAX_EXPIRE_IN_MILLIS), equalTo(15L));
    }

    @Test
    public void cleanup7000ExpiredAmount21000Bulksize1000() throws InterruptedException {

        // ARRANGE
        String now = String.valueOf(System.currentTimeMillis());
        String maxExpire = String.valueOf(MAX_EXPIRE_IN_MILLIS);
        for (int i = 1; i <= 21000; i++) {
            evalScriptPutNoReturn(":project:server:test:test1:test" + i, "{\"content\": \"test" + i + "\"}", i % 3 == 0 ? now : maxExpire);
        }
        Thread.sleep(100);

        // ACT
        long start = System.currentTimeMillis();
        Long count1round = (Long) evalScriptCleanup(0, System.currentTimeMillis(), 1000, true);
        Long count2round = (Long) evalScriptCleanup(0, System.currentTimeMillis(), 1000, true);
        Long count3round = (Long) evalScriptCleanup(0, System.currentTimeMillis(), 1000, true);
        Long count4round = (Long) evalScriptCleanup(0, System.currentTimeMillis(), 1000, true);
        Long count5round = (Long) evalScriptCleanup(0, System.currentTimeMillis(), 1000, true);
        Long count6round = (Long) evalScriptCleanup(0, System.currentTimeMillis(), 1000, true);
        Long count7round = (Long) evalScriptCleanup(0, System.currentTimeMillis(), 1000, true);
        long end = System.currentTimeMillis();

        System.out.println("clean 7K: " + DurationFormatUtils.formatDuration(end - start, "HH:mm:ss:SSS"));

        // ASSERT
        assertThat(count1round, equalTo(1000L));
        assertThat(count2round, equalTo(1000L));
        assertThat(count3round, equalTo(1000L));
        assertThat(count4round, equalTo(1000L));
        assertThat(count5round, equalTo(1000L));
        assertThat(count6round, equalTo(1000L));
        assertThat(count7round, equalTo(1000L));
        assertThat(jedis.zcount("rest-storage:collections:project:server:test:test1", 0d, MAX_EXPIRE_IN_MILLIS), equalTo(14000L));

    }

    @Test
    public void cleanupTwoExpiredButOneResourceAlreadyDeleted() {

        // ARRANGE
        String now = String.valueOf(System.currentTimeMillis());
        evalScriptPutNoReturn(":project:server:test:test1", "{\"content\": \"test/test1/test1\"}", now);
        evalScriptPutNoReturn(":project:server:test:test2", "{\"content\": \"test/test1/test2\"}", now);

        // delete bypassing the script, results in an orphan entry in the expirableSet
        assertThat(jedis.del("rest-storage:resources:project:server:test:test1"), equalTo(1L));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1"), equalTo(false));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test2"), equalTo(true));
        assertThat(jedis.zcount("rest-storage:expirable", 0, MAX_EXPIRE_IN_MILLIS), equalTo(2L));

        // ACT
        Long count = (Long) evalScriptCleanup(0, System.currentTimeMillis());

        // ASSERT
        // refer to https://github.com/swisspush/vertx-rest-storage/issues/83, we actually deleted only 1 but still say 2
        assertThat(count, equalTo(2L));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1"), equalTo(false));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test2"), equalTo(false));
        assertThat(jedis.zcount("rest-storage:expirable", 0, MAX_EXPIRE_IN_MILLIS), equalTo(0L));
    }

    @Ignore
    @Test
    public void cleanup1000000ExpiredAmount2000000Bulksize1000() throws InterruptedException {

        // ARRANGE
        // check the amount already written: zcount rest-storage:collections:project:server:test:test1 0 99999999999999
        // the dump.rdb file had the size of 315M after the 2M inserts
        String now = String.valueOf(System.currentTimeMillis());
        String maxExpire = String.valueOf(MAX_EXPIRE_IN_MILLIS);
        for (int i = 1; i <= 2000000; i++) {
            evalScriptPut(":project:server:test:test1:test" + i, "{\"content\": \"test" + i + "\"}", i % 2 == 0 ? now : maxExpire);
        }
        Thread.sleep(10);

        // ACT
        long start = System.currentTimeMillis();
        long count = 1;
        while (count > 1) {
            count = (Long) evalScriptCleanup(0, System.currentTimeMillis(), 1000, true);
        }
        long end = System.currentTimeMillis();

        System.out.println("clean 1M: " + DurationFormatUtils.formatDuration(end - start, "HH:mm:ss:SSS"));

        // ASSERT
        assertThat(jedis.zcount("rest-storage:collections:project:server:test:test1", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS), equalTo(1000000L));
    }

    private Object evalScriptCleanup(final long minscore, final long now) {
        return evalScriptCleanup(minscore, now, 1000, false);
    }

    private Object evalScriptCleanup(final long minscore, final long now, final int bulkSize) {
        return evalScriptCleanup(minscore, now, bulkSize, false);
    }

    @SuppressWarnings({ "rawtypes", "unchecked", "serial" })
    private Object evalScriptCleanup(final long minscore, final long now, final int bulkSize, final boolean stripLogNotice) {

        Map<String, String> values = new HashMap<>();
        values.put("delscript", readScript("del.lua", stripLogNotice).replaceAll("return", "--return"));

        StrSubstitutor sub = new StrSubstitutor(values, "--%(", ")");
        String cleanupScript = sub.replace(readScript("cleanup.lua", stripLogNotice));
        return jedis.eval(cleanupScript, new ArrayList(), new ArrayList() {
                    {
                        add(prefixResources);
                        add(prefixCollections);
                        add(prefixDeltaResources);
                        add(prefixDeltaEtags);
                        add(expirableSet);
                        add(String.valueOf(minscore));
                        add(String.valueOf(MAX_EXPIRE_IN_MILLIS));
                        add("false");
                        add("true");
                        add(String.valueOf(now));
                        add(String.valueOf(bulkSize));

                    }
                }
        );
    }

    @SuppressWarnings({ "rawtypes", "unchecked", "serial" })
    private void evalScriptPutNoReturn(final String resourceName, final String resourceValue, final String expire) {
        String putScript = readScript("put.lua", true);
        jedis.eval(putScript, new ArrayList() {
                    {
                        add(resourceName);
                    }
                }, new ArrayList() {
                    {
                        add(prefixResources);
                        add(prefixCollections);
                        add(expirableSet);
                        add("false");
                        add(expire);
                        add("99999999999999");
                        add(resourceValue);
                        add(UUID.randomUUID().toString());
                        add(prefixLock);
                    }
                }
        );
    }
}
