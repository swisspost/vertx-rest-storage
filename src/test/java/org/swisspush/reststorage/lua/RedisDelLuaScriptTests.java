package org.swisspush.reststorage.lua;

import org.junit.Test;
import org.swisspush.reststorage.util.LockMode;

import java.util.ArrayList;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class RedisDelLuaScriptTests extends AbstractLuaScriptTest {

    private final static String RESOURCE = "resource";
    private static final String MAX_EXPIRE_IN_MILLIS_STR = "99999999999999";
    private static final double MAX_EXPIRE_IN_MILLIS = Double.parseDouble(MAX_EXPIRE_IN_MILLIS_STR);

    @Test
    public void deleteResource2BranchesDeleteOnRootNode() {

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");
        evalScriptPut(":project:server:test:test11:test22", "{\"content\": \"test/test1/test2\"}");

        // ACT
        evalScriptDel(":project");

        // ASSERT
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test11:test2"), equalTo(false));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test11", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test11:test22"), equalTo(false));
    }

    @Test
    public void deleteResource2BranchesDeleteOnForkNode() {

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");
        evalScriptPut(":project:server:test:test11:test22", "{\"content\": \"test/test1/test2\"}");

        // ACT
        evalScriptDel(":project:server:test");

        // ASSERT
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", System.currentTimeMillis(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test11:test2"), equalTo(false));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test11", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test11:test22"), equalTo(false));
    }

    @Test
    public void deleteResource2BranchesDeleteOneLevelAboveBranch() {

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");
        evalScriptPut(":project:server:test:test11:test22", "{\"content\": \"test/test1/test2\"}");

        // ACT
        evalScriptDel(":project:server:test:test1");

        // ASSERT
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).iterator().next(), equalTo("server"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).iterator().next(), equalTo("test"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).iterator().next(), equalTo("test11"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test11", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).iterator().next(), equalTo("test22"));
        assertThat(jedis.hget("rest-storage:resources:project:server:test:test11:test22", RESOURCE), equalTo("{\"content\": \"test/test1/test2\"}"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1:test2"), equalTo(false));
    }

    @Test
    public void deleteResource2BranchesDeleteOnOneResource() {

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");
        evalScriptPut(":project:server:test:test11:test22", "{\"content\": \"test/test1/test2\"}");

        // ACT
        evalScriptDel(":project:server:test:test1:test2");

        // ASSERT
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).iterator().next(), equalTo("server"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).iterator().next(), equalTo("test"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).iterator().next(), equalTo("test11"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test11", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).iterator().next(), equalTo("test22"));
        assertThat(jedis.hget("rest-storage:resources:project:server:test:test11:test22", RESOURCE), equalTo("{\"content\": \"test/test1/test2\"}"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test11:test2"), equalTo(false));
    }

    @Test
    public void deleteResource2BranchesDeleteOnBothResources() {

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");
        evalScriptPut(":project:server:test:test11:test22", "{\"content\": \"test/test1/test2\"}");

        // ACT
        evalScriptDel(":project:server:test:test1:test2");
        evalScriptDel(":project:server:test:test11:test22");

        // ASSERT
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1:test2"), equalTo(false));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test11", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test11:test22"), equalTo(false));
    }

    @Test
    public void deleteExpiredResourceWithMaxScoreAtMax() throws InterruptedException {

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}", MAX_EXPIRE_IN_MILLIS_STR);
        Thread.sleep(10);

        // ACT
        evalScriptDel(":project:server:test:test1:test2");

        // ASSERT
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1:test2"), equalTo(false));

        String valueGet = (String) evalScriptGet(":project:server:test:test1:test2", getNowAsString());
        assertThat(valueGet, equalTo("notFound"));

    }

    @Test
    public void deleteExpiredResourceWithMaxScoreNow() throws InterruptedException {

        // ARRANGE
        String now = String.valueOf(System.currentTimeMillis());
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}", now);
        Thread.sleep(10);

        // ACT
        long after = System.currentTimeMillis();
        evalScriptDel(":project:server:test:test1:test2", after);

        // ASSERT
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1:test2"), equalTo(true));

        String valueGet = (String) evalScriptGet(":project:server:test:test1:test2", getNowAsString());
        assertThat(valueGet, equalTo("notFound"));
    }

    @Test
    public void deleteExpiredResourceOfTwo() throws InterruptedException {

        // ARRANGE
        String now = String.valueOf(System.currentTimeMillis());
        String nowPlus1000sec = String.valueOf((System.currentTimeMillis() + 1000000));
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}", now);
        evalScriptPut(":project:server:test:test11:test22", "{\"content\": \"test/test1/test2\"}", nowPlus1000sec);
        Thread.sleep(10);

        // ACT
        long after = System.currentTimeMillis();
        evalScriptDel(":project:server:test:test1:test2", after);

        // ASSERT
        String afterNow = String.valueOf(System.currentTimeMillis());
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", Double.parseDouble(afterNow), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", Double.parseDouble(afterNow), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", Double.parseDouble(afterNow), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test11", Double.parseDouble(afterNow), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test11:test22"), equalTo(true));
    }

    @Test
    public void deleteNotExpiredResourceOfTwo() throws InterruptedException {

        // ARRANGE
        String now = String.valueOf(System.currentTimeMillis());
        String nowPlus1000sec = String.valueOf((System.currentTimeMillis() + 1000000));
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}", now);
        evalScriptPut(":project:server:test:test11:test22", "{\"content\": \"test/test1/test2\"}", nowPlus1000sec);
        Thread.sleep(10);

        // ACT
        long after = System.currentTimeMillis();
        evalScriptDel(":project:server:test:test11:test22", after);

        // ASSERT
        assertThat(jedis.exists("rest-storage:collections:project"), equalTo(true));
        assertThat(jedis.exists("rest-storage:collections:project:server"), equalTo(true));
        assertThat(jedis.exists("rest-storage:collections:project:server:test"), equalTo(true));
        assertThat(jedis.exists("rest-storage:collections:project:server:test:test11"), equalTo(false));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test11:test22"), equalTo(false));
    }

    @Test
    public void deleteSilentLockedResourceWithoutOwnership() {
        // ARRANGE
        String path = ":project:lock:delete:";
        String content = "{\"content\":\"locked\"}";

        // ACT - 1 (without any owner)
        evalScriptPut(path, content, MAX_EXPIRE, "etag", "test", LockMode.SILENT, 10);
        String value = evalScriptDel(path, "", LockMode.SILENT, 10);

        // ASSERT - 1
        assertThat(value, equalTo("silent"));
        assertThat(jedis.exists(prefixLock + path), equalTo(true));

        // ACT - 2 (with different owner)
        value = evalScriptDel(path, "test2", LockMode.SILENT, 10);

        // ASSERT - 2
        assertThat(value, equalTo("silent"));
        assertThat(jedis.exists(prefixLock + path), equalTo(true));
    }

    @Test
    public void deleteSilentLockedResourceWithOwnership() {
        // ARRANGE
        String path = ":project:lock:delete:";
        String content = "{\"content\":\"locked\"}";

        // ACT
        evalScriptPut(path, content, MAX_EXPIRE, "etag", "test", LockMode.SILENT, 10);
        assertThat(jedis.exists("rest-storage:resources" + path), equalTo(true));
        String value = evalScriptDel(path, "test", LockMode.SILENT, 10);

        // ASSERT
        assertThat(value, equalTo("deleted"));
        assertThat(jedis.exists(prefixLock + path), equalTo(true));
        assertThat(jedis.exists("rest-storage:resources" + path), equalTo(false));
    }

    @Test
    public void deleteRejectLockedResourceWithoutOwnership() {
        // ARRANGE
        String path = ":project:lock:delete:";
        String content = "{\"content\":\"locked\"}";

        // ACT - 1 (without any owner)
        evalScriptPut(path, content, MAX_EXPIRE, "etag", "test", LockMode.REJECT, 10);
        String value = evalScriptDel(path, "", LockMode.REJECT, 10);

        // ASSERT - 1
        assertThat(value, equalTo("reject"));
        assertThat(jedis.exists(prefixLock + path), equalTo(true));

        // ACT - 2 (with different owner)
        value = evalScriptDel(path, "test2", LockMode.REJECT, 10);

        // ASSERT - 2
        assertThat(value, equalTo("reject"));
        assertThat(jedis.exists(prefixLock + path), equalTo(true));
    }

    @Test
    public void deleteRejectLockedResourceWithOwnership() {
        // ARRANGE
        String path = ":project:lock:delete:";
        String content = "{\"content\":\"locked\"}";

        // ACT
        evalScriptPut(path, content, MAX_EXPIRE, "etag", "test", LockMode.REJECT, 10);
        assertThat(jedis.exists("rest-storage:resources" + path), equalTo(true));
        String value = evalScriptDel(path, "test", LockMode.REJECT, 10);

        // ASSERT
        assertThat(value, equalTo("deleted"));
        assertThat(jedis.exists(prefixLock + path), equalTo(true));
        assertThat(jedis.exists("rest-storage:resources" + path), equalTo(false));
    }

    @Test
    public void deleteCollectionWithLockedRessource() {
        // ARRANGE
        String base = ":project:lock:delete:collection";
        String path1 = base + ":res1";
        String path2 = base + ":res2";
        String path3 = base + ":res3";

        String content1 = "{\"content\":\"1\"}";
        String content2 = "{\"content\":\"locked\"}";
        String content3 = "{\"content\":\"3\"}";

        // ACT
        evalScriptPut(path1, content1, MAX_EXPIRE);
        evalScriptPut(path2, content2, MAX_EXPIRE, "etag", "test", LockMode.REJECT, 10);
        evalScriptPut(path3, content3, MAX_EXPIRE);

        assertThat(jedis.exists("rest-storage:resources" + path1), equalTo(true));
        assertThat(jedis.exists("rest-storage:resources" + path2), equalTo(true));
        assertThat(jedis.exists("rest-storage:resources" + path3), equalTo(true));

        String value = (String) evalScriptDel(base);

        // ASSERT
        assertThat(value, equalTo("deleted"));
        assertThat(jedis.exists(prefixLock + path2), equalTo(false));
        assertThat(jedis.exists("rest-storage:resources" + path1), equalTo(false));
        assertThat(jedis.exists("rest-storage:resources" + path2), equalTo(false));
        assertThat(jedis.exists("rest-storage:resources" + path3), equalTo(false));
    }

    @Test
    public void tryToDeleteWhileAndAfterResourceLock() {
        // ARRANGE
        long lockExpire = 5;
        String path = ":project:lock:delete:";
        String content = "{\"content\":\"locked\"}";

        // ACT
        jedis.expire(prefixLock +  path, 0);
        String lockedValue = evalScriptPut(path, content, AbstractLuaScriptTest.MAX_EXPIRE, "etag", "owner1", LockMode.SILENT, lockExpire);
        assertThat(lockedValue, is(equalTo("OK")));
        assertThat(jedis.hget("rest-storage:resources" + path, RESOURCE), equalTo(content));

        // ASSERT
        await().atMost(lockExpire * 2, SECONDS).until(() ->
                evalScriptDel(path, "", LockMode.SILENT, 10),
                equalTo("deleted")
        );

        assertThat(jedis.exists(prefixLock + path), equalTo(false));
    }

    @Test
    public void testNoConfirmCollectionDeleteAndNotDeleteRecursive() {

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");

        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1:test2"), equalTo(true));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));

        // ACT -> delete on level collection
        String value = (String) evalScriptDel(":project:server:test:test1", false, false);

        // ASSERT
        assertThat(value, equalTo("deleted"));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1:test2"), equalTo(false));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");

        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1:test2"), equalTo(true));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));

        // ACT -> delete on level resource
        value = (String) evalScriptDel(":project:server:test:test1:test2", false, false);

        // ASSERT
        assertThat(value, equalTo("deleted"));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1:test2"), equalTo(false));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
    }

    @Test
    public void testNoConfirmCollectionDeleteAndDeleteRecursive() {

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");

        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1:test2"), equalTo(true));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));

        // ACT -> delete on level collection
        String value = (String) evalScriptDel(":project:server:test:test1", false, true);

        // ASSERT
        assertThat(value, equalTo("deleted"));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1:test2"), equalTo(false));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");

        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1:test2"), equalTo(true));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));

        // ACT -> delete on level resource
        value = (String) evalScriptDel(":project:server:test:test1:test2", false, true);

        // ASSERT
        assertThat(value, equalTo("deleted"));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1:test2"), equalTo(false));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
    }

    @Test
    public void testConfirmCollectionDeleteAndNotDeleteRecursive() {

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");

        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1:test2"), equalTo(true));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));

        // ACT -> delete on level collection
        String value = (String) evalScriptDel(":project:server:test:test1", true, false);

        // ASSERT
        assertThat(value, equalTo("notEmpty"));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1:test2"), equalTo(true));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");

        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1:test2"), equalTo(true));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));

        // ACT -> delete on level resource
        value = (String) evalScriptDel(":project:server:test:test1:test2", true, false);

        // ASSERT
        assertThat(value, equalTo("deleted"));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1:test2"), equalTo(false));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
    }

    @Test
    public void testConfirmCollectionDeleteAndDeleteRecursive() {

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");

        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1:test2"), equalTo(true));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));

        // ACT -> delete on level collection
        String value = (String) evalScriptDel(":project:server:test:test1", true, true);

        // ASSERT
        assertThat(value, equalTo("deleted"));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1:test2"), equalTo(false));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));

        // ARRANGE
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");

        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1:test2"), equalTo(true));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(1));

        // ACT -> delete on level resource
        value = (String) evalScriptDel(":project:server:test:test1:test2", true, true);

        // ASSERT
        assertThat(value, equalTo("deleted"));
        assertThat(jedis.exists("rest-storage:resources:project:server:test:test1:test2"), equalTo(false));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", getNowAsDouble(), MAX_EXPIRE_IN_MILLIS).size(), equalTo(0));
    }


    @SuppressWarnings({ "rawtypes", "unchecked", "serial" })
    private String evalScriptDel(final String resourceName, final String lockOwner, LockMode lockMode, long lockExpire) {
        String delScript = readScript("del.lua");
        String lockExpireInMillis = String.valueOf(System.currentTimeMillis() + (lockExpire * 1000));
        return (String) jedis.eval(delScript, new ArrayList() {
            {
                add(resourceName);
            }
        },
                new ArrayList() {
                    {
                        add(prefixResources);
                        add(prefixCollections);
                        add(prefixDeltaResources);
                        add(prefixDeltaEtags);
                        add(expirableSet);
                        add("0");
                        add(MAX_EXPIRE_IN_MILLIS_STR);
                        add("false");
                        add("false");
                        add(prefixLock);
                        add(lockOwner);
                        add(lockMode.text());
                        add(lockExpireInMillis);
                    }
                }
        );
    }


    @SuppressWarnings({ "rawtypes", "unchecked", "serial" })
    private Object evalScriptDel(final String resourceName) {
        String delScript = readScript("del.lua");
        return jedis.eval(delScript, new ArrayList() {
            {
                add(resourceName);
            }
        },
                new ArrayList() {
                    {
                        add(prefixResources);
                        add(prefixCollections);
                        add(prefixDeltaResources);
                        add(prefixDeltaEtags);
                        add(expirableSet);
                        add("0");
                        add(MAX_EXPIRE_IN_MILLIS_STR);
                        add("false");
                        add("false");
                        add(prefixLock);
                    }
                }
        );
    }

    @SuppressWarnings({ "rawtypes", "unchecked", "serial" })
    private Object evalScriptDel(final String resourceName, boolean confirmCollectionDelete, boolean deleteRecursive) {
        String delScript = readScript("del.lua");
        return jedis.eval(delScript, new ArrayList() {
                    {
                        add(resourceName);
                    }
                },
                new ArrayList() {
                    {
                        add(prefixResources);
                        add(prefixCollections);
                        add(prefixDeltaResources);
                        add(prefixDeltaEtags);
                        add(expirableSet);
                        add("0");
                        add(MAX_EXPIRE_IN_MILLIS_STR);
                        add(confirmCollectionDelete ? "true" : "false");
                        add(deleteRecursive ? "true" : "false");
                        add(prefixLock);
                    }
                }
        );
    }

    @SuppressWarnings({ "rawtypes", "unchecked", "serial" })
    private Object evalScriptDel(final String resourceName, final long maxscore) {
        String delScript = readScript("del.lua");
        return jedis.eval(delScript, new ArrayList() {
            {
                add(resourceName);
            }
        }, new ArrayList() {
            {
                add(prefixResources);
                add(prefixCollections);
                add(prefixDeltaResources);
                add(prefixDeltaEtags);
                add(expirableSet);
                add(getNowAsString());
                add(String.valueOf(MAX_EXPIRE_IN_MILLIS));
                add("false");
                add("false");
                add(prefixLock);
            }
        }
        );
    }
}
