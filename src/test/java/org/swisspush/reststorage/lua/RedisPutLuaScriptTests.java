package org.swisspush.reststorage.lua;

import io.vertx.core.json.JsonObject;
import org.junit.Test;
import org.swisspush.reststorage.util.LockMode;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class RedisPutLuaScriptTests extends AbstractLuaScriptTest {

    private final static String COMPRESSED = "compressed";
    private final static String RESOURCE = "resource";
    private final static String ETAG = "etag";
    private final static String OWNER = "owner";
    private final static String MODE = "mode";

    private static final double MIN_SCORE = 0d;
    private static final double MAX_SCORE = 99999999999999d;

    @Test
    public void putResourcePathDepthIs3() {

        // ACT
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");

        // ASSERT
        assertThat("server", equalTo(jedis.zrangeByScore("rest-storage:collections:project", MIN_SCORE, MAX_SCORE).iterator().next()));
        assertThat("test", equalTo(jedis.zrangeByScore("rest-storage:collections:project:server", MIN_SCORE, MAX_SCORE).iterator().next()));
        assertThat("test1", equalTo(jedis.zrangeByScore("rest-storage:collections:project:server:test", MIN_SCORE, MAX_SCORE).iterator().next()));
        assertThat("test2", equalTo(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", MIN_SCORE, MAX_SCORE).iterator().next()));
        assertThat("{\"content\": \"test/test1/test2\"}", equalTo(jedis.hget("rest-storage:resources:project:server:test:test1:test2", RESOURCE)));
    }

    @Test
    public void putResourcePathDepthIs3WithSiblings() {

        // ACT
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");
        evalScriptPut(":project:server:test:test11", "{\"content\": \"test/test11\"}");

        // ASSERT
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", MIN_SCORE, MAX_SCORE).iterator().next(), equalTo("server"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", MIN_SCORE, MAX_SCORE).iterator().next(), equalTo("test"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", MIN_SCORE, MAX_SCORE).iterator().next(), equalTo("test1"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", MIN_SCORE, MAX_SCORE).iterator().next(), equalTo("test2"));
        assertThat(jedis.hget("rest-storage:resources:project:server:test:test1:test2", RESOURCE), equalTo("{\"content\": \"test/test1/test2\"}"));
    }

    @Test
    public void putResourcePathDepthIs3WithSiblingsFolderAndDocument() {

        // ACT
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");

        // ASSERT
        assertThat(jedis.zrangeByScore("rest-storage:collections:project", MIN_SCORE, MAX_SCORE).iterator().next(), equalTo("server"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server", MIN_SCORE, MAX_SCORE).iterator().next(), equalTo("test"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test", MIN_SCORE, MAX_SCORE).iterator().next(), equalTo("test1"));
        assertThat(jedis.zrangeByScore("rest-storage:collections:project:server:test:test1", MIN_SCORE, MAX_SCORE).iterator().next(), equalTo("test2"));
        assertThat(jedis.hget("rest-storage:resources:project:server:test:test1:test2", RESOURCE), equalTo("{\"content\": \"test/test1/test2\"}"));
    }

    @Test
    public void putResourceMergeOnEmpty() {

        // ACT
        evalScriptPutMerge(":project:server:test:test1:test2", "{\"content\": \"test_test1_test3\"}");

        // ASSERT
        String result = jedis.hget("rest-storage:resources:project:server:test:test1:test2", RESOURCE);
        JsonObject obj = new JsonObject(result);
        assertThat(obj.getString("content"), equalTo("test_test1_test3"));
    }

    @Test
    public void putResourceMergeOnExisting() {

        // ACT
        evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test_test1_test2\"}");
        evalScriptPutMerge(":project:server:test:test1:test2", "{\"content\": \"test_test1_test3\"}");

        // ASSERT
        String result = jedis.hget("rest-storage:resources:project:server:test:test1:test2", RESOURCE);
        JsonObject obj = new JsonObject(result);
        assertThat(obj.getString("content"), equalTo("test_test1_test3"));
    }

    @Test
    public void testStoreCompressedAndUnCompressedWithSameEtagValue() {

        String originalContent = "{\"content\": \"originalContent\"}";
        String modifiedContent = "{\"content\": \"modifiedContent\"}";
        String etagValue = "etag1";

        // Scenario 1: PUT 1 = {uncompressed, etag1}, PUT 2 = {compressed, etag1}
        String putResult1 = evalScriptPut(":project:server:test:test1:test2", originalContent, AbstractLuaScriptTest.MAX_EXPIRE, etagValue, false);
        assertThat(jedis.hexists("rest-storage:resources:project:server:test:test1:test2", COMPRESSED), is(false));
        String putResult2 = evalScriptPut(":project:server:test:test1:test2", modifiedContent, AbstractLuaScriptTest.MAX_EXPIRE, etagValue, true);
        assertThat(jedis.hexists("rest-storage:resources:project:server:test:test1:test2", COMPRESSED), is(true));

        assertThat(putResult1, is(not(equalTo("notModified"))));
        assertThat(putResult2, is(not(equalTo("notModified"))));
        assertThat(jedis.hget("rest-storage:resources:project:server:test:test1:test2", ETAG), equalTo(etagValue));
        assertThat(jedis.hget("rest-storage:resources:project:server:test:test1:test2", RESOURCE), equalTo(modifiedContent));

        // Scenario 2: PUT 1 = {compressed, etag1}, PUT 2 = {uncompressed, etag1}
        jedis.flushAll();
        putResult1 = evalScriptPut(":project:server:test:test1:test2", originalContent, AbstractLuaScriptTest.MAX_EXPIRE, etagValue, true);
        assertThat(jedis.hexists("rest-storage:resources:project:server:test:test1:test2", COMPRESSED), is(true));
        putResult2 = evalScriptPut(":project:server:test:test1:test2", modifiedContent, AbstractLuaScriptTest.MAX_EXPIRE, etagValue, false);
        assertThat(jedis.hexists("rest-storage:resources:project:server:test:test1:test2", COMPRESSED), is(false));

        assertThat(putResult1, is(not(equalTo("notModified"))));
        assertThat(putResult2, is(not(equalTo("notModified"))));
        assertThat(jedis.hget("rest-storage:resources:project:server:test:test1:test2", ETAG), equalTo(etagValue));
        assertThat(jedis.hget("rest-storage:resources:project:server:test:test1:test2", RESOURCE), equalTo(modifiedContent));

        // Scenario 3: PUT 1 = {compressed, etag1}, PUT 2 = {compressed, etag1}
        jedis.flushAll();
        putResult1 = evalScriptPut(":project:server:test:test1:test2", originalContent, AbstractLuaScriptTest.MAX_EXPIRE, etagValue, true);
        assertThat(jedis.hexists("rest-storage:resources:project:server:test:test1:test2", COMPRESSED), is(true));
        putResult2 = evalScriptPut(":project:server:test:test1:test2", modifiedContent, AbstractLuaScriptTest.MAX_EXPIRE, etagValue, true);
        assertThat(jedis.hexists("rest-storage:resources:project:server:test:test1:test2", COMPRESSED), is(true));

        assertThat(putResult1, is(not(equalTo("notModified"))));
        assertThat(putResult2, is(equalTo("notModified")));
        assertThat(jedis.hget("rest-storage:resources:project:server:test:test1:test2", ETAG), equalTo(etagValue));
        assertThat(jedis.hget("rest-storage:resources:project:server:test:test1:test2", RESOURCE), equalTo(originalContent));

        // Scenario 4: PUT 1 = {uncompressed, etag1}, PUT 2 = {uncompressed, etag1}
        jedis.flushAll();
        putResult1 = evalScriptPut(":project:server:test:test1:test2", originalContent, AbstractLuaScriptTest.MAX_EXPIRE, etagValue, false);
        assertThat(jedis.hexists("rest-storage:resources:project:server:test:test1:test2", COMPRESSED), is(false));
        putResult2 = evalScriptPut(":project:server:test:test1:test2", modifiedContent, AbstractLuaScriptTest.MAX_EXPIRE, etagValue, false);
        assertThat(jedis.hexists("rest-storage:resources:project:server:test:test1:test2", COMPRESSED), is(false));

        assertThat(putResult1, is(not(equalTo("notModified"))));
        assertThat(putResult2, is(equalTo("notModified")));
        assertThat(jedis.hget("rest-storage:resources:project:server:test:test1:test2", ETAG), equalTo(etagValue));
        assertThat(jedis.hget("rest-storage:resources:project:server:test:test1:test2", RESOURCE), equalTo(originalContent));
    }

    @Test
    public void putResourceWithProvidedEtagValue() {

        // ACT
        String originalContent = "{\"content\": \"originalContent\"}";
        String value = evalScriptPut(":project:server:test:test1:test2", originalContent, AbstractLuaScriptTest.MAX_EXPIRE, "myFancyEtagValue");
        String modifiedContent = "{\"content\": \"modifiedContent\"}";
        String value2 = evalScriptPut(":project:server:test:test1:test2", modifiedContent, AbstractLuaScriptTest.MAX_EXPIRE, "myFancyEtagValue");

        // ASSERT
        assertThat(value, is(not(equalTo("notModified"))));
        assertThat(value2, is(equalTo("notModified")));

        assertThat(jedis.hget("rest-storage:resources:project:server:test:test1:test2", ETAG), equalTo("myFancyEtagValue"));
        assertThat(jedis.hget("rest-storage:resources:project:server:test:test1:test2", RESOURCE), equalTo(originalContent));
    }

    @Test
    public void putResourceWithNotProvidedEtagValue() {

        // ACT
        String originalContent = "{\"content\": \"originalContent\"}";
        String value = evalScriptPut(":project:server:test:test1:test2", originalContent);

        // ASSERT
        assertThat(value, is(not(equalTo("notModified"))));
        String etagValue = jedis.hget("rest-storage:resources:project:server:test:test1:test2", ETAG);
        assertThat(jedis.hget("rest-storage:resources:project:server:test:test1:test2", RESOURCE), equalTo(originalContent));

        // ACT
        String modifiedContent = "{\"content\": \"modifiedContent\"}";
        String value2 = evalScriptPut(":project:server:test:test1:test2", modifiedContent);

        // ASSERT
        assertThat(value2, is(not(equalTo("notModified"))));
        assertThat(jedis.hget("rest-storage:resources:project:server:test:test1:test2", RESOURCE), equalTo(modifiedContent));
        assertThat(jedis.hget("rest-storage:resources:project:server:test:test1:test2", ETAG), is(not(equalTo(etagValue))));
    }

    @Test
    public void tryToPutResourceOnCollectionPathOneLevelBelow() {

        // ARRANGE
        String resultPutTest2 = evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");
        String resultPutTest1 = evalScriptPut(":project:server:test:test1", "{\"content\": \"test/test1\"}");

        // ACT
        List<String> valueTest1 = (List<String>) evalScriptGet(":project:server:test:test1");
        List<String> valueTest2 = (List<String>) evalScriptGet(":project:server:test:test1:test2");

        // ASSERT
        assertThat(resultPutTest2, equalTo("OK"));
        assertThat(resultPutTest1, equalTo("existingCollection"));
        assertThat(valueTest1, hasItem("test2"));
        assertThat(valueTest2.get(1), equalTo("{\"content\": \"test/test1/test2\"}"));
    }

    @Test
    public void tryToPutResourceOnCollectionPathTwoLevelsBelow() {

        // ARRANGE
        String resultPutTest2 = evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");
        String resultPutTest1 = evalScriptPut(":project:server:test", "{\"content\": \"test\"}");

        // ACT
        List<String> valueTest1 = (List<String>) evalScriptGet(":project:server:test");
        List<String> valueTest2 = (List<String>) evalScriptGet(":project:server:test:test1:test2");

        // ASSERT
        assertThat(resultPutTest2, equalTo("OK"));
        assertThat(resultPutTest1, equalTo("existingCollection"));
        assertThat(valueTest1, hasItem("test1:"));
        assertThat(valueTest2.get(1), equalTo("{\"content\": \"test/test1/test2\"}"));
    }

    @Test
    public void tryToPutResourceOnCollectionPathThreeLevelsBelow() {

        // ARRANGE
        String resultPutTest2 = evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");
        String resultPutTest1 = evalScriptPut(":project:server", "{\"content\": \"test\"}");

        // ACT
        List<String> valueTest1 = (List<String>) evalScriptGet(":project:server");
        List<String> valueTest2 = (List<String>) evalScriptGet(":project:server:test:test1:test2");

        // ASSERT
        assertThat(resultPutTest2, equalTo("OK"));
        assertThat(resultPutTest1, equalTo("existingCollection"));
        assertThat(valueTest1, hasItem("test:"));
        assertThat(valueTest2.get(1), equalTo("{\"content\": \"test/test1/test2\"}"));
    }

    @Test
    public void tryToPutResourceOnCollectionFirstLevel() {

        // ARRANGE
        String resultPutTest2 = evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");
        String resultPutTest1 = evalScriptPut(":project", "{\"content\": \"test\"}");

        // ACT
        List<String> valueTest1 = (List<String>) evalScriptGet(":project");
        List<String> valueTest2 = (List<String>) evalScriptGet(":project:server:test:test1:test2");

        // ASSERT
        assertThat(resultPutTest2, equalTo("OK"));
        assertThat(resultPutTest1, equalTo("existingCollection"));
        assertThat(valueTest1, hasItem("server:"));
        assertThat(valueTest2.get(1), equalTo("{\"content\": \"test/test1/test2\"}"));
    }

    @Test
    public void tryToPutCollectionOnResourceOneLevelAbovePath() {

        // ARRANGE
        String resultPutTest1 = evalScriptPut(":project:server:test:test1", "{\"content\": \"test/test1\"}");
        String resultPutTest2 = evalScriptPut(":project:server:test:test1:test2", "{\"content\": \"test/test1/test2\"}");

        // ACT
        List<String> valueTest1 = (List<String>) evalScriptGet(":project:server:test:test1");
        String valueTest2 = (String) evalScriptGet(":project:server:test:test1:test2");

        // ASSERT
        assertThat(resultPutTest1, equalTo("OK"));
        assertThat(resultPutTest2, equalTo("existingResource rest-storage:resources:project:server:test:test1"));
        assertThat(valueTest1.get(1), equalTo("{\"content\": \"test/test1\"}"));
        assertThat(valueTest2, equalTo("notFound"));
    }

    @Test
    public void tryToPutCollectionOnResourceTwoLevelsAbovePath() {

        // ARRANGE
        String resultPutTest1 = evalScriptPut(":project:server:test:test1", "{\"content\": \"test/test1\"}");
        String resultPutTest3 = evalScriptPut(":project:server:test:test1:test2:test3", "{\"content\": \"test/test1/test2/test3\"}");

        // ACT
        List<String> valueTest1 = (List<String>) evalScriptGet(":project:server:test:test1");
        String valueTest3 = (String) evalScriptGet(":project:server:test:test1:test2:test3");

        // ASSERT
        assertThat(resultPutTest1, equalTo("OK"));
        assertThat(resultPutTest3, equalTo("existingResource rest-storage:resources:project:server:test:test1"));
        assertThat(valueTest1.get(1), equalTo("{\"content\": \"test/test1\"}"));
        assertThat(valueTest3, equalTo("notFound"));
    }

    @Test
    public void tryToPutCollectionOnResourceOneLevelOneLevelAbovePath() {

        // ARRANGE
        String resultPutNemo = evalScriptPut(":project", "{\"content\": \"nemo\"}");
        String resultPutNemoServer = evalScriptPut(":project:server", "{\"content\": \"nemo/server\"}");

        // ACT
        List<String> valueNemo = (List<String>) evalScriptGet(":project");
        String valueServer = (String) evalScriptGet(":project:server");

        // ASSERT
        assertThat(resultPutNemo, equalTo("OK"));
        assertThat(resultPutNemoServer, equalTo("existingResource rest-storage:resources:project"));
        assertThat(valueNemo.get(1), equalTo("{\"content\": \"nemo\"}"));
        assertThat(valueServer, equalTo("notFound"));
    }

    @Test
    public void tryToPutCollectionOnResourceFourLevelOneLevelAbovePath() {

        // ARRANGE
        String resultPutNemo = evalScriptPut(":nemo:server:tests:crush:test1", "{\"content\": \"nemo\"}");
        String resultPutNemoServer = evalScriptPut(":nemo:server:tests:crush:test1:test2:test3", "{\"content\": \"nemo/server\"}");

        // ACT
        List<String> valueNemo = (List<String>) evalScriptGet(":nemo:server:tests:crush:test1");
        String valueServer = (String) evalScriptGet(":nemo:server:tests:crush:test1:test2:test3");

        // ASSERT
        assertThat(resultPutNemo, equalTo("OK"));
        assertThat(resultPutNemoServer, equalTo("existingResource rest-storage:resources:nemo:server:tests:crush:test1"));
        assertThat(valueNemo.get(1), equalTo("{\"content\": \"nemo\"}"));
        assertThat(valueServer, equalTo("notFound"));
    }

    @SuppressWarnings({ "rawtypes", "unchecked", "serial" })
    private Object evalScriptPutMerge(final String resourceName1, final String resourceValue1) {
        String putScript = readScript("put.lua");
        return jedis.eval(putScript, new ArrayList() {
                    {
                        add(resourceName1);
                    }
                }, new ArrayList() {
                    {
                        add(prefixResources);
                        add(prefixCollections);
                        add(expirableSet);
                        add("true");
                        add("99999999999999");
                        add("99999999999999");
                        add(resourceValue1);
                        add(UUID.randomUUID().toString());
                        add(prefixLock);
                    }
                }
        );
    }

    @Test
    public void putResourceWithSilentLockDifferentEtag() {
        // ARRANGE
        String basePath = ":project:server:silent:";
        String lockedPath =  basePath + "myResource";
        String anotherPath = basePath + "anotherPath";

        String lockedResource = "{\"content\" : \"locked\" }";
        String normalResource = "{\"content\" : \"normal\" }";
        String tryToLockResourceOwner2 = "{\"content\" : \"owner2\" }";
        String anotherResource = "{\"content\" : \"anotherResource\" }";

        // ACT
        String lockedValue = evalScriptPut(lockedPath, lockedResource, AbstractLuaScriptTest.MAX_EXPIRE, "etag1", "owner1", LockMode.SILENT, 300);
        String normalValue = evalScriptPut(lockedPath, normalResource, AbstractLuaScriptTest.MAX_EXPIRE, "etag2");
        String tryToLockValue = evalScriptPut(lockedPath, tryToLockResourceOwner2, AbstractLuaScriptTest.MAX_EXPIRE, "etag3", "owner2", LockMode.SILENT, 300);
        String anotherValue = evalScriptPut(anotherPath, anotherResource, AbstractLuaScriptTest.MAX_EXPIRE);

        // ASSERT
        assertThat(lockedValue, is(equalTo("OK")));
        assertThat(normalValue, is(equalTo(LockMode.SILENT.text())));
        assertThat(tryToLockValue, is(equalTo(LockMode.SILENT.text())));
        assertThat(anotherValue, is(equalTo("OK")));

        assertThat(jedis.hget("rest-storage:resources" + lockedPath, RESOURCE), equalTo(lockedResource));
        assertThat(jedis.hget("rest-storage:resources" + anotherPath, RESOURCE), equalTo(anotherResource));

        assertThat(jedis.hget(prefixLock + lockedPath, OWNER), equalTo("owner1"));
        assertThat(jedis.hget(prefixLock + lockedPath, MODE), equalTo(LockMode.SILENT.text()));
    }

    @Test
    public void putResourceWithSilentLockSameEtag() {
        // ARRANGE
        String basePath = ":project:server:silent:";
        String lockedPath =  basePath + "myResource";
        String anotherPath = basePath + "anotherPath";

        String lockedResource = "{\"content\" : \"locked\" }";
        String normalResource = "{\"content\" : \"normal\" }";
        String tryToLockResourceOwner2 = "{\"content\" : \"owner2\" }";
        String anotherResource = "{\"content\" : \"anotherResource\" }";

        // ACT
        String lockedValue = evalScriptPut(lockedPath, lockedResource, AbstractLuaScriptTest.MAX_EXPIRE, "etag", "owner1", LockMode.SILENT, 300);
        String normalValue = evalScriptPut(lockedPath, normalResource, AbstractLuaScriptTest.MAX_EXPIRE, "etag");
        String tryToLockValue = evalScriptPut(lockedPath, tryToLockResourceOwner2, AbstractLuaScriptTest.MAX_EXPIRE, "etag", "owner2", LockMode.SILENT, 300);
        String anotherValue = evalScriptPut(anotherPath, anotherResource, AbstractLuaScriptTest.MAX_EXPIRE, "etag");

        // ASSERT
        assertThat(lockedValue, is(equalTo("OK")));
        assertThat(normalValue, is(equalTo(LockMode.SILENT.text())));
        assertThat(tryToLockValue, is(equalTo(LockMode.SILENT.text())));
        assertThat(anotherValue, is(equalTo("OK")));

        assertThat(jedis.hget("rest-storage:resources" + lockedPath, RESOURCE), equalTo(lockedResource));
        assertThat(jedis.hget("rest-storage:resources" + anotherPath, RESOURCE), equalTo(anotherResource));

        assertThat(jedis.hget(prefixLock + lockedPath, OWNER), equalTo("owner1"));
        assertThat(jedis.hget(prefixLock + lockedPath, MODE), equalTo(LockMode.SILENT.text()));
    }

    @Test
    public void putResourceWithRejectLockDifferentEtag() {
        // ARRANGE
        String basePath = ":project:server:reject:";
        String lockedPath =  basePath + "myResource";
        String anotherPath = basePath + "anotherPath";

        String lockedResource = "{\"content\" : \"locked\" }";
        String normalResource = "{\"content\" : \"normal\" }";
        String tryToLockResourceOwner2 = "{\"content\" : \"owner2\" }";
        String anotherResource = "{\"content\" : \"anotherResource\" }";

        // ACT
        String lockedValue = evalScriptPut(lockedPath, lockedResource, AbstractLuaScriptTest.MAX_EXPIRE, "etag1", "owner1", LockMode.REJECT, 300);
        String normalValue = evalScriptPut(lockedPath, normalResource, AbstractLuaScriptTest.MAX_EXPIRE, "etag2");
        String tryToLockValue = evalScriptPut(lockedPath, tryToLockResourceOwner2, AbstractLuaScriptTest.MAX_EXPIRE, "etag3", "owner2", LockMode.REJECT, 300);
        String anotherValue = evalScriptPut(anotherPath, anotherResource, AbstractLuaScriptTest.MAX_EXPIRE);

        // ASSERT
        assertThat(lockedValue, is(equalTo("OK")));
        assertThat(normalValue, is(equalTo(LockMode.REJECT.text())));
        assertThat(tryToLockValue, is(equalTo(LockMode.REJECT.text())));
        assertThat(anotherValue, is(equalTo("OK")));

        assertThat(jedis.hget("rest-storage:resources" + lockedPath, RESOURCE), equalTo(lockedResource));
        assertThat(jedis.hget("rest-storage:resources" + anotherPath, RESOURCE), equalTo(anotherResource));

        assertThat(jedis.hget(prefixLock + lockedPath, OWNER), equalTo("owner1"));
        assertThat(jedis.hget(prefixLock + lockedPath, MODE), equalTo(LockMode.REJECT.text()));
    }

    @Test
    public void putResourceWithRejectLockSameEtag() {
        // ARRANGE
        String basePath = ":project:server:reject:";
        String lockedPath =  basePath + "myResource";
        String anotherPath = basePath + "anotherPath";

        String lockedResource = "{\"content\" : \"locked\" }";
        String normalResource = "{\"content\" : \"normal\" }";
        String tryToLockResourceOwner2 = "{\"content\" : \"owner2\" }";
        String anotherResource = "{\"content\" : \"anotherResource\" }";

        // ACT
        String lockedValue = evalScriptPut(lockedPath, lockedResource, AbstractLuaScriptTest.MAX_EXPIRE, "etag", "owner1", LockMode.REJECT, 300);
        String normalValue = evalScriptPut(lockedPath, normalResource, AbstractLuaScriptTest.MAX_EXPIRE, "etag");
        String tryToLockValue = evalScriptPut(lockedPath, tryToLockResourceOwner2, AbstractLuaScriptTest.MAX_EXPIRE, "etag", "owner2", LockMode.REJECT, 300);
        String anotherValue = evalScriptPut(anotherPath, anotherResource, AbstractLuaScriptTest.MAX_EXPIRE);

        // ASSERT
        assertThat(lockedValue, is(equalTo("OK")));
        assertThat(normalValue, is(equalTo(LockMode.REJECT.text())));
        assertThat(tryToLockValue, is(equalTo(LockMode.REJECT.text())));
        assertThat(anotherValue, is(equalTo("OK")));

        assertThat(jedis.hget("rest-storage:resources" + lockedPath, RESOURCE), equalTo(lockedResource));
        assertThat(jedis.hget("rest-storage:resources" + anotherPath, RESOURCE), equalTo(anotherResource));

        assertThat(jedis.hget(prefixLock + lockedPath, OWNER), equalTo("owner1"));
        assertThat(jedis.hget(prefixLock + lockedPath, MODE), equalTo(LockMode.REJECT.text()));
    }

    @Test
    public void tryToPutLockedResourceSameOwner() {
        // ARRANGE
        String basePath = ":project:server:silent:";
        String lockedPath =  basePath + "myResource";

        String lockedResource = "{\"content\" : \"locked\" }";
        String overwrittenResource = "{\"content\" : \"overwritten by owner of lock\" }";
        String normalResource = "{\"content\" : \"normal\" }";

        // ACT 1
        String lockedValue = evalScriptPut(lockedPath, lockedResource, AbstractLuaScriptTest.MAX_EXPIRE, "etag", "owner1", LockMode.SILENT, 300);

        // ASSERT 1
        assertThat(lockedValue, is(equalTo("OK")));
        assertThat(jedis.hget("rest-storage:resources" + lockedPath, RESOURCE), equalTo(lockedResource));

        // ACT 2
        String overwrittenValue = evalScriptPut(lockedPath, overwrittenResource, AbstractLuaScriptTest.MAX_EXPIRE, "etag1", "owner1", LockMode.SILENT, 300);

        // ASSERT 2
        assertThat(overwrittenValue, is(equalTo("OK")));
        assertThat(jedis.hget("rest-storage:resources" + lockedPath, RESOURCE), equalTo(overwrittenResource));

        // ACT 3
        String normalValue = evalScriptPut(lockedPath, normalResource, AbstractLuaScriptTest.MAX_EXPIRE);

        // ASSERT 3
        assertThat(normalValue, is(equalTo(LockMode.SILENT.text())));
        assertThat(jedis.hget("rest-storage:resources" + lockedPath, RESOURCE), equalTo(overwrittenResource));
    }

    @Test
    public void tryToPutWhileAndAfterResourceLock() {
        // ARRANGE
        long lockExpire = 5;
        String basePath = ":project:server:silent:expire:";
        String lockedPath =  basePath + "myResource";

        String lockedResource = "{\"content\" : \"locked\" }";
        String newResource = "{\"content\" : \"new\" }";

        // ACT 1
        jedis.expire(prefixLock +  lockedPath, 0);
        String lockedValue = evalScriptPut(lockedPath, lockedResource, AbstractLuaScriptTest.MAX_EXPIRE, "etag", "owner1", LockMode.SILENT, lockExpire);
        String newValue = evalScriptPut(lockedPath, newResource, AbstractLuaScriptTest.MAX_EXPIRE);

        // ASSERT 1
        assertThat(lockedValue, is(equalTo("OK")));
        assertThat(newValue, is(equalTo(LockMode.SILENT.text())));

        assertThat(jedis.hget("rest-storage:resources" + lockedPath, RESOURCE), equalTo(lockedResource));

        // ACT 2 - wait
        await().atMost(lockExpire * 2, SECONDS).until(() ->
                evalScriptPut(lockedPath, newResource, AbstractLuaScriptTest.MAX_EXPIRE),
                equalTo("OK")
        );

        // ASSERT 2
        assertThat(jedis.hget("rest-storage:resources" + lockedPath, RESOURCE), equalTo(newResource));
        assertThat(jedis.exists(prefixLock + lockedPath), equalTo(false));
    }
}
