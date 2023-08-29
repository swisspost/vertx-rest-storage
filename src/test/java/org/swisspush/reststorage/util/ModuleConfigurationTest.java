package org.swisspush.reststorage.util;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;

import static org.swisspush.reststorage.util.ModuleConfiguration.StorageType;
import static org.swisspush.reststorage.util.ModuleConfiguration.fromJsonObject;

/**
 * Tests for {@link ModuleConfiguration} class.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
@RunWith(VertxUnitRunner.class)
public class ModuleConfigurationTest {

    @Test
    public void testDefaultConfiguration(TestContext testContext) {
        ModuleConfiguration config = new ModuleConfiguration();

        // go through JSON encode/decode
        String json = config.asJsonObject().encodePrettily();
        config = ModuleConfiguration.fromJsonObject(new JsonObject(json));

        testContext.assertEquals(config.getRoot(), ".");
        testContext.assertEquals(config.getStorageType(), StorageType.filesystem);
        testContext.assertEquals(config.getPort(), 8989);
        testContext.assertTrue(config.isHttpRequestHandlerEnabled());
        testContext.assertFalse(config.isHttpRequestHandlerAuthenticationEnabled());
        testContext.assertNull(config.getHttpRequestHandlerUsername());
        testContext.assertNull(config.getHttpRequestHandlerPassword());
        testContext.assertEquals(config.getPrefix(), "");
        testContext.assertEquals(config.getStorageAddress(), "resource-storage");
        testContext.assertNull(config.getEditorConfig());
        testContext.assertEquals(config.getRedisHost(), "localhost");
        testContext.assertEquals(config.getRedisPort(), 6379);
        testContext.assertFalse(config.isRedisEnableTls());
        testContext.assertEquals(config.getExpirablePrefix(), "rest-storage:expirable");
        testContext.assertEquals(config.getResourcesPrefix(), "rest-storage:resources");
        testContext.assertEquals(config.getCollectionsPrefix(), "rest-storage:collections");
        testContext.assertEquals(config.getDeltaResourcesPrefix(), "delta:resources");
        testContext.assertEquals(config.getDeltaEtagsPrefix(), "delta:etags");
        testContext.assertEquals(config.getResourceCleanupAmount(), 100000L);
        testContext.assertEquals(config.getLockPrefix(), "rest-storage:locks");
        testContext.assertFalse(config.isConfirmCollectionDelete());
        testContext.assertFalse(config.isRejectStorageWriteOnLowMemory());
        testContext.assertEquals(config.getFreeMemoryCheckIntervalMs(), 60000L);
        testContext.assertFalse(config.isReturn200onDeleteNonExisting());
        testContext.assertEquals(config.getMaxRedisWaitingHandlers(), 2048);
    }

    @Test
    public void testOverrideConfiguration(TestContext testContext) {
        ModuleConfiguration config = new ModuleConfiguration()
                .redisHost("anotherhost")
                .redisPort(1234)
                .redisEnableTls(true)
                .editorConfig(new HashMap<>() {{
                    put("myKey", "myValue");
                }})
                .confirmCollectionDelete(true)
                .httpRequestHandlerEnabled(false)
                .httpRequestHandlerAuthenticationEnabled(true)
                .httpRequestHandlerUsername("foo")
                .httpRequestHandlerPassword("bar")
                .rejectStorageWriteOnLowMemory(true)
                .freeMemoryCheckIntervalMs(10000)
                .maxRedisWaitingHandlers(4096)
                .return200onDeleteNonExisting(true);

        // go through JSON encode/decode
        String json = config.asJsonObject().encodePrettily();
        config = ModuleConfiguration.fromJsonObject(new JsonObject(json));

        // default values
        testContext.assertEquals(config.getRoot(), ".");
        testContext.assertEquals(config.getStorageType(), StorageType.filesystem);
        testContext.assertEquals(config.getPort(), 8989);
        testContext.assertEquals(config.getPrefix(), "");
        testContext.assertEquals(config.getStorageAddress(), "resource-storage");
        testContext.assertEquals(config.getExpirablePrefix(), "rest-storage:expirable");
        testContext.assertEquals(config.getResourcesPrefix(), "rest-storage:resources");
        testContext.assertEquals(config.getCollectionsPrefix(), "rest-storage:collections");
        testContext.assertEquals(config.getDeltaResourcesPrefix(), "delta:resources");
        testContext.assertEquals(config.getDeltaEtagsPrefix(), "delta:etags");
        testContext.assertEquals(config.getResourceCleanupAmount(), 100000L);
        testContext.assertEquals(config.getLockPrefix(), "rest-storage:locks");

        // overridden values
        testContext.assertEquals(config.getRedisHost(), "anotherhost");
        testContext.assertEquals(config.getRedisPort(), 1234);
        testContext.assertTrue(config.isRedisEnableTls());
        testContext.assertFalse(config.isHttpRequestHandlerEnabled());
        testContext.assertNotNull(config.getEditorConfig());
        testContext.assertTrue(config.getEditorConfig().containsKey("myKey"));
        testContext.assertEquals(config.getEditorConfig().get("myKey"), "myValue");
        testContext.assertTrue(config.isConfirmCollectionDelete());
        testContext.assertTrue(config.isRejectStorageWriteOnLowMemory());
        testContext.assertEquals(config.getFreeMemoryCheckIntervalMs(), 10000L);
        testContext.assertTrue(config.isReturn200onDeleteNonExisting());
        testContext.assertEquals(config.getMaxRedisWaitingHandlers(), 4096);
        testContext.assertTrue(config.isHttpRequestHandlerAuthenticationEnabled());
        testContext.assertEquals(config.getHttpRequestHandlerUsername(), "foo");
        testContext.assertEquals(config.getHttpRequestHandlerPassword(), "bar");
    }

    @Test
    public void testGetDefaultAsJsonObject(TestContext testContext){
        ModuleConfiguration config = new ModuleConfiguration();
        JsonObject json = config.asJsonObject();

        testContext.assertEquals(json.getString("root"), ".");
        testContext.assertEquals(json.getString("storageType"), StorageType.filesystem.name());
        testContext.assertEquals(json.getInteger("port"), 8989);
        testContext.assertFalse(json.getBoolean("httpRequestHandlerAuthenticationEnabled"));
        testContext.assertTrue(json.getBoolean("httpRequestHandlerEnabled"));
        testContext.assertFalse(json.getBoolean("redisEnableTls"));
        testContext.assertNull(json.getJsonObject("httpRequestHandlerUsername"));
        testContext.assertNull(json.getJsonObject("httpRequestHandlerPassword"));
        testContext.assertEquals(json.getString("prefix"), "");
        testContext.assertEquals(json.getString("storageAddress"), "resource-storage");
        testContext.assertNull(json.getJsonObject("editorConfig"));
        testContext.assertEquals(json.getString("redisHost"), "localhost");
        testContext.assertEquals(json.getInteger("redisPort"), 6379);
        testContext.assertEquals(json.getInteger("maxRedisWaitingHandlers"), 2048);
        testContext.assertNull(json.getString("redisAuth"));
        testContext.assertNull(json.getString("redisPassword"));
        testContext.assertNull(json.getString("redisUser"));
        testContext.assertEquals(json.getString("expirablePrefix"), "rest-storage:expirable");
        testContext.assertEquals(json.getString("resourcesPrefix"), "rest-storage:resources");
        testContext.assertEquals(json.getString("collectionsPrefix"), "rest-storage:collections");
        testContext.assertEquals(json.getString("deltaResourcesPrefix"), "delta:resources");
        testContext.assertEquals(json.getString("deltaEtagsPrefix"), "delta:etags");
        testContext.assertEquals(json.getLong("resourceCleanupAmount"), 100000L);
        testContext.assertEquals(json.getString("lockPrefix"), "rest-storage:locks");
        testContext.assertFalse(json.getBoolean("confirmCollectionDelete"));
        testContext.assertFalse(json.getBoolean("rejectStorageWriteOnLowMemory"));
        testContext.assertEquals(json.getLong("freeMemoryCheckIntervalMs"), 60000L);
    }

    @Test
    public void testGetOverriddenAsJsonObject(TestContext testContext){

        ModuleConfiguration config = new ModuleConfiguration()
                .redisHost("anotherhost")
                .redisPort(1234)
                .redisEnableTls(true)
                .editorConfig(new HashMap<>() {{
                    put("myKey", "myValue");
                }})
                .maxRedisWaitingHandlers(4096)
                .httpRequestHandlerEnabled(false)
                .httpRequestHandlerAuthenticationEnabled(true)
                .httpRequestHandlerUsername("foo")
                .httpRequestHandlerPassword("bar")
                .confirmCollectionDelete(true)
                .rejectStorageWriteOnLowMemory(true)
                .freeMemoryCheckIntervalMs(5000);

        JsonObject json = config.asJsonObject();

        // default values
        testContext.assertEquals(json.getString("root"), ".");
        testContext.assertEquals(json.getString("storageType"), StorageType.filesystem.name());
        testContext.assertEquals(json.getInteger("port"), 8989);
        testContext.assertFalse(json.getBoolean("httpRequestHandlerEnabled"));
        testContext.assertEquals(json.getString("prefix"), "");
        testContext.assertEquals(json.getString("storageAddress"), "resource-storage");
        testContext.assertEquals(json.getString("expirablePrefix"), "rest-storage:expirable");
        testContext.assertEquals(json.getString("resourcesPrefix"), "rest-storage:resources");
        testContext.assertEquals(json.getString("collectionsPrefix"), "rest-storage:collections");
        testContext.assertEquals(json.getString("deltaResourcesPrefix"), "delta:resources");
        testContext.assertEquals(json.getString("deltaEtagsPrefix"), "delta:etags");
        testContext.assertEquals(json.getLong("resourceCleanupAmount"), 100000L);
        testContext.assertEquals(json.getString("lockPrefix"), "rest-storage:locks");


        // overridden values
        testContext.assertEquals(json.getString("redisHost"), "anotherhost");
        testContext.assertEquals(json.getInteger("redisPort"), 1234);
        testContext.assertTrue(json.getBoolean("redisEnableTls"));
        testContext.assertTrue(json.getBoolean("confirmCollectionDelete"));
        testContext.assertTrue(json.getBoolean("rejectStorageWriteOnLowMemory"));
        testContext.assertEquals(config.getFreeMemoryCheckIntervalMs(), 5000L);
        testContext.assertEquals(json.getInteger("maxRedisWaitingHandlers"), 4096);

        testContext.assertNotNull(json.getJsonObject("editorConfig"));
        testContext.assertTrue(json.getJsonObject("editorConfig").containsKey("myKey"));
        testContext.assertEquals(json.getJsonObject("editorConfig").getString("myKey"), "myValue");

        testContext.assertTrue(json.getBoolean("httpRequestHandlerAuthenticationEnabled"));
        testContext.assertEquals(json.getString("httpRequestHandlerUsername"), "foo");
        testContext.assertEquals(json.getString("httpRequestHandlerPassword"), "bar");
    }

    @Test
    public void testGetDefaultFromJsonObject(TestContext testContext){
        JsonObject json  = new ModuleConfiguration().asJsonObject();
        ModuleConfiguration config = fromJsonObject(json);

        testContext.assertEquals(config.getRoot(), ".");
        testContext.assertEquals(config.getStorageType(), StorageType.filesystem);
        testContext.assertEquals(config.getPort(), 8989);
        testContext.assertEquals(config.getPrefix(), "");
        testContext.assertEquals(config.getStorageAddress(), "resource-storage");
        testContext.assertNull(config.getEditorConfig());
        testContext.assertEquals(config.getRedisHost(), "localhost");
        testContext.assertEquals(config.getRedisPort(), 6379);
        testContext.assertFalse(json.getBoolean("redisEnableTls"));
        testContext.assertEquals(config.getMaxRedisWaitingHandlers(), 2048);
        testContext.assertEquals(config.getExpirablePrefix(), "rest-storage:expirable");
        testContext.assertEquals(config.getResourcesPrefix(), "rest-storage:resources");
        testContext.assertEquals(config.getCollectionsPrefix(), "rest-storage:collections");
        testContext.assertEquals(config.getDeltaResourcesPrefix(), "delta:resources");
        testContext.assertEquals(config.getDeltaEtagsPrefix(), "delta:etags");
        testContext.assertEquals(config.getResourceCleanupAmount(), 100000L);
        testContext.assertEquals(config.getLockPrefix(), "rest-storage:locks");
        testContext.assertFalse(config.isConfirmCollectionDelete());
        testContext.assertFalse(config.isRejectStorageWriteOnLowMemory());
        testContext.assertEquals(config.getFreeMemoryCheckIntervalMs(), 60000L);
        testContext.assertTrue(config.isHttpRequestHandlerEnabled());
        testContext.assertFalse(config.isHttpRequestHandlerAuthenticationEnabled());
        testContext.assertNull(config.getHttpRequestHandlerUsername());
        testContext.assertNull(config.getHttpRequestHandlerPassword());
    }

    @Test
    public void testGetOverriddenFromJsonObject(TestContext testContext){

        JsonObject json = new JsonObject();
        json.put("root", "newroot");
        json.put("storageType", "redis");
        json.put("port", 1234);
        json.put("redisEnableTls", true);
        json.put("httpRequestHandlerEnabled", false);
        json.put("httpRequestHandlerAuthenticationEnabled", true);
        json.put("httpRequestHandlerUsername", "foo");
        json.put("httpRequestHandlerPassword", "bar");
        json.put("prefix", "newprefix");
        json.put("storageAddress", "newStorageAddress");
        json.put("editorConfig", new JsonObject().put("myKey", "myValue"));
        json.put("redisHost", "newredishost");
        json.put("redisPort", 4321);
        json.put("maxRedisWaitingHandlers", 4096);
        json.put("expirablePrefix", "newExpirablePrefix");
        json.put("resourcesPrefix", "newResourcesPrefix");
        json.put("collectionsPrefix", "newCollectionsPrefix");
        json.put("deltaResourcesPrefix", "newDeltaResourcesPrefix");
        json.put("deltaEtagsPrefix", "newDeltaEtagsPrefix");
        json.put("resourceCleanupAmount", 999L);
        json.put("lockPrefix", "newLockPrefix");
        json.put("confirmCollectionDelete", true);
        json.put("rejectStorageWriteOnLowMemory", true);
        json.put("freeMemoryCheckIntervalMs", 30000);

        ModuleConfiguration config = fromJsonObject(json);
        testContext.assertEquals(config.getRoot(), "newroot");
        testContext.assertEquals(config.getStorageType(), StorageType.redis);
        testContext.assertEquals(config.getPort(), 1234);
        testContext.assertTrue(config.isRedisEnableTls());
        testContext.assertFalse(config.isHttpRequestHandlerEnabled());
        testContext.assertTrue(config.isHttpRequestHandlerAuthenticationEnabled());
        testContext.assertEquals(config.getHttpRequestHandlerUsername(), "foo");
        testContext.assertEquals(config.getHttpRequestHandlerPassword(), "bar");
        testContext.assertEquals(config.getPrefix(), "newprefix");
        testContext.assertEquals(config.getStorageAddress(), "newStorageAddress");

        testContext.assertNotNull(config.getEditorConfig());
        testContext.assertTrue(config.getEditorConfig().containsKey("myKey"));
        testContext.assertEquals(config.getEditorConfig().get("myKey"), "myValue");

        testContext.assertEquals(config.getRedisHost(), "newredishost");
        testContext.assertEquals(config.getRedisPort(), 4321);
        testContext.assertEquals(config.getMaxRedisWaitingHandlers(), 4096);
        testContext.assertEquals(config.getExpirablePrefix(), "newExpirablePrefix");
        testContext.assertEquals(config.getResourcesPrefix(), "newResourcesPrefix");
        testContext.assertEquals(config.getCollectionsPrefix(), "newCollectionsPrefix");
        testContext.assertEquals(config.getDeltaResourcesPrefix(), "newDeltaResourcesPrefix");
        testContext.assertEquals(config.getDeltaEtagsPrefix(), "newDeltaEtagsPrefix");
        testContext.assertEquals(config.getResourceCleanupAmount(), 999L);
        testContext.assertEquals(config.getLockPrefix(), "newLockPrefix");
        testContext.assertTrue(config.isConfirmCollectionDelete());
        testContext.assertTrue(config.isRejectStorageWriteOnLowMemory());
        testContext.assertEquals(config.getFreeMemoryCheckIntervalMs(), 30000L);
    }
}