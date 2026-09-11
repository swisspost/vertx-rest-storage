package org.swisspush.reststorage.redis;

import io.restassured.RestAssured;
import io.restassured.parsing.Parser;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.swisspush.reststorage.ConfigurableTestCase;
import org.swisspush.reststorage.JedisFactory;
import org.swisspush.reststorage.RestStorageMod;
import org.swisspush.reststorage.util.ModuleConfiguration;
import org.swisspush.reststorage.util.ResourceNameUtil;
import redis.clients.jedis.Jedis;

import java.util.Set;

@RunWith(VertxUnitRunner.class)
public abstract class RedisStorageIntegrationTestCase extends ConfigurableTestCase {

    Jedis jedis = null;

    /**
     * Allows the whole integration test suite to be run twice - once with Redis Cluster
     * path-based partitioning disabled (default) and once with it enabled - via
     * {@code -Dreststorage.test.redisClusterPartitioningEnabled=true}. All key-name assumptions in
     * these tests must therefore go through {@link #resourceKey(String)} / {@link #assertExpirableSetCount}
     * instead of hardcoding raw Redis key names, since the exact key name depends on this flag.
     */
    protected static final boolean PARTITIONING_ENABLED =
            Boolean.parseBoolean(System.getProperty("reststorage.test.redisClusterPartitioningEnabled", "false"));

    @Before
    public void setUp(TestContext context) {
        vertx = Vertx.vertx();
        jedis = JedisFactory.createJedis();

        // RestAssured Configuration
        RestAssured.port = REST_STORAGE_PORT;
        RestAssured.requestSpecification = REQUEST_SPECIFICATION;
        RestAssured.registerParser("application/json; charset=utf-8", Parser.JSON);
        RestAssured.defaultParser = Parser.JSON;

        ModuleConfiguration modConfig = new ModuleConfiguration()
                .storageType(ModuleConfiguration.StorageType.redis)
                .confirmCollectionDelete(true)
                .maxStorageExpandSubresources(5)
                .redisClusterPartitioningEnabled(PARTITIONING_ENABLED)
                .storageAddress("rest-storage");

        updateModuleConfiguration(modConfig);

        RestStorageMod restStorageMod = new RestStorageMod();
        vertx.deployVerticle(restStorageMod, new DeploymentOptions().setConfig(modConfig.asJsonObject()), context.asyncAssertSuccess(stringAsyncResult1 -> {
            // standard code: will called @Before every test
            RestAssured.basePath = "";
        }));
    }

    /**
     * chance for specific unit test classes to change config here
     */
    protected void updateModuleConfiguration(ModuleConfiguration modConfig) {
    }

    @After
    public void tearDown(TestContext context) {
        jedis.flushAll();
        jedis.close();
        vertx.close(context.asyncAssertSuccess());
    }

    protected void assertExpirableSetCount(TestContext testContext, Long count){
        // With partitioning enabled, the (single, global) "rest-storage:expirable" set is replaced by
        // one "rest-storage:expirable:{tag}" set per partition, so sum the count across every matching key.
        long total = 0L;
        Set<String> keys = jedis.keys("rest-storage:expirable*");
        for (String key : keys) {
            total += jedis.zcount(key, 0d, Double.MAX_VALUE);
        }
        testContext.assertEquals(count, total);
    }

    /**
     * Builds the exact Redis key ({@code rest-storage:resources<encodedPath>}, tagged when
     * partitioning is enabled) that {@code RedisStorage} uses to store the resource at {@code path}.
     * Use this instead of hardcoding raw Redis key names in tests, since the key layout depends on
     * {@link #PARTITIONING_ENABLED}.
     */
    protected String resourceKey(String path) {
        String encodedPath = ResourceNameUtil.replaceColonsAndSemiColons(path).replace("/", ":");
        if (!encodedPath.startsWith(":")) {
            encodedPath = ":" + encodedPath;
        }
        PartitionContext ctx = PartitionContext.forPath(encodedPath, PARTITIONING_ENABLED, "rest-storage:expirable");
        return "rest-storage:resources" + ctx.getKey();
    }
}