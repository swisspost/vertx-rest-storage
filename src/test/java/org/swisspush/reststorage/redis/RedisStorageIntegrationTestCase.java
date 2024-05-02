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
import redis.clients.jedis.Jedis;

@RunWith(VertxUnitRunner.class)
public abstract class RedisStorageIntegrationTestCase extends ConfigurableTestCase {

    Jedis jedis = null;

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
        testContext.assertEquals(count, jedis.zcount("rest-storage:expirable", 0d, Double.MAX_VALUE));
    }
}