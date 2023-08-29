package org.swisspush.reststorage;

import org.junit.Test;
import org.swisspush.reststorage.redis.RedisStorageIntegrationTestCase;
import org.swisspush.reststorage.util.ModuleConfiguration;

import static io.restassured.RestAssured.when;

public class Return200onDeleteNonExistingTest extends RedisStorageIntegrationTestCase {

    @Override
    protected void updateModuleConfiguration(ModuleConfiguration modConfig) {
        modConfig.return200onDeleteNonExisting(true);
    }

    @Test
    public void expect200() {
        when().delete("resources/does-not-exist").then().assertThat().statusCode(200);
    }
}
