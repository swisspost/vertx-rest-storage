package org.swisspush.reststorage;

import org.junit.Test;

import static io.restassured.RestAssured.when;

public class Return404onDeleteNonExistingTest extends RedisStorageIntegrationTestCase {

    @Test
    public void expect404() {
        // default config response with 404 if we delete a non-existing resource
        when().delete("resources/does-not-exist").then().assertThat().statusCode(404);
    }
}
