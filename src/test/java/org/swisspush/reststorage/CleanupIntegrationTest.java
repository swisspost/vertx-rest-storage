package org.swisspush.reststorage;

import io.restassured.http.ContentType;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.swisspush.reststorage.redis.RedisStorageIntegrationTestCase;

import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.await;
import static io.restassured.RestAssured.get;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.core.IsEqual.equalTo;

@RunWith(VertxUnitRunner.class)
public class CleanupIntegrationTest extends RedisStorageIntegrationTestCase {

    @Test
    public void testNothingToCleanup(TestContext testContext) {
        Async async = testContext.async();
        validateCleanupResults(0,0);
        async.complete();
    }

    @Test
    public void testCleanupAmountBelowBulkSize(TestContext testContext) {
        Async async = testContext.async();
        generateResourcesAndWaitUntilExpired(100);
        validateCleanupResults(100,0);
        async.complete();
    }

    @Test
    public void testCleanupAmountHigherThanBulkSize(TestContext testContext) {
        Async async = testContext.async();
        generateResourcesAndWaitUntilExpired(300);
        validateCleanupResults(300,0);
        async.complete();
    }

    private void generateResourcesAndWaitUntilExpired(int amountOfResources){
        for (int i = 1; i <= amountOfResources; i++) {
            given().
                    header("x-expire-after", "1").
                    body("{ \"foo\": \"bar1\" }").
                    when().
                    put("resource_"+i).
                    then().
                    assertThat().statusCode(200);
        }
        await().atMost(3, TimeUnit.SECONDS).until(() -> get("resource_"+ amountOfResources).statusCode(), equalTo(404));
    }

    private void validateCleanupResults(int cleanedResources, int expiredResourcesLeft){
        given()
                .post("/server/_cleanup")
                .then()
                .assertThat()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("", allOf(hasKey("cleanedResources"), hasKey("expiredResourcesLeft")))
                .body("cleanedResources", equalTo(cleanedResources))
                .body("expiredResourcesLeft", equalTo(expiredResourcesLeft));
    }
}
