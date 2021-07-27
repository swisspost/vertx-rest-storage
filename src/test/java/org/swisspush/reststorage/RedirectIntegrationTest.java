package org.swisspush.reststorage;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static io.restassured.RestAssured.get;
import static io.restassured.RestAssured.with;

@RunWith(VertxUnitRunner.class)
public class RedirectIntegrationTest extends RedisStorageIntegrationTestCase {

    @Test
    public void testGetHTMLResourceWithoutTrailingSlash(TestContext testContext) {
        Async async = testContext.async();
        RestAssured.basePath = "/pages";
        with().body("<h1>nemo.html</h1>").put("nemo.html");
        get("nemo.html").then().assertThat().
                statusCode(200).
                assertThat().
                contentType(ContentType.HTML);
        async.complete();
    }

    @Test
    public void testGetHTMLResourceWithTrailingSlash(TestContext testContext) {
        Async async = testContext.async();
        RestAssured.basePath = "/pages";
        with().body("<h1>nemo.html</h1>").put("nemo.html");
        get("nemo.html/").then().assertThat().
                statusCode(200).
                assertThat().
                contentType(ContentType.HTML);
        async.complete();
    }
}
