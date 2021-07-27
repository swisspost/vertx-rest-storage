package org.swisspush.reststorage;

import io.restassured.RestAssured;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static io.restassured.RestAssured.*;

@RunWith(VertxUnitRunner.class)
public class PathLevelIntegrationTest extends RedisStorageIntegrationTestCase {

    @Test
    public void testTryToPutResourceOverCollection(TestContext context) {
        Async async = context.async();
        RestAssured.basePath = "";
        with().put("/tests/crush/test1/test2/test3");
        // here we assume, that on the path server is already a collection
        given().
                body("{ \"foo\": \"bar\" }").
                when().
                put("/tests").
                then().
                assertThat().statusCode(405);
        async.complete();
    }

    @Test
    public void testPut4levels(TestContext context) {
        Async async = context.async();

        given().
                body("{ \"foo\": \"bar\" }").
                when().
                put("/tests/crush/test1/test2/test3/test4").
                then().
                assertThat().statusCode(200);

        // test level 1 with and without trailing slash
        context.assertEquals("[test2/]", get("/tests/crush/test1").body().jsonPath().get("test1").toString());
        context.assertEquals("[test2/]", get("/tests/crush/test1/").body().jsonPath().get("test1").toString());

        // test level 2 with and without trailing slash
        context.assertEquals("[test3/]", get("/tests/crush/test1/test2").body().jsonPath().get("test2").toString());
        context.assertEquals("[test3/]", get("/tests/crush/test1/test2/").body().jsonPath().get("test2").toString());

        // test level 3 with and without trailing slash
        context.assertEquals("[test4]", get("/tests/crush/test1/test2/test3").body().jsonPath().get("test3").toString());
        context.assertEquals("[test4]", get("/tests/crush/test1/test2/test3/").body().jsonPath().get("test3").toString());

        // test4 level
        context.assertEquals("{ \"foo\": \"bar\" }", get("/tests/crush/test1/test2/test3/test4").body().asString());

        async.complete();
    }

    @Test
    public void testPutResourceOverCollection(TestContext context) {
        Async async = context.async();

        given().
                body("{ \"foo\": \"bar\" }").
                when().
                put("/tests/crush/test1/test2/test3/test4").
                then().
                assertThat().statusCode(200);

        given().
                body("{ \"foo\": \"bar\" }").
                when().
                put("/tests/crush/test1/test2").
                then().
                assertThat().statusCode(405);

        async.complete();
    }

    @Test
    public void testPutCollectionOverResource(TestContext context) {
        Async async = context.async();

        given().
                body("{ \"foo\": \"bar\" }").
                when().
                put("/tests/crush/test1/test2").
                then().
                assertThat().statusCode(200);

        given().
                body("{ \"foo\": \"bar\" }").
                when().
                put("/tests/crush/test1/test2/test3/test4").
                then().
                assertThat().statusCode(405);

        async.complete();
    }
}
