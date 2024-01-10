package org.swisspush.reststorage;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.swisspush.reststorage.redis.RedisStorageIntegrationTestCase;

import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.*;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.TWO_SECONDS;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;

@RunWith(VertxUnitRunner.class)
public class ExpirationIntegrationTest extends RedisStorageIntegrationTestCase {

    @Test
    public void testPutInvalidExpireFloat(TestContext context) {
        Async async = context.async();
        given().
                header("x-expire-after", "1.22").
                body("{ \"foo\": \"bar1\" }").
                when().
                put("expireisfloat").
                then().
                assertThat().statusCode(400);

        when().get("expireaftertwoseconds").then().assertThat().statusCode(404);
        async.complete();
    }

    @Test
    public void testPutInvalidExpireNaN(TestContext context) {
        Async async = context.async();
        given().
                header("x-expire-after", "asdfasdf").
                body("{ \"foo\": \"bar1\" }").
                when().
                put("invalidExpireNan").
                then().
                assertThat().statusCode(400);

        when().get("invalidExpireNan").then().assertThat().statusCode(404);
        async.complete();
    }

    @Test
    public void testPutExpireAfterOneSecond(TestContext context) {
        Async async = context.async();
        given().
                header("x-expire-after", "1").
                body("{ \"foo\": \"bar1\" }").
                when().
                put("expireaftertwoseconds").
                then().
                assertThat().statusCode(200);

        assertExpirableSetCount(context, 1L);

        await().atMost(TWO_SECONDS).until(() -> get("expireaftertwoseconds").statusCode(), equalTo(404));
        async.complete();
    }

    @Test
    public void testPutExpiresNever(TestContext context) {
        Async async = context.async();
        given().
                header("x-expire-after", "-1").
                body("{ \"foo\": \"bar1\" }").
                when().
                put("expiresNever").
                then().
                assertThat().statusCode(200);

        assertExpirableSetCount(context, 0L);

        await().timeout(TWO_SECONDS).until(() -> get("expiresNever").statusCode(), equalTo(200));
        async.complete();
    }

    @Test
    public void testPutExpiresNeverNoHeader(TestContext context) {
        Async async = context.async();
        given().
                body("{ \"foo\": \"bar1\" }").
                when().
                put("expiresNeverNoHeader").
                then().
                assertThat().statusCode(200);

        assertExpirableSetCount(context, 0L);

        await().timeout(TWO_SECONDS).until(() -> get("expiresNeverNoHeader").statusCode(), equalTo(200));
        async.complete();
    }

    /**
     * Should be handled as expires never, meaning there should not be
     * an entry in the expirable set when providing a very large value
     * for the x-expire-after header
     */
    @Test
    public void testPutExpiresVeryLate(TestContext context) {
        Async async = context.async();
        given().
                header("x-expire-after", "309475041760").
                body("{ \"foo\": \"bar1\" }").
                when().
                put("expiresVeryLate").
                then().
                assertThat().statusCode(200);

        assertExpirableSetCount(context, 0L);

        await().timeout(TWO_SECONDS).until(() -> get("expiresVeryLate").statusCode(), equalTo(200));

        async.complete();
    }

    @Test
    public void testPutAfterExpiration(TestContext context) {
        Async async = context.async();
        given().
                header("x-expire-after", "1").
                body("{ \"foo\": \"bar1\" }").
                when().
                put("putafterexpiration").
                then().
                assertThat().statusCode(200);

        assertExpirableSetCount(context, 1L);

        await().atMost(3, TimeUnit.SECONDS).until(() -> get("putafterexpiration").statusCode(), equalTo(404));

        given().
                header("x-expire-after", "10").
                body("{ \"foo\": \"bar2\" }").
                when().
                put("putafterexpiration").
                then().
                assertThat().statusCode(200);

        when().get("putafterexpiration").then().statusCode(200).body("foo", equalTo("bar2"));
        async.complete();
    }

    @Test
    public void testPutAfterImmediateExpiration(TestContext context) {
        Async async = context.async();
        given().
                header("x-expire-after", "0").
                body("{ \"foo\": \"bar1\" }").
                when().
                put("expireimmediatly").
                then().
                assertThat().statusCode(200);


        await().atMost(TWO_SECONDS).until(() -> get("expireimmediatly").statusCode(), equalTo(404));

        given().
                header("x-expire-after", "10").
                body("{ \"foo\": \"bar2\" }").
                when().
                put("expireimmediatly").
                then().
                assertThat().statusCode(200);

        when().get("/expireimmediatly").then().statusCode(200).body("foo", equalTo("bar2"));
        async.complete();
    }

    @Test
    public void testPutMultipleBranchesAfterImmediateExpiration(TestContext context) {
        Async async = context.async();
        given().
                header("x-expire-after", "0").
                body("{ \"foo\": \"bar1\" }").
                when().
                put("resexpireimmediatly/branch1").
                then().
                assertThat().statusCode(200);

        given().
                header("x-expire-after", "0").
                body("{ \"foo\": \"bar2\" }").
                when().
                put("resexpireimmediatly/branch2").
                then().
                assertThat().statusCode(200);

        given().
                header("x-expire-after", "0").
                body("{ \"foo\": \"bar3\" }").
                when().
                put("resexpireimmediatly/branch3").
                then().
                assertThat().statusCode(200);


        await().atMost(TWO_SECONDS).until(() -> get("resexpireimmediatly/branch1").statusCode(), equalTo(404));

        when().get("resexpireimmediatly/branch2").then().assertThat().statusCode(404);
        when().get("resexpireimmediatly/branch3").then().assertThat().statusCode(404);

        given().
                header("x-expire-after", "100").
                body("{ \"foo\": \"bar11\" }").
                when().
                put("resexpireimmediatly/branch1").
                then().
                assertThat().statusCode(200);

        given().
                body("{ \"foo\": \"bar22\" }").
                when().
                put("resexpireimmediatly/branch2").
                then().
                assertThat().statusCode(200);

        given().
                header("x-expire-after", "10").
                body("{ \"foo\": \"bar33\" }").
                when().
                put("resexpireimmediatly/branch3").
                then().
                assertThat().statusCode(200);

        when().get("resexpireimmediatly/branch1").then().statusCode(200).body("foo", equalTo("bar11"));
        when().get("resexpireimmediatly/branch2").then().statusCode(200).body("foo", equalTo("bar22"));
        when().get("resexpireimmediatly/branch3").then().statusCode(200).body("foo", equalTo("bar33"));
        async.complete();
    }

    @Test
    public void testPutWithNoExpiryAfterImmediateExpiration(TestContext context) {
        Async async = context.async();
        given().
                header("x-expire-after", "0").
                body("{ \"foo\": \"bar1\" }").
                when().
                put("resexpireimmediatly/branch1").
                then().
                assertThat().statusCode(200);

        when().get("resexpireimmediatly/branch1").then().assertThat().statusCode(404);

        given().
                body("{ \"foo\": \"bar11\" }").
                when().
                put("resexpireimmediatly/branch1").
                then().
                assertThat().statusCode(200);

        when().get("resexpireimmediatly/branch1").then().statusCode(200).body("foo", equalTo("bar11"));
        async.complete();
    }

    @Test
    public void testCollectionDoesNotExpireBeforeContainedResource(TestContext context) {
        Async async = context.async();

        given().
                body("{ \"foo\": \"bar1\" }").
                when().
                put("root/foo/bar1").
                then().
                assertThat().statusCode(200);

        given().
                header("x-expire-after", "1").
                body("{ \"foo\": \"bar2\" }").
                when().
                put("root/foo/bar2").
                then().
                assertThat().statusCode(200);

        await().atMost(3, TimeUnit.SECONDS).until(() -> get("root/foo/bar2").statusCode(), equalTo(404));

        when().get("root/foo/bar1").then().assertThat().statusCode(200);
        when().get("root/foo/bar2").then().assertThat().statusCode(404);
        when().get("root/foo").then().assertThat().statusCode(200)
                .and().body("foo", hasSize(1))
                .and().body("foo", hasItem("bar1"));
        when().get("root").then().assertThat().statusCode(200).and().body("root", hasItem("foo/"));

        async.complete();
    }

    @Test
    public void testCollectionDoesExpireAccordingToOlderContainedResource(TestContext context) {
        Async async = context.async();

        given().
                header("x-expire-after", "2").
                body("{ \"foo\": \"bar1\" }").
                when().
                put("root/foo/bar1").
                then().
                assertThat().statusCode(200);

        given().
                header("x-expire-after", "1").
                body("{ \"foo\": \"bar2\" }").
                when().
                put("root/foo/bar2").
                then().
                assertThat().statusCode(200);

        when().get("root/foo").then().assertThat().statusCode(200).and().body("foo", hasItem("bar2"));

        await().atMost(3, TimeUnit.SECONDS).until(() -> get("root/foo/bar2").statusCode(), equalTo(404));

        when().get("root/foo/bar1").then().assertThat().statusCode(200);
        when().get("root/foo/bar2").then().assertThat().statusCode(404);
        when().get("root/foo/").then().assertThat().statusCode(200)
                .and().body("foo", hasSize(1))
                .and().body("foo", hasItem("bar1"));
        when().get("root").then().assertThat().statusCode(200).and().body("root", hasItem("foo/"));

        await().atMost(3, TimeUnit.SECONDS).until(() -> get("root/foo/bar1").statusCode(), equalTo(404));

        when().get("root/foo/bar1").then().assertThat().statusCode(404);
        when().get("root/foo/bar2").then().assertThat().statusCode(404);
        when().get("root/foo/").then().assertThat().statusCode(404);
        when().get("root").then().assertThat().statusCode(404);

        async.complete();
    }
}
