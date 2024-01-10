
package org.swisspush.reststorage;

import io.restassured.RestAssured;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.swisspush.reststorage.redis.RedisStorageIntegrationTestCase;

import java.time.Duration;
import java.util.List;

import static io.restassured.RestAssured.*;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;

@RunWith(VertxUnitRunner.class)
public class CrudIntegrationTest extends RedisStorageIntegrationTestCase {

    @Test
    public void testDoubleSlashesHandling(TestContext context) {
        Async async = context.async();

        // put with double slashes
        with().urlEncodingEnabled(false).body("{ \"foo\": \"bar\" }").put("resources//myres");
        // get with single/double slashes should both work
        when().get("resources/myres").then().assertThat().body("foo", equalTo("bar"));
        given().urlEncodingEnabled(false).when().get("resources//myres").then().assertThat().body("foo", equalTo("bar"));

        // delete with double slashes. after this, the resource should be gone
        given().urlEncodingEnabled(false).delete("resources//myres").then().assertThat().statusCode(200);
        given().delete("resources/myres").then().assertThat().statusCode(404);

        // get again with double slashes
        when().get("resources/myres").then().assertThat().statusCode(404);
        given().urlEncodingEnabled(false).when().get("resources//myres").then().assertThat().statusCode(404);

        async.complete();
    }

    @Test
    public void testPutGetDelete(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar\" }").put("res");
        when().get("res").then().assertThat().body("foo", equalTo("bar"));
        delete("res");
        when().get("res").then().assertThat().statusCode(404);
        async.complete();
    }

    @Test
    public void testList(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar\" }").put("resources/res1");
        with().body("{ \"foo\": \"bar2\" }").put("resources/res2");

        checkGETStatusCodeWithAwait5sec("resources/res1", 200);
        checkGETStatusCodeWithAwait5sec("resources/res2", 200);

        when().get("resources").then().assertThat().body("resources", hasItems("res1", "res2"));
        async.complete();
    }

    @Test
    public void testMerge(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar\", \"hello\": \"world\" }").put("res");
        checkGETStatusCodeWithAwait5sec("res", 200);
        with().
                param("merge", "true").
                body("{ \"foo\": \"bar2\" }").
                put("res");
        checkGETStatusCodeWithAwait5sec("res", 200);
        get("res").then().assertThat().
                statusCode(200).
                body("foo", equalTo("bar2")).
                body("hello", equalTo("world"));
        async.complete();
    }

    @Test
    public void testTwoBranchesDeleteOnLeafOfOneBranch(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar1\" }").put("branch1/res/leaf");
        with().body("{ \"foo\": \"bar2\" }").put("branch2/res/leaf");
        checkGETStatusCodeWithAwait5sec("branch1/res/leaf", 200);
        checkGETStatusCodeWithAwait5sec("branch2/res/leaf", 200);

        delete("branch2").then().assertThat().statusCode(400).body(equalTo("Bad Request: directory not empty. Use recursive=true parameter to delete"));
        checkGETStatusCodeWithAwait5sec("branch2", 200);

        RestAssured.basePath = "";

        when().get("branch1").then().assertThat().statusCode(200);
        when().get("branch1/res").then().assertThat().statusCode(200);
        when().get("branch1/res/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar1"));
        when().get("branch2").then().assertThat().statusCode(200);
        when().get("branch2/res").then().assertThat().statusCode(200);
        when().get("branch2/res/leaf").then().assertThat().statusCode(200);

        delete("branch2/res/leaf");
        async.complete();
    }

    @Test
    public void testTwoBranchesDeleteOnLeafOfOneBranchRecursive(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar1\" }").put("branch1/res/leaf");
        with().body("{ \"foo\": \"bar2\" }").put("branch2/res/leaf");
        checkGETStatusCodeWithAwait5sec("branch1/res/leaf", 200);
        checkGETStatusCodeWithAwait5sec("branch2/res/leaf", 200);

        with().param("recursive", "true").delete("branch2");
        checkGETStatusCodeWithAwait5sec("branch2", 404);

        RestAssured.basePath = "";

        when().get("branch1").then().assertThat().statusCode(200);
        when().get("branch1/res").then().assertThat().statusCode(200);
        when().get("branch1/res/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar1"));
        when().get("branch2").then().assertThat().statusCode(404);
        when().get("branch2/res").then().assertThat().statusCode(404);
        when().get("branch2/res/leaf").then().assertThat().statusCode(404);

        delete("branch2/res/leaf");
        async.complete();
    }

    @Test
    public void testTwoBranchesDeleteOnNodeUnderLeafOfOneBranch(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar1\" }").put("branch1/res/res1/leaf");
        with().body("{ \"foo\": \"bar2\" }").put("branch2/res/res2/leaf");
        checkGETStatusCodeWithAwait5sec("branch1/res/res1/leaf", 200);
        checkGETStatusCodeWithAwait5sec("branch2/res/res2/leaf", 200);

        delete("branch2/res").then().assertThat().statusCode(400).body(equalTo("Bad Request: directory not empty. Use recursive=true parameter to delete"));
        checkGETStatusCodeWithAwait5sec("branch2/res", 200);

        RestAssured.basePath = "";

        when().get("branch1").then().assertThat().statusCode(200);
        when().get("branch1/res").then().assertThat().statusCode(200);
        when().get("branch1/res/res1/").then().assertThat().statusCode(200);
        when().get("branch1/res/res1/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar1"));
        when().get("branch2").then().assertThat().statusCode(200);
        when().get("branch2/res").then().assertThat().statusCode(200);
        when().get("branch2/res/res2").then().assertThat().statusCode(200);
        when().get("branch2/res/res2/leaf").then().assertThat().statusCode(200);
        async.complete();
    }

    @Test
    public void testTwoBranchesDeleteOnNodeUnderLeafOfOneBranchRecursive(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar1\" }").put("branch1/res/res1/leaf");
        with().body("{ \"foo\": \"bar2\" }").put("branch2/res/res2/leaf");
        checkGETStatusCodeWithAwait5sec("branch1/res/res1/leaf", 200);
        checkGETStatusCodeWithAwait5sec("branch2/res/res2/leaf", 200);

        with().param("recursive", "true").delete("branch2/res");
        checkGETStatusCodeWithAwait5sec("branch2/res", 404);

        RestAssured.basePath = "";

        when().get("branch1").then().assertThat().statusCode(200);
        when().get("branch1/res").then().assertThat().statusCode(200);
        when().get("branch1/res/res1/").then().assertThat().statusCode(200);
        when().get("branch1/res/res1/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar1"));
        when().get("branch2").then().assertThat().statusCode(404);
        when().get("branch2/res").then().assertThat().statusCode(404);
        when().get("branch2/res/res2").then().assertThat().statusCode(404);
        when().get("branch2/res/res2/leaf").then().assertThat().statusCode(404);
        async.complete();
    }

    @Test
    public void testTwoBranchesDeleteOnNodeAfterBranch(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar1\" }").put("branch1/res/res1/leaf");
        with().body("{ \"foo\": \"bar2\" }").put("branch2/res/res2/leaf");
        checkGETStatusCodeWithAwait5sec("branch1/res/res1/leaf", 200);
        checkGETStatusCodeWithAwait5sec("branch2/res/res2/leaf", 200);

        delete("branch1/res").then().assertThat().statusCode(400).body(equalTo("Bad Request: directory not empty. Use recursive=true parameter to delete"));
        checkGETStatusCodeWithAwait5sec("branch1/res", 200);

        RestAssured.basePath = "";

        when().get("branch2").then().assertThat().statusCode(200);
        when().get("branch2/res").then().assertThat().statusCode(200);
        when().get("branch2/res/res2/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar2"));
        when().get("branch1").then().assertThat().statusCode(200);
        when().get("branch1/res").then().assertThat().statusCode(200);
        when().get("branch1/res/res1/leaf").then().assertThat().statusCode(200);
        async.complete();
    }

    @Test
    public void testTwoBranchesDeleteOnNodeAfterBranchRecursive(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar1\" }").put("branch1/res/res1/leaf");
        with().body("{ \"foo\": \"bar2\" }").put("branch2/res/res2/leaf");
        checkGETStatusCodeWithAwait5sec("branch1/res/res1/leaf", 200);
        checkGETStatusCodeWithAwait5sec("branch2/res/res2/leaf", 200);

        with().param("recursive", "true").delete("branch1/res").then().assertThat().statusCode(200);
        checkGETStatusCodeWithAwait5sec("branch1/res", 404);

        RestAssured.basePath = "";

        when().get("branch2").then().assertThat().statusCode(200);
        when().get("branch2/res").then().assertThat().statusCode(200);
        when().get("branch2/res/res2/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar2"));
        when().get("branch1").then().assertThat().statusCode(404);
        when().get("branch1/res").then().assertThat().statusCode(404);
        when().get("branch1/res/leaf").then().assertThat().statusCode(404);
        when().get("branch1/res/res1/leaf").then().assertThat().statusCode(404);
        async.complete();
    }

    @Test
    public void testTwoBranchesDeleteOnRoot(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar\" }").put("node/node/node/branch1/res/res1/leaf");
        with().body("{ \"foo\": \"bar\" }").put("node/node/node/branch2/res/res2/leaf");
        checkGETStatusCodeWithAwait5sec("node/node/node/branch1/res/res1/leaf", 200);
        checkGETStatusCodeWithAwait5sec("node/node/node/branch2/res/res2/leaf", 200);

        RestAssured.basePath = "";

        delete("/").then().assertThat().statusCode(400).body(equalTo("Bad Request: directory not empty. Use recursive=true parameter to delete"));
        checkGETStatusCodeWithAwait5sec("/", 200);

        when().get("node").then().assertThat().statusCode(200);
        when().get("node/node").then().assertThat().statusCode(200);
        when().get("node/node/node").then().assertThat().statusCode(200);
        when().get("node/node/node/branch2").then().assertThat().statusCode(200);
        when().get("node/node/node/branch2/res").then().assertThat().statusCode(200);
        when().get("node/node/node/branch2/res/res2/leaf").then().assertThat().statusCode(200);
        when().get("node/node/node/branch1").then().assertThat().statusCode(200);
        when().get("node/node/node/branch1/res").then().assertThat().statusCode(200);
        when().get("node/node/node/branch1/res/res1/leaf").then().assertThat().statusCode(200);
        async.complete();
    }

    @Test
    public void testTwoBranchesDeleteOnRootRecursive(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar\" }").put("node/node/node/branch1/res/res1/leaf");
        with().body("{ \"foo\": \"bar\" }").put("node/node/node/branch2/res/res2/leaf");
        checkGETStatusCodeWithAwait5sec("node/node/node/branch1/res/res1/leaf", 200);
        checkGETStatusCodeWithAwait5sec("node/node/node/branch2/res/res2/leaf", 200);

        RestAssured.basePath = "";

        with().param("recursive", "true").delete("/").then().assertThat().statusCode(200);
        checkGETStatusCodeWithAwait5sec("/", 404);

        when().get("node").then().assertThat().statusCode(404);
        when().get("node/node").then().assertThat().statusCode(404);
        when().get("node/node/node").then().assertThat().statusCode(404);
        when().get("node/node/node/branch2").then().assertThat().statusCode(404);
        when().get("node/node/node/branch2/res").then().assertThat().statusCode(404);
        when().get("node/node/node/branch2/res/res2/leaf").then().assertThat().statusCode(404);
        when().get("node/node/node/branch1").then().assertThat().statusCode(404);
        when().get("node/node/node/branch1/res").then().assertThat().statusCode(404);
        when().get("node/node/node/branch1/res/leaf").then().assertThat().statusCode(404);
        when().get("node/node/node/branch1/res/res1/leaf").then().assertThat().statusCode(404);
        async.complete();
    }

    @Test
    public void testThreeBranchesDeleteOnLeafOfOneBranch(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar1\" }").put("node/branch1/res/leaf");
        with().body("{ \"foo\": \"bar2\" }").put("node/branch2/res/leaf");
        with().body("{ \"foo\": \"bar3\" }").put("node/branch3/res/leaf");
        checkGETStatusCodeWithAwait5sec("node/branch1/res/leaf", 200);
        checkGETStatusCodeWithAwait5sec("node/branch2/res/leaf", 200);
        checkGETStatusCodeWithAwait5sec("node/branch3/res/leaf", 200);

        delete("node/branch3/res/leaf");
        checkGETStatusCodeWithAwait5sec("node/branch3/res/leaf", 404);

        RestAssured.basePath = "";

        when().get("node/branch1").then().assertThat().statusCode(200);
        when().get("node/branch1/res").then().assertThat().statusCode(200);
        when().get("node/branch1/res/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar1"));
        when().get("node/branch2").then().assertThat().statusCode(200);
        when().get("node/branch2/res").then().assertThat().statusCode(200);
        when().get("node/branch2/res/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar2"));
        when().get("node/branch3").then().assertThat().statusCode(404);
        when().get("node/branch3/res").then().assertThat().statusCode(404);
        when().get("node/branch3/res/leaf").then().assertThat().statusCode(404);
        async.complete();
    }

    @Test
    public void testThreeBranchesDeleteOnNodeUnderLeafOfOneBranch(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar1\" }").put("branch1/res/res1/leaf");
        with().body("{ \"foo\": \"bar2\" }").put("branch2/res/res2/leaf");
        with().body("{ \"foo\": \"bar3\" }").put("branch3/res/res3/leaf");
        checkGETStatusCodeWithAwait5sec("branch1/res/res1/leaf", 200);
        checkGETStatusCodeWithAwait5sec("branch2/res/res2/leaf", 200);
        checkGETStatusCodeWithAwait5sec("branch3/res/res3/leaf", 200);

        delete("branch1/res").then().assertThat().statusCode(400).body(equalTo("Bad Request: directory not empty. Use recursive=true parameter to delete"));
        checkGETStatusCodeWithAwait5sec("branch1/res", 200);

        RestAssured.basePath = "";

        when().get("/branch1").then().assertThat().statusCode(200);
        when().get("/branch1/res").then().assertThat().statusCode(200);
        when().get("/branch1/res/res1/").then().assertThat().statusCode(200);
        when().get("/branch1/res/res1/leaf").then().assertThat().statusCode(200);
        when().get("/branch2").then().assertThat().statusCode(200);
        when().get("/branch2/res").then().assertThat().statusCode(200);
        when().get("/branch2/res/res2").then().assertThat().statusCode(200);
        when().get("/branch2/res/res2/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar2"));
        when().get("/branch3").then().assertThat().statusCode(200);
        when().get("/branch3/res").then().assertThat().statusCode(200);
        when().get("/branch3/res/res3").then().assertThat().statusCode(200);
        when().get("/branch3/res/res3/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar3"));
        async.complete();
    }

    @Test
    public void testThreeBranchesDeleteOnNodeUnderLeafOfOneBranchRecursive(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar1\" }").put("branch1/res/res1/leaf");
        with().body("{ \"foo\": \"bar2\" }").put("branch2/res/res2/leaf");
        with().body("{ \"foo\": \"bar3\" }").put("branch3/res/res3/leaf");
        checkGETStatusCodeWithAwait5sec("branch1/res/res1/leaf", 200);
        checkGETStatusCodeWithAwait5sec("branch2/res/res2/leaf", 200);
        checkGETStatusCodeWithAwait5sec("branch3/res/res3/leaf", 200);

        with().param("recursive", "true").delete("branch1/res").then().assertThat().statusCode(200);
        checkGETStatusCodeWithAwait5sec("branch1/res", 404);

        RestAssured.basePath = "";

        when().get("/branch1").then().assertThat().statusCode(404);
        when().get("/branch1/res").then().assertThat().statusCode(404);
        when().get("/branch1/res/res1/").then().assertThat().statusCode(404);
        when().get("/branch1/res/res1/leaf").then().assertThat().statusCode(404);
        when().get("/branch2").then().assertThat().statusCode(200);
        when().get("/branch2/res").then().assertThat().statusCode(200);
        when().get("/branch2/res/res2").then().assertThat().statusCode(200);
        when().get("/branch2/res/res2/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar2"));
        when().get("/branch3").then().assertThat().statusCode(200);
        when().get("/branch3/res").then().assertThat().statusCode(200);
        when().get("/branch3/res/res3").then().assertThat().statusCode(200);
        when().get("/branch3/res/res3/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar3"));
        async.complete();
    }

    @Test
    public void testThreeBranchesDeleteOnNodeAfterBranch(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar1\" }").put("branch1/res/res1/leaf");
        with().body("{ \"foo\": \"bar2\" }").put("branch2/res/res2/leaf");
        with().body("{ \"foo\": \"bar3\" }").put("branch3/res/res3/leaf");
        checkGETStatusCodeWithAwait5sec("branch1/res/res1/leaf", 200);
        checkGETStatusCodeWithAwait5sec("branch2/res/res2/leaf", 200);
        checkGETStatusCodeWithAwait5sec("branch3/res/res3/leaf", 200);

        delete("branch2/res").then().assertThat().statusCode(400).body(equalTo("Bad Request: directory not empty. Use recursive=true parameter to delete"));
        checkGETStatusCodeWithAwait5sec("branch2/res", 200);

        RestAssured.basePath = "";


        when().get("branch1").then().assertThat().statusCode(200);
        when().get("branch1/res").then().assertThat().statusCode(200);
        when().get("branch1/res/res1").then().assertThat().statusCode(200);
        when().get("branch1/res/res1/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar1"));
        when().get("branch2").then().assertThat().statusCode(200);
        when().get("branch2/res").then().assertThat().statusCode(200);
        when().get("branch2/res/res2").then().assertThat().statusCode(200);
        when().get("branch2/res/res2/leaf").then().assertThat().statusCode(200);
        when().get("branch3").then().assertThat().statusCode(200);
        when().get("branch3/res").then().assertThat().statusCode(200);
        when().get("branch3/res/res3").then().assertThat().statusCode(200);
        when().get("branch3/res/res3/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar3"));
        async.complete();
    }

    @Test
    public void testThreeBranchesDeleteOnNodeAfterBranchRecursive(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar1\" }").put("branch1/res/res1/leaf");
        with().body("{ \"foo\": \"bar2\" }").put("branch2/res/res2/leaf");
        with().body("{ \"foo\": \"bar3\" }").put("branch3/res/res3/leaf");
        checkGETStatusCodeWithAwait5sec("branch1/res/res1/leaf", 200);
        checkGETStatusCodeWithAwait5sec("branch2/res/res2/leaf", 200);
        checkGETStatusCodeWithAwait5sec("branch3/res/res3/leaf", 200);

        with().param("recursive", "true").delete("branch2/res").then().assertThat().statusCode(200);
        checkGETStatusCodeWithAwait5sec("branch2/res", 404);

        RestAssured.basePath = "";


        when().get("branch1").then().assertThat().statusCode(200);
        when().get("branch1/res").then().assertThat().statusCode(200);
        when().get("branch1/res/res1").then().assertThat().statusCode(200);
        when().get("branch1/res/res1/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar1"));
        when().get("branch2").then().assertThat().statusCode(404);
        when().get("branch2/res").then().assertThat().statusCode(404);
        when().get("branch2/res/res2").then().assertThat().statusCode(404);
        when().get("branch2/res/res2/leaf").then().assertThat().statusCode(404);
        when().get("branch3").then().assertThat().statusCode(200);
        when().get("branch3/res").then().assertThat().statusCode(200);
        when().get("branch3/res/res3").then().assertThat().statusCode(200);
        when().get("branch3/res/res3/leaf").then().assertThat().statusCode(200).body("foo", equalTo("bar3"));
        async.complete();
    }

    @Test
    public void testThreeBranchesDeleteOnRoot(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar\" }").put("node/node/node/branch1/res/res1/leaf");
        with().body("{ \"foo\": \"bar\" }").put("node/node/node/branch2/res/res2/leaf");
        with().body("{ \"foo\": \"bar\" }").put("node/node/node/branch3/res/res3/leaf");
        checkGETStatusCodeWithAwait5sec("node/node/node/branch1/res/res1/leaf", 200);
        checkGETStatusCodeWithAwait5sec("node/node/node/branch2/res/res2/leaf", 200);
        checkGETStatusCodeWithAwait5sec("node/node/node/branch3/res/res3/leaf", 200);

        RestAssured.basePath = "";

        delete("/").then().assertThat().statusCode(400).body(equalTo("Bad Request: directory not empty. Use recursive=true parameter to delete"));
        checkGETStatusCodeWithAwait5sec("/", 200);

        when().get("node").then().assertThat().statusCode(200);
        when().get("node/node").then().assertThat().statusCode(200);
        when().get("node/node/node").then().assertThat().statusCode(200);
        when().get("node/node/node/branch2").then().assertThat().statusCode(200);
        when().get("node/node/node/branch2/res").then().assertThat().statusCode(200);
        when().get("node/node/node/branch2/res/res2/leaf").then().assertThat().statusCode(200);
        when().get("node/node/node/branch1").then().assertThat().statusCode(200);
        when().get("node/node/node/branch1/res").then().assertThat().statusCode(200);
        when().get("node/node/node/branch1/res/res1/leaf").then().assertThat().statusCode(200);
        when().get("node/node/node/branch3").then().assertThat().statusCode(200);
        when().get("node/node/node/branch3/res").then().assertThat().statusCode(200);
        when().get("node/node/node/branch3/res/res3/leaf").then().assertThat().statusCode(200);
        async.complete();
    }

    @Test
    public void testThreeBranchesDeleteOnRootRecursive(TestContext context) {
        Async async = context.async();
        with().body("{ \"foo\": \"bar\" }").put("node/node/node/branch1/res/res1/leaf");
        with().body("{ \"foo\": \"bar\" }").put("node/node/node/branch2/res/res2/leaf");
        with().body("{ \"foo\": \"bar\" }").put("node/node/node/branch3/res/res3/leaf");
        checkGETStatusCodeWithAwait5sec("node/node/node/branch1/res/res1/leaf", 200);
        checkGETStatusCodeWithAwait5sec("node/node/node/branch2/res/res2/leaf", 200);
        checkGETStatusCodeWithAwait5sec("node/node/node/branch3/res/res3/leaf", 200);

        RestAssured.basePath = "";

        with().param("recursive", "true").delete("/").then().assertThat().statusCode(200);
        checkGETStatusCodeWithAwait5sec("/", 404);

        when().get("node").then().assertThat().statusCode(404);
        when().get("node/node").then().assertThat().statusCode(404);
        when().get("node/node/node").then().assertThat().statusCode(404);
        when().get("node/node/node/branch2").then().assertThat().statusCode(404);
        when().get("node/node/node/branch2/res").then().assertThat().statusCode(404);
        when().get("node/node/node/branch2/res/res2/leaf").then().assertThat().statusCode(404);
        when().get("node/node/node/branch1").then().assertThat().statusCode(404);
        when().get("node/node/node/branch1/res").then().assertThat().statusCode(404);
        when().get("node/node/node/branch1/res/leaf").then().assertThat().statusCode(404);
        when().get("node/node/node/branch1/res/res1/leaf").then().assertThat().statusCode(404);
        when().get("node/node/node/branch3").then().assertThat().statusCode(404);
        when().get("node/node/node/branch3/res").then().assertThat().statusCode(404);
        when().get("node/node/node/branch3/res/leaf").then().assertThat().statusCode(404);
        when().get("node/node/node/branch3/res/res3/leaf").then().assertThat().statusCode(404);
        async.complete();
    }

    @Test
    public void testGetSingleResourceWithColonsAndSemiColonsInName(TestContext context) {
        Async async = context.async();

        with().urlEncodingEnabled(false).body("{ \"foo\": \"bar\" }").put("resources/1_hello-:@$&()*+,;=-._~!'");
        checkGETStatusCodeWithAwait5secNoUrlEncoding("resources/1_hello-:@$&()*+,;=-._~!'", 200);

        async.complete();
    }

    @Test
    public void testGetAndDeleteResourceWithColonsAndSemiColonsInTheName(TestContext context) {
        Async async = context.async();

        String res1 = "999_hello-:@$&()*+,;=-._~!'";

        // PUT resource
        with().urlEncodingEnabled(false).body("{ \"foo\": \"bar_999\" }").put("resources/"+res1);

        // GET resource
        checkGETStatusCodeWithAwait5secNoUrlEncoding("resources/"+res1, 200);
        context.assertEquals("bar_999", given().urlEncodingEnabled(false).when().get("resources/"+res1).jsonPath().getString("foo"));

        // DELETE resource
        given().urlEncodingEnabled(false).when().delete("resources/"+res1).then().assertThat().statusCode(200);

        // check if deleted
        given().urlEncodingEnabled(false).when().get("resources/"+res1).then().assertThat().statusCode(404);

        async.complete();
    }

    @Test
    public void testGetCollectionHavingResourcesWithColonsAndSemiColonsInTheName(TestContext context) {
        Async async = context.async();

        String res1 = "1_hello-:@$&()*+,;=-._~!'";
        String res2 = "2_hello-:@$&()*+,;=-._~!'";
        String res3 = "3_hello";

        with().urlEncodingEnabled(false).body("{ \"foo\": \"bar\" }").put("resources/"+res1);
        with().urlEncodingEnabled(false).body("{ \"foo\": \"bar2\" }").put("resources/"+res2);
        with().urlEncodingEnabled(false).body("{ \"foo\": \"bar3\" }").put("resources/"+res3);

        checkGETStatusCodeWithAwait5secNoUrlEncoding("resources/"+res1, 200);
        checkGETStatusCodeWithAwait5secNoUrlEncoding("resources/"+res2, 200);
        checkGETStatusCodeWithAwait5secNoUrlEncoding("resources/"+res3, 200);

        when().get("resources").then().assertThat().body("resources", hasItems(res1, res2, res3));
        async.complete();
    }

    @Test
    public void testGetHTMLCollectionHavingResourcesWithColonsAndSemiColonsInTheName(TestContext context) {
        Async async = context.async();

        String res1 = "1_hello-:@$&()*+,;=-._~!'";
        String res2 = "2_hello-:@$&()*+,;=-._~!'";
        String res3 = "3_hello";

        with().urlEncodingEnabled(false).body("{ \"foo\": \"bar\" }").put("resources/"+res1);
        with().urlEncodingEnabled(false).body("{ \"foo\": \"bar2\" }").put("resources/"+res2);
        with().urlEncodingEnabled(false).body("{ \"foo\": \"bar3\" }").put("resources/"+res3);

        checkGETStatusCodeWithAwait5secNoUrlEncoding("resources/"+res1, 200);
        checkGETStatusCodeWithAwait5secNoUrlEncoding("resources/"+res2, 200);
        checkGETStatusCodeWithAwait5secNoUrlEncoding("resources/"+res3, 200);

        String title = given().header("Accept", "text/html").when().get("resources").htmlPath().getString("html.head.title");
        context.assertEquals("resources", title);

        List<String> listItems = given().header("Accept", "text/html").when().get("resources").htmlPath().getList("html.body.ul.li");
        context.assertEquals(res1, listItems.get(1));
        context.assertEquals(res2, listItems.get(2));
        context.assertEquals(res3, listItems.get(3));

        async.complete();
    }

    private void checkGETStatusCodeWithAwait5sec(final String request, final Integer statusCode) {
        await().atMost(Duration.ofSeconds(5)).until(() -> String.valueOf(when().get(request).getStatusCode()), equalTo(String.valueOf(statusCode)));
    }

    private void checkGETStatusCodeWithAwait5secNoUrlEncoding(final String request, final Integer statusCode) {
        await().atMost(Duration.ofSeconds(5)).until(() -> String.valueOf(given().urlEncodingEnabled(false).when().get(request).getStatusCode()), equalTo(String.valueOf(statusCode)));
    }
}
