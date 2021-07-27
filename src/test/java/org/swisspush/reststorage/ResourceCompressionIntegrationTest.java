
package org.swisspush.reststorage;

import io.restassured.specification.RequestSpecification;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static io.restassured.RestAssured.*;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.swisspush.reststorage.util.HttpRequestHeader.COMPRESS_HEADER;
import static org.swisspush.reststorage.util.HttpRequestHeader.IF_NONE_MATCH_HEADER;

@RunWith(VertxUnitRunner.class)
public class ResourceCompressionIntegrationTest extends RedisStorageIntegrationTestCase {

    @Test
    public void testPutGetWithCompression(TestContext context) {
        Async async = context.async();
        putResource("{ \"foo\": \"bar\" }", true, 200);
        getResource("res", 200, "foo", "bar");
        async.complete();
    }

    /**
     * The resource has always to be overwritten, if the compression state differs between the sent resource
     * and the stored resource. This behaviour is, to prevent unexpected behaviour considering the etag mechanism.
     */
    @Test
    public void testStoreCompressedAndUnCompressed() {

        String originalContent = "{\"content\": \"originalContent\"}";
        String modifiedContent = "{\"content\": \"modifiedContent\"}";

        // Scenario 1: PUT 1 = {uncompressed, etag1}, PUT 2 = {compressed, etag1}
        putResource(originalContent, false, 200);
        putResource(modifiedContent, true, 200);
        getResource("res", 200, "content", "modifiedContent");

        // Scenario 2: PUT 1 = {compressed, etag1}, PUT 2 = {uncompressed, etag1}
        jedis.flushAll();
        putResource(originalContent, true, 200);
        putResource(modifiedContent, false, 200);
        getResource("res", 200, "content", "modifiedContent");

        // Scenario 3: PUT 1 = {compressed, etag1}, PUT 2 = {compressed, etag1}
        jedis.flushAll();
        putResource(originalContent, true, 200);
        putResource(modifiedContent, true, 304);
        getResource("res", 200, "content", "originalContent");

        // Scenario 4: PUT 1 = {uncompressed, etag1}, PUT 2 = {uncompressed, etag1}
        jedis.flushAll();
        putResource(originalContent, false, 200);
        putResource(modifiedContent, false, 304);
        getResource("res", 200, "content", "originalContent");
    }

    @Test
    public void testCompressAndMerge(TestContext context) {
        Async async = context.async();
        with()
                .header(COMPRESS_HEADER.getName(), "true")
                .param("merge", "true")
                .body("{ \"foo\": \"bar2\" }")
                .put("res")
                .then().assertThat()
                .statusCode(400)
                .body(containsString("Invalid parameter/header combination: merge parameter and " + COMPRESS_HEADER.getName() + " header cannot be used concurrently"));
        async.complete();
    }

    @Test
    public void testGetFailHandlingForCorruptCompressedData(TestContext context) {
        Async async = context.async();
        putResource("{ \"foo\": \"bar\" }", true, 200);

        // cripple compressed data to make it impossible to decompress
        jedis.hset("rest-storage:resources:res", "resource", "xxx");

        when()
                .get("res")
                .then().assertThat()
                .statusCode(500)
                .body(containsString("Error during decompression of resource: Not in GZIP format"));

        async.complete();
    }

    private void putResource(String body, boolean storeCompressed, int statusCode){
        RequestSpecification spec = given().header(IF_NONE_MATCH_HEADER.getName(), "etag1");
        if(storeCompressed){
            spec = spec.header(COMPRESS_HEADER.getName(), "true");
        }
        spec.body(body).put("res").then().assertThat().statusCode(statusCode);
    }

    private void getResource(String path, int statusCode, String bodyProperty, String equalToValue){
        when()
                .get(path)
                .then().assertThat()
                .statusCode(statusCode)
                .body(bodyProperty, equalTo(equalToValue));
    }
}
