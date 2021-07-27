/*
 * ------------------------------------------------------------------------------------------------
 * Copyright 2014 by Swiss Post, Information Technology Services
 * ------------------------------------------------------------------------------------------------
 * $Id$
 * ------------------------------------------------------------------------------------------------
 */

package org.swisspush.reststorage;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static io.restassured.RestAssured.*;
import static org.hamcrest.Matchers.*;


@RunWith(VertxUnitRunner.class)
public class StorageExpandIntegrationTest extends RedisStorageIntegrationTestCase {

    final String ETAG_HEADER = "Etag";
    final String IF_NONE_MATCH_HEADER = "if-none-match";
    final String POST_STORAGE_EXP = "/server/resources?storageExpand=true";
    final int BAD_REQUEST = 400;
    final String BAD_REQUEST_PARSE_MSG = "Bad Request: Unable to parse body of storageExpand POST request";
    final String COMPRESS_HEADER = "x-stored-compressed";

    @Before
    public void setPath() {
        RestAssured.basePath = "/server/resources";
        delete("/");
    }

    @Test
    public void testPOSTWithoutExpandParam(TestContext context) {
        Async async = context.async();
        given()
                .body("{ \"foo\": \"bar1\" }")
                .when()
                .post("/server/resources")
                .then()
                .assertThat().statusCode(405);

        async.complete();
    }

    @Test
    public void testPOSTWithWrongBody(TestContext context) {
        Async async = context.async();
        given()
                .body("{ \"foo\": \"bar1\" }")
                .when()
                .post(POST_STORAGE_EXP)
                .then()
                .assertThat().statusCode(BAD_REQUEST)
                .assertThat().body(equalTo("Bad Request: Expected array field 'subResources' with names of resources"));

        given()
                .body("{ \"foo\": bar1 }")
                .when()
                .post(POST_STORAGE_EXP)
                .then()
                .assertThat().statusCode(BAD_REQUEST)
                .assertThat().body(equalTo(BAD_REQUEST_PARSE_MSG));

        given()
                .body("{ \"subResources\": [\"res1\", \"res2\", 123] }")
                .when()
                .post(POST_STORAGE_EXP)
                .then()
                .assertThat().statusCode(BAD_REQUEST)
                .assertThat().body(equalTo(BAD_REQUEST_PARSE_MSG));

        given()
                .body("\"foo\": \"bar1\"")
                .when()
                .post(POST_STORAGE_EXP)
                .then()
                .assertThat().statusCode(BAD_REQUEST)
                .assertThat().body(equalTo(BAD_REQUEST_PARSE_MSG));

        async.complete();
    }

    @Test
    public void testWithInvalidResource(TestContext context) {
        Async async = context.async();
        delete("/server/resources");

        with().body("{ \"foo\": \"bar1\" }").put("/server/resources/res1");

        // invalid
        with().body("{ \"foo\"}").put("/server/resources/res2");

        with().body("{ \"foo\": \"bar3\" }").put("/server/resources/res3");

        given()
                .body("{ \"subResources\": [\"res1\", \"res2\", \"res3\"] }")
                .when()
                .post(POST_STORAGE_EXP)
                .then()
                .assertThat().statusCode(500)
                .body("", hasKey("error"))
                .body("error", equalTo("Error decoding invalid json resource 'res2'"));

        async.complete();
    }

    @Test
    public void testWithMultipleInvalidResources(TestContext context) {
        Async async = context.async();
        delete("/server/resources");

        // invalid
        with().body("{ \"foo\": \"bar1\"").put("/server/resources/res1");

        // invalid
        with().body("{ \"foo\"}").put("/server/resources/res2");

        with().body("{ \"foo\": \"bar3\" }").put("/server/resources/res3");

        given()
                .body("{ \"subResources\": [\"res1\", \"res2\", \"res3\"] }")
                .when()
                .post(POST_STORAGE_EXP)
                .then()
                .assertThat().statusCode(500)
                .body("", hasKey("error"))
                .body("error", equalTo("Error decoding invalid json resource 'res1'"));

        async.complete();
    }

    @Test
    public void testSimpleWith3Resources(TestContext context) {
        Async async = context.async();
        delete("/server/resources");

        with().body("{ \"foo\": \"bar1\" }").put("/server/resources/res1");
        with().body("{ \"foo\": \"bar2\" }").put("/server/resources/res2");
        with().body("{ \"foo\": \"bar3\" }").put("/server/resources/res3");

        given()
                .body("{ \"subResources\": [\"res1\", \"res2\", \"res3\"] }")
                .when()
                .post(POST_STORAGE_EXP)
                .then()
                .assertThat().statusCode(200).contentType(ContentType.JSON).header(ETAG_HEADER, not(empty()))
                .body("", allOf(hasKey("res1"), hasKey("res2"), hasKey("res3")))
                .body("res1.foo", equalTo("bar1"))
                .body("res2.foo", equalTo("bar2"))
                .body("res3.foo", equalTo("bar3"));

        async.complete();
    }

    @Test
    public void testDoubleSlashesHandlingForPOSTRequests(TestContext context) {
        Async async = context.async();
        delete("/server/resources");

        with().body("{ \"foo\": \"bar1\" }").put("/server/resources/res1");
        with().body("{ \"foo\": \"bar2\" }").put("/server/resources/res2");
        with().body("{ \"foo\": \"bar3\" }").put("/server/resources/res3");

        given()
                .urlEncodingEnabled(false)
                .body("{ \"subResources\": [\"res1\", \"res2\", \"res3\"] }")
                .when()
                .post("/server//resources?storageExpand=true")
                .then()
                .assertThat().statusCode(200).contentType(ContentType.JSON).header(ETAG_HEADER, not(empty()))
                .body("", allOf(hasKey("res1"), hasKey("res2"), hasKey("res3")))
                .body("res1.foo", equalTo("bar1"))
                .body("res2.foo", equalTo("bar2"))
                .body("res3.foo", equalTo("bar3"));

        async.complete();
    }

    @Test
    public void testSimpleWithUnknownSubResources(TestContext context) {
        Async async = context.async();
        delete("/server/resources");

        with().body("{ \"foo\": \"bar1\" }").put("/server/resources/res1");
        with().body("{ \"foo\": \"bar2\" }").put("/server/resources/res2");
        with().body("{ \"foo\": \"bar3\" }").put("/server/resources/res3");

        given()
                .body("{ \"subResources\": [\"res1\", \"res2XX\", \"res3\"] }")
                .when()
                .post(POST_STORAGE_EXP)
                .then()
                .assertThat().statusCode(200).contentType(ContentType.JSON).header(ETAG_HEADER, not(empty()))
                .body("", allOf(hasKey("res1"), hasKey("res3")))
                .body("", not(hasKey("res2")))
                .body("res1.foo", equalTo("bar1"))
                .body("res3.foo", equalTo("bar3"));

        async.complete();
    }

    @Test
    public void testSimpleWithMissingSubResources(TestContext context) {
        Async async = context.async();
        delete("/server/resources");

        with().body("{ \"foo\": \"bar1\" }").put("/server/resources/res1");
        with().body("{ \"foo\": \"bar3\" }").put("/server/resources/res3");

        given()
                .body("{ \"subResources\": [\"res1\", \"res2\", \"res3\"] }")
                .when()
                .post(POST_STORAGE_EXP)
                .then()
                .assertThat().statusCode(200).contentType(ContentType.JSON).header(ETAG_HEADER, not(empty()))
                .body("", allOf(hasKey("res1"), hasKey("res3")))
                .body("", not(hasKey("res2")))
                .body("res1.foo", equalTo("bar1"))
                .body("res3.foo", equalTo("bar3"));

        async.complete();
    }

    @Test
    public void testSimpleWithEmptyResult(TestContext context) {
        Async async = context.async();
        delete("/server/resources");

        given()
                .body("{ \"subResources\": [\"res1\", \"res2\", \"res3\"] }")
                .when()
                .post(POST_STORAGE_EXP)
                .then()
                .assertThat().statusCode(404);

        async.complete();
    }

    @Test
    public void testSimpleWithResourcesAndCollections(TestContext context) {
        Async async = context.async();
        delete("/server/resources");

        with().body("{ \"foo\": \"bar1\" }").put("/server/resources/res1");
        with().body("{ \"foo\": \"bar2\" }").put("/server/resources/res2");
        with().body("{ \"foo\": \"bar3\" }").put("/server/resources/res3");
        with().body("{ \"foo\": \"sub1\" }").put("/server/resources/sub/sub1");
        with().body("{ \"foo\": \"sub2\" }").put("/server/resources/sub/sub2");

        given()
                .body("{ \"subResources\": [\"res1\", \"res2\", \"res3\", \"sub/\"] }")
                .when()
                .post(POST_STORAGE_EXP)
                .then()
                .assertThat().statusCode(200).contentType(ContentType.JSON).header(ETAG_HEADER, not(empty()))
                .body("", allOf(hasKey("res1"), hasKey("res2"), hasKey("res3"), hasKey("sub")))
                .body("res1.foo", equalTo("bar1"))
                .body("res2.foo", equalTo("bar2"))
                .body("res3.foo", equalTo("bar3"))
                .body("sub", hasItems("sub1", "sub2"));

        delete("/server/resources");

        with().body("{ \"foo\": \"sub1\" }").put("/server/resources/sub/sub1");
        with().body("{ \"foo\": \"sub2\" }").put("/server/resources/sub/sub2");
        with().body("{ \"foo\": \"sub2\" }").put("/server/resources/anothersub/sub3");
        with().body("{ \"foo\": \"sub2\" }").put("/server/resources/anothersub/sub4");
        with().body("{ \"foo\": \"sub2\" }").put("/server/resources/anothersub/sub5/subsub1");

        given()
                .body("{ \"subResources\": [\"anothersub/\", \"sub/\"] }")
                .when()
                .post(POST_STORAGE_EXP)
                .then()
                .assertThat().statusCode(200).contentType(ContentType.JSON).header(ETAG_HEADER, not(empty()))
                .body("", allOf(hasKey("anothersub"), hasKey("sub")))
                .body("sub.get(0)", equalTo("sub1"))
                .body("sub.get(1)", equalTo("sub2"))
                .body("anothersub.get(0)", equalTo("sub5/"))
                .body("anothersub.get(1)", equalTo("sub3"))
                .body("anothersub.get(2)", equalTo("sub4"));

        with().body("{ \"foo\": \"sub1\" }").put("/server/resources/level1/sub/sub1");
        with().body("{ \"foo\": \"sub2\" }").put("/server/resources/level1/sub/sub2");
        with().body("{ \"foo\": \"sub2\" }").put("/server/resources/level1/anothersub/sub3");
        with().body("{ \"foo\": \"sub2\" }").put("/server/resources/level1/anothersub/sub4");

        given()
                .body("{ \"subResources\": [\"level1/\"] }")
                .when()
                .post(POST_STORAGE_EXP)
                .then()
                .assertThat().statusCode(200).contentType(ContentType.JSON).header(ETAG_HEADER, not(empty()))
                .body("level1", hasItems("anothersub/", "sub/"));

        async.complete();
    }

    @Test
    public void testSameEtagForSameResult(TestContext context) {
        Async async = context.async();
        delete("/server/resources");

        with().body("{ \"foo\": \"bar1\" }").put("/server/resources/res1");
        with().body("{ \"foo\": \"bar2\" }").put("/server/resources/res2");
        with().body("{ \"foo\": \"bar3\" }").put("/server/resources/res3");

        Response post1 = given()
                .body("{ \"subResources\": [\"res1\", \"res2\", \"res3\"] }")
                .when()
                .post(POST_STORAGE_EXP);
        String etagPost1 = post1.getHeader(ETAG_HEADER);

        Response post2 = given()
                .body("{ \"subResources\": [\"res1\", \"res2\", \"res3\"] }")
                .when()
                .post(POST_STORAGE_EXP);
        String etagPost2 = post2.getHeader(ETAG_HEADER);

        Response post3 = given()
                .body("{ \"subResources\": [\"res1\", \"res2\"] }")
                .when()
                .post(POST_STORAGE_EXP);
        String etagPost3 = post3.getHeader(ETAG_HEADER);

        context.assertEquals(etagPost1, etagPost2);
        context.assertNotEquals(etagPost2, etagPost3);

        async.complete();
    }

    @Test
    public void testIfNoneMatchHeaderProvided(TestContext context) {
        Async async = context.async();
        delete("/server/resources");

        with().body("{ \"foo\": \"bar1\" }").put("/server/resources/res1");
        with().body("{ \"foo\": \"bar2\" }").put("/server/resources/res2");
        with().body("{ \"foo\": \"bar3\" }").put("/server/resources/res3");

        String etag = "SomeEtagValue";

        Response post1 = given().header(IF_NONE_MATCH_HEADER, etag)
                .body("{ \"subResources\": [\"res1\", \"res2\", \"res3\"] }")
                .when()
                .post(POST_STORAGE_EXP);
        String etagPost1 = post1.getHeader(ETAG_HEADER);

        context.assertNotNull(etagPost1);
        context.assertNotEquals(etagPost1, etag);

        given().header(IF_NONE_MATCH_HEADER, etagPost1)
                .body("{ \"subResources\": [\"res1\", \"res2\", \"res3\"] }")
                .when()
                .post(POST_STORAGE_EXP)
                .then()
                .assertThat().statusCode(304);

        async.complete();
    }

    @Test
    public void testWithResourcesHavingDoubleQuotesInTheName(TestContext context) {
        Async async = context.async();
        delete("/server/resources");

        with().urlEncodingEnabled(false).body("{ \"foo\": \"bar1\" }").put("/server/resources/res:1");
        with().urlEncodingEnabled(false).body("{ \"foo\": \"bar2\" }").put("/server/resources/res2");
        with().urlEncodingEnabled(false).body("{ \"foo\": \"bar3\" }").put("/server/resources/999;_hello-:@$&()*+,=-._~!'");

        given()
                .urlEncodingEnabled(false)
                .body("{ \"subResources\": [\"res:1\", \"res2\", \"999;_hello-:@$&()*+,=-._~!'\"] }")
                .when()
                .post(POST_STORAGE_EXP).prettyPrint();

        given()
                .urlEncodingEnabled(false)
                .body("{ \"subResources\": [\"res:1\", \"res2\", \"999;_hello-:@$&()*+,=-._~!'\"] }")
                .when()
                .post(POST_STORAGE_EXP)
                .then()
                .assertThat().statusCode(200).contentType(ContentType.JSON).header(ETAG_HEADER, not(empty()))
                .body("", allOf(hasKey("res:1"), hasKey("res2"), hasKey("999;_hello-:@$&()*+,=-._~!'")))
                .body("res2.foo", equalTo("bar2"));

        async.complete();
    }

    @Test
    public void testCompressedAndUncompressedDataInCollection(TestContext context) {
        Async async = context.async();
        delete("/server/resources");

        with().body("{ \"foo\": \"bar1\" }").put("/server/resources/res1");
        with().header(COMPRESS_HEADER, "true").body("{ \"foo\": \"bar2\" }").put("/server/resources/res2");
        with().body("{ \"foo\": \"bar3\" }").put("/server/resources/res3");

        given()
                .body("{ \"subResources\": [\"res1\", \"res2\", \"res3\"] }")
                .when()
                .post(POST_STORAGE_EXP)
                .then()
                .assertThat()
                .statusCode(409)
                .body(containsString("Collections having compressed resources are not supported in storage expand"));


        // make storage expand again without the compressed resource
        given()
                .body("{ \"subResources\": [\"res1\", \"res3\"] }")
                .when()
                .post(POST_STORAGE_EXP)
                .then()
                .assertThat().statusCode(200).contentType(ContentType.JSON).header(ETAG_HEADER, not(empty()))
                .body("", allOf(hasKey("res1"), hasKey("res3")))
                .body("res1.foo", equalTo("bar1"))
                .body("res3.foo", equalTo("bar3"));

        async.complete();
    }

    @Test
    public void testCompressedResourcesInSubResources(TestContext context) {
        Async async = context.async();
        delete("/server/resources");

        with().body("{ \"foo\": \"bar1\" }").put("/server/resources/res1");
        with().body("{ \"foo\": \"bar2\" }").put("/server/resources/res2");
        with().body("{ \"foo\": \"bar3\" }").put("/server/resources/res3");

        with().body("{ \"foo\": \"sub1\" }").put("/server/resources/sub/sub1");
        with().header(COMPRESS_HEADER, "true").body("{ \"foo\": \"sub2\" }").put("/server/resources/sub/sub2");

        given()
                .body("{ \"subResources\": [\"res1\", \"res2\", \"res3\", \"sub/\"] }")
                .when()
                .post(POST_STORAGE_EXP)
                .then()
                .assertThat().statusCode(200).contentType(ContentType.JSON).header(ETAG_HEADER, not(empty()))
                .body("", allOf(hasKey("res1"), hasKey("res2"), hasKey("res3"), hasKey("sub")))
                .body("res1.foo", equalTo("bar1"))
                .body("res2.foo", equalTo("bar2"))
                .body("res3.foo", equalTo("bar3"))
                .body("sub", hasItems("sub1", "sub2"));

        given()
                .body("{ \"subResources\": [\"sub1\", \"sub2\"] }")
                .when()
                .post("/server/resources/sub?storageExpand=true")
                .then()
                .assertThat()
                .statusCode(409)
                .body(containsString("Collections having compressed resources are not supported in storage expand"));

        async.complete();
    }
}
