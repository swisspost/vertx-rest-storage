package org.swisspush.reststorage;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.swisspush.reststorage.redis.RedisStorageIntegrationTestCase;
import org.swisspush.reststorage.util.LockMode;
import org.swisspush.reststorage.util.StatusCode;

import java.util.HashMap;
import java.util.Map;

import static com.jayway.awaitility.Awaitility.await;
import static io.restassured.RestAssured.*;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;


/**
 * Testclass to test the lock mechanism.
 *
 * @author https://github.com/ljucam [Mario Ljuca]
 */
@RunWith(VertxUnitRunner.class)
public class LockIntegrationTest extends RedisStorageIntegrationTestCase {
    private final String LOCK_HEADER = "x-lock";
    private final String LOCK_MODE_HEADER = "x-lock-mode";
    private final String LOCK_EXPIRE_AFTER_HEADER = "x-lock-expire-after";

    @Test
    public void testPutSilentLockDifferentOwner(TestContext context) {
        Async async = context.async();

        String path = "/lock/test/testcontent";
        String content = "{\"content\":\"unlocked\"}";
        String lockedContent = "{\"content\":\"locked\"}";
        String foreignContent = "{\"content\":\"foreign\"}";


        // perform a PUT of normal content
        given().body(content).put(path).then().assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().body("content", is("unlocked"));

        // perform a PUT with lock
        given()
                .body(lockedContent)
                .headers(createLockHeaders("test", LockMode.SILENT, 10))
                .put(path)
                .then()
                .assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().body("content", is("locked"));

        // perform PUT without ownership for the lock
        given().body(foreignContent).put(path).then().assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().body("content", is("locked"));

        // perform PUT with different ownership
        given()
                .body(foreignContent)
                .headers(createLockHeaders("test2", LockMode.SILENT, 10))
                .put(path)
                .then()
                .assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().body("content", is("locked"));

        async.complete();
    }

    @Test
    public void testPutSilentLockSameOwner(TestContext context) {
        Async async = context.async();

        String path = "/lock/test/testcontent";
        String content = "{\"content\":\"unlocked\"}";
        String lockedContent = "{\"content\":\"locked\"}";
        String newLockedContent = "{\"content\":\"new\"}";

        // perform a PUT of normal content
        given().body(content).put(path).then().assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().body("content", is("unlocked"));

        // perform the first PUT with lock
        given()
                .body(lockedContent)
                .headers(createLockHeaders("test", LockMode.SILENT, 10))
                .put(path)
                .then()
                .assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().body("content", is("locked"));

        // perform the scond PUT with same lock owner
        given()
                .body(newLockedContent)
                .headers(createLockHeaders("test", LockMode.SILENT, 10))
                .put(path)
                .then()
                .assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().body("content", is("new"));

        async.complete();
    }

    @Test
    public void testPutSilentLockExpire(TestContext context) {
        Async async = context.async();

        long expireAfter = 5;
        String path = "/lock/test/testcontent";
        String content = "{\"content\":\"unlocked\"}";
        String lockedContent = "{\"content\":\"locked\"}";

        // perform a PUT of normal content
        given().body(content).put(path).then().assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().body("content", is("unlocked"));

        // perform a PUT with lock
        given()
                .body(lockedContent)
                .headers(createLockHeaders("test", LockMode.SILENT, expireAfter))
                .put(path)
                .then()
                .assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().body("content", is("locked"));

        // perform PUT without lock (wait at most 2 x expire time)
        await().atMost(expireAfter * 2, SECONDS).until(() -> {
                given().body(content).put(path).then().assertThat().statusCode(StatusCode.OK.getStatusCode());
                return get(path).getBody().jsonPath().getString("content");
            }, is("unlocked")
        );

        async.complete();
    }

    @Test
    public void testPutRejectLockDifferentOwner(TestContext context ) {
        Async async = context.async();

        String path = "/lock/test/testcontent";
        String content = "{\"content\":\"unlocked\"}";
        String lockedContent = "{\"content\":\"locked\"}";
        String foreignContent = "{\"content\":\"foreign\"}";


        // perform a PUT of normal content
        given().body(content).put(path).then().assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().body("content", is("unlocked"));

        // perform a PUT with lock
        given()
                .body(lockedContent)
                .headers(createLockHeaders("test", LockMode.REJECT, 10))
                .put(path)
                .then()
                .assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().body("content", is("locked"));

        // perform PUT without ownership for the lock
        given().body(foreignContent).put(path).then().assertThat().statusCode(StatusCode.CONFLICT.getStatusCode());
        get(path).then().assertThat().body("content", is("locked"));

        // perform PUT with different ownership
        given()
                .body(foreignContent)
                .headers(createLockHeaders("test2", LockMode.SILENT, 10))
                .put(path)
                .then()
                .assertThat().statusCode(StatusCode.CONFLICT.getStatusCode());
        get(path).then().assertThat().body("content", is("locked"));

        async.complete();
    }

    @Test
    public void testPutRejectLockSameOwner(TestContext context) {
        Async async = context.async();

        String path = "/lock/test/testcontent";
        String content = "{\"content\":\"unlocked\"}";
        String lockedContent = "{\"content\":\"locked\"}";
        String newLockedContent = "{\"content\":\"new\"}";

        // perform a PUT of normal content
        given().body(content).put(path).then().assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().body("content", is("unlocked"));

        // perform the first PUT with lock
        given()
                .body(lockedContent)
                .headers(createLockHeaders("test", LockMode.REJECT, 10))
                .put(path)
                .then()
                .assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().body("content", is("locked"));

        // perform the scond PUT with same lock owner
        given()
                .body(newLockedContent)
                .headers(createLockHeaders("test", LockMode.REJECT, 10))
                .put(path)
                .then()
                .assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().body("content", is("new"));

        async.complete();
    }

    @Test
    public void testPutRejectLockExpire(TestContext context) {
        Async async = context.async();

        long expireAfter = 5;
        String path = "/lock/test/testcontent";
        String content = "{\"content\":\"unlocked\"}";
        String lockedContent = "{\"content\":\"locked\"}";

        // perform a PUT of normal content
        given().body(content).put(path).then().assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().body("content", is("unlocked"));

        // perform a PUT with lock
        given()
                .body(lockedContent)
                .headers(createLockHeaders("test", LockMode.REJECT, expireAfter))
                .put(path)
                .then()
                .assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().body("content", is("locked"));

        // perform PUT without lock (should return conflict)
        given().body(content).put(path).then().assertThat().statusCode(StatusCode.CONFLICT.getStatusCode());

        // perform PUT without lock (wait at most 2 x expire time)
        await().atMost(expireAfter * 2, SECONDS).until(() -> {
                    given().body(content).put(path);
                    return get(path).getBody().jsonPath().getString("content");
                }, is("unlocked")
        );

        async.complete();
    }

    @Test
    public void testDeleteSilentLockSameOwner(TestContext context) {
        Async async = context.async();

        String path = "/lock/test/testcontent";
        String lockedContent = "{\"content\":\"locked\"}";

        // perform a PUT with lock
        given()
                .body(lockedContent)
                .headers(createLockHeaders("test", LockMode.SILENT, 10))
                .put(path)
                .then()
                .assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().body("content", is("locked"));

        // perform DELETE with same lock owner
        with()
                .headers(createLockHeaders("test", LockMode.SILENT, 10))
                .delete(path)
                .then().assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().statusCode(StatusCode.NOT_FOUND.getStatusCode());

        async.complete();
    }

    @Test
    public void testDeleteSilentLockDifferentOwner(TestContext context) {
        Async async = context.async();

        String path = "/lock/test/testcontent";
        String lockedContent = "{\"content\":\"locked\"}";

        // perform a PUT without lock
        given().body(lockedContent).put(path).then().assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().body("content", is("locked"));

        // perform DELETE with lock
        with()
                .headers(createLockHeaders("test", LockMode.SILENT, 10))
                .delete(path)
                .then().assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().statusCode(StatusCode.NOT_FOUND.getStatusCode());

        // perform a PUT without lock
        given().body(lockedContent).put(path).then().assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().statusCode(StatusCode.NOT_FOUND.getStatusCode());

        async.complete();
    }

    @Test
    public void testDeleteRejectLockSameOwner(TestContext context) {
        Async async = context.async();

        String path = "/lock/test/testcontent";
        String lockedContent = "{\"content\":\"unlocked\"}";

        // perform a PUT with lock
        given()
                .body(lockedContent)
                .headers(createLockHeaders("test", LockMode.REJECT, 10))
                .put(path)
                .then()
                .assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().body("content", is("unlocked"));

        // perform DELETE with same lock owner
        with()
                .headers(createLockHeaders("test", LockMode.REJECT, 10))
                .delete(path)
                .then().assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().statusCode(StatusCode.NOT_FOUND.getStatusCode());

        async.complete();
    }

    @Test
    public void testDeleteRejectLockDifferentOwner(TestContext context) {
        Async async = context.async();

        String path = "/lock/test/testcontent";
        String lockedContent = "{\"content\":\"unlocked\"}";

        // perform a PUT without lock
        given().body(lockedContent).put(path).then().assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().body("content", is("unlocked"));

        // perform DELETE with lock
        with()
                .headers(createLockHeaders("test", LockMode.REJECT, 10))
                .delete(path)
                .then().assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().statusCode(StatusCode.NOT_FOUND.getStatusCode());

        // perform a PUT without lock
        given().body(lockedContent).put(path).then().assertThat().statusCode(StatusCode.CONFLICT.getStatusCode());
        get(path).then().assertThat().statusCode(StatusCode.NOT_FOUND.getStatusCode());

        async.complete();
    }

    @Test
    public void testDeleteRejectLockExpire(TestContext context) {
        Async async = context.async();

        long expireAfter = 5;
        String path = "/lock/test/testcontent";
        String lockedContent = "{\"content\":\"unlocked\"}";
        String newContent = "{\"content\":\"new\"}";

        // perform a PUT without lock
        given().body(lockedContent).put(path).then().assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().body("content", is("unlocked"));

        // perform DELETE with lock
        with()
                .headers(createLockHeaders("test", LockMode.REJECT, expireAfter))
                .delete(path)
                .then().assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().statusCode(StatusCode.NOT_FOUND.getStatusCode());

        // perform PUT without lock (should return conflict)
        given().body(newContent).put(path).then().assertThat().statusCode(StatusCode.CONFLICT.getStatusCode());

        // perform PUT without lock (wait at most 2 x expire time)
        await().atMost(expireAfter * 2, SECONDS).until(() -> {
                    given().body(newContent).put(path);
                    if ( given().body(newContent).put(path).getStatusCode() == StatusCode.OK.getStatusCode() ) {
                        return get(path).getBody().jsonPath().getString("content");
                    }
                    else {
                        return "notfound";
                    }
                }, is("new")
        );

        async.complete();
    }

    @Test
    public void testDeleteSilentLockExpire(TestContext context) {
        Async async = context.async();

        long expireAfter = 5;
        String path = "/lock/test/testcontent";
        String lockedContent = "{\"content\":\"unlocked\"}";
        String newContent = "{\"content\":\"new\"}";

        // perform a PUT without lock
        given().body(lockedContent).put(path).then().assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().body("content", is("unlocked"));

        // perform DELETE with lock
        with()
                .headers(createLockHeaders("test", LockMode.SILENT, expireAfter))
                .delete(path)
                .then().assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().statusCode(StatusCode.NOT_FOUND.getStatusCode());

        // perform PUT without lock (should return ok)
        given().body(newContent).put(path).then().assertThat().statusCode(StatusCode.OK.getStatusCode());
        get(path).then().assertThat().statusCode(StatusCode.NOT_FOUND.getStatusCode());

        // perform PUT without lock (wait at most 2 x expire time)
        await().atMost(expireAfter * 2, SECONDS).until(() -> {
                    given().body(newContent).put(path).then().assertThat().statusCode(StatusCode.OK.getStatusCode());

                    if ( get(path).getStatusCode() == StatusCode.OK.getStatusCode() ) {
                        return get(path).getBody().jsonPath().getString("content");
                    }
                    else {
                        return "notfound";
                    }
                }, is("new")
        );

        async.complete();
    }

    @Test
    public void testDeleteCollectionWithLockedResource(TestContext context) {
        Async async = context.async();

        String base = "/lock/test/lockcollection/";
        String path1 = base + "res1";
        String path2 = base + "res2";
        String path3 = base + "res3";

        String content1 = "{\"content\":\"res1\"}";
        String content2 = "{\"content\":\"locked\"}";
        String content3 = "{\"content\":\"res2\"}";

        // create collection
        given().body(content1).put(path1).then().assertThat().statusCode(StatusCode.OK.getStatusCode());
        given()
                .body(content2)
                .headers(createLockHeaders("test", LockMode.REJECT, 10))
                .put(path2)
                .then()
                .assertThat().statusCode(StatusCode.OK.getStatusCode());
        given().body(content3).put(path3).then().assertThat().statusCode(StatusCode.OK.getStatusCode());

        // try to delete resource (should fail)
        delete(path2).then().statusCode(StatusCode.CONFLICT.getStatusCode());

        // try to delete collection (should succeed)
        delete(base).then().assertThat().statusCode(400).body(equalTo("Bad Request: directory not empty. Use recursive=true parameter to delete"));
        get(path2).then().assertThat().statusCode(StatusCode.OK.getStatusCode());

        async.complete();
    }

    @Test
    public void testDeleteCollectionWithLockedResourceRecursive(TestContext context) {
        Async async = context.async();

        String base = "/lock/test/lockcollection/";
        String path1 = base + "res1";
        String path2 = base + "res2";
        String path3 = base + "res3";

        String content1 = "{\"content\":\"res1\"}";
        String content2 = "{\"content\":\"locked\"}";
        String content3 = "{\"content\":\"res2\"}";

        // create collection
        given().body(content1).put(path1).then().assertThat().statusCode(StatusCode.OK.getStatusCode());
        given()
                .body(content2)
                .headers(createLockHeaders("test", LockMode.REJECT, 10))
                .put(path2)
                .then()
                .assertThat().statusCode(StatusCode.OK.getStatusCode());
        given().body(content3).put(path3).then().assertThat().statusCode(StatusCode.OK.getStatusCode());

        // try to delete resource (should fail)
        delete(path2).then().statusCode(StatusCode.CONFLICT.getStatusCode());

        // try to delete collection (should succeed)
        with().param("recursive", "true").delete(base).then().statusCode(StatusCode.OK.getStatusCode());
        get(path2).then().assertThat().statusCode(StatusCode.NOT_FOUND.getStatusCode());

        async.complete();
    }

    private Map<String, Object> createLockHeaders(String owner, LockMode mode, long expire) {
        Map<String, Object> lockHeaders = new HashMap<>();
        lockHeaders.put(LOCK_HEADER, owner);
        lockHeaders.put(LOCK_MODE_HEADER, mode.text());
        lockHeaders.put(LOCK_EXPIRE_AFTER_HEADER, expire);
        return lockHeaders;
    }
}
