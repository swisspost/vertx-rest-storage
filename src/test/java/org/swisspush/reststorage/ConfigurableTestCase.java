package org.swisspush.reststorage;

import io.restassured.builder.RequestSpecBuilder;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import org.junit.After;

import static org.swisspush.reststorage.util.HttpRequestHeader.CONTENT_TYPE;

public abstract class ConfigurableTestCase {
    protected Vertx vertx;

    // restAssured Configuration
    protected static final int REST_STORAGE_PORT = 8989;
    protected static RequestSpecification REQUEST_SPECIFICATION = new RequestSpecBuilder()
            .addHeader(CONTENT_TYPE.getName(), "application/json")
            .setPort(8989)
            .setBasePath("/")
            .build();

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }
}
