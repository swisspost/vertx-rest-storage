package org.swisspush.reststorage.util;

import io.vertx.core.MultiMap;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.swisspush.reststorage.util.HttpRequestParam.*;

/**
 * <p>
 * Tests for the {@link HttpRequestParam} class
 * </p>
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
@RunWith(VertxUnitRunner.class)
public class HttpRequestParamTest {

    MultiMap params;

    @Before
    public void setUp() {
        params = new HeadersMultiMap();
    }

    @Test
    public void testContainsHeader(TestContext context){
        context.assertFalse(HttpRequestParam.containsParam(null, STORAGE_EXPAND_PARAMETER));
        context.assertFalse(HttpRequestParam.containsParam(params, STORAGE_EXPAND_PARAMETER));

        params.set("storageExpand", "true");
        context.assertTrue(HttpRequestParam.containsParam(params, STORAGE_EXPAND_PARAMETER));

        params.clear();
        params.set("storageexpand", "true");
        context.assertTrue(HttpRequestParam.containsParam(params, STORAGE_EXPAND_PARAMETER));

        params.clear();
        params.set("STORAGEEXPAND", "true");
        context.assertTrue(HttpRequestParam.containsParam(params, STORAGE_EXPAND_PARAMETER));

        params.clear();
        params.set("xstorageExpand", "true");
        context.assertFalse(HttpRequestParam.containsParam(params, STORAGE_EXPAND_PARAMETER));
    }

    @Test
    public void testGetString(TestContext context){
        params.set(LIMIT_PARAMETER.getName(), "99");
        context.assertEquals("99", getString(params, LIMIT_PARAMETER));

        params.set(LIMIT_PARAMETER.getName(), "444");
        context.assertEquals("444", getString(params, LIMIT_PARAMETER));

        params.set(LIMIT_PARAMETER.getName(), "0");
        context.assertEquals("0", getString(params, LIMIT_PARAMETER));

        params.set(LIMIT_PARAMETER.getName(), "9999999999999999999");
        context.assertEquals("9999999999999999999", getString(params, LIMIT_PARAMETER));

        params.set(LIMIT_PARAMETER.getName(), "");
        context.assertEquals("", getString(params, LIMIT_PARAMETER));

        params.set(LIMIT_PARAMETER.getName(), "xyz");
        context.assertEquals("xyz", getString(params, LIMIT_PARAMETER));

        params.clear();
        context.assertNull(getString(params, LIMIT_PARAMETER));

        context.assertNull(getString(null, LIMIT_PARAMETER));
    }

    @Test
    public void testGetBoolean(TestContext context){
        params.set(STORAGE_EXPAND_PARAMETER.getName(), "true");
        context.assertTrue(getBoolean(params, STORAGE_EXPAND_PARAMETER));

        params.set(STORAGE_EXPAND_PARAMETER.getName(), "TRUE");
        context.assertTrue(getBoolean(params, STORAGE_EXPAND_PARAMETER));

        params.set(STORAGE_EXPAND_PARAMETER.getName(), "trUe");
        context.assertTrue(getBoolean(params, STORAGE_EXPAND_PARAMETER));

        params.set(STORAGE_EXPAND_PARAMETER.getName(), "false");
        context.assertFalse(getBoolean(params, STORAGE_EXPAND_PARAMETER));

        params.set(STORAGE_EXPAND_PARAMETER.getName(), "yes");
        context.assertFalse(getBoolean(params, STORAGE_EXPAND_PARAMETER));

        params.set(STORAGE_EXPAND_PARAMETER.getName(), "124");
        context.assertFalse(getBoolean(params, STORAGE_EXPAND_PARAMETER));

        params.set(STORAGE_EXPAND_PARAMETER.getName(), "foo");
        context.assertFalse(getBoolean(params, STORAGE_EXPAND_PARAMETER));

        params.set(STORAGE_EXPAND_PARAMETER.getName(), "1");
        context.assertFalse(getBoolean(params, STORAGE_EXPAND_PARAMETER));

        params.set(STORAGE_EXPAND_PARAMETER.getName(), "0");
        context.assertFalse(getBoolean(params, STORAGE_EXPAND_PARAMETER));

        params.set(STORAGE_EXPAND_PARAMETER.getName(), "");
        context.assertFalse(getBoolean(params, STORAGE_EXPAND_PARAMETER));

        params.clear();
        context.assertFalse(getBoolean(params, STORAGE_EXPAND_PARAMETER));

        context.assertFalse(getBoolean(null, STORAGE_EXPAND_PARAMETER));
    }
}
