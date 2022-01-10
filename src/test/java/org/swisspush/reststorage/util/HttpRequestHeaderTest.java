package org.swisspush.reststorage.util;

import io.vertx.core.MultiMap;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.swisspush.reststorage.util.HttpRequestHeader.*;

/**
 * <p>
 * Tests for the {@link HttpRequestHeader} class
 * </p>
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
@RunWith(VertxUnitRunner.class)
public class HttpRequestHeaderTest {

    MultiMap headers;

    @Before
    public void setUp() {
        headers = new HeadersMultiMap();
    }

    @Test
    public void testContainsHeader(TestContext context){
        context.assertFalse(containsHeader(null, LOCK_HEADER));
        context.assertFalse(containsHeader(headers, ETAG_HEADER));

        headers.set("x-expire-after", "99");
        context.assertTrue(containsHeader(headers, EXPIRE_AFTER_HEADER));

        headers.clear();
        headers.set("X-EXPIRE-AFTER", "99");
        context.assertTrue(containsHeader(headers, EXPIRE_AFTER_HEADER));

        headers.clear();
        headers.set("x-Expire-After", "99");
        context.assertTrue(containsHeader(headers, EXPIRE_AFTER_HEADER));

        headers.clear();
        headers.set("xexpireafter", "99");
        context.assertFalse(containsHeader(headers, EXPIRE_AFTER_HEADER));
    }

    @Test
    public void testGetInteger(TestContext context){
        headers.set(LOCK_EXPIRE_AFTER_HEADER.getName(), "99");
        context.assertEquals(99, getInteger(headers, LOCK_EXPIRE_AFTER_HEADER));

        headers.set(LOCK_EXPIRE_AFTER_HEADER.getName(), "444");
        context.assertEquals(444, getInteger(headers, LOCK_EXPIRE_AFTER_HEADER));

        headers.set(LOCK_EXPIRE_AFTER_HEADER.getName(), "0");
        context.assertEquals(0, getInteger(headers, LOCK_EXPIRE_AFTER_HEADER));

        headers.set(LOCK_EXPIRE_AFTER_HEADER.getName(), "9999999999999999999");
        context.assertNull(getInteger(headers, LOCK_EXPIRE_AFTER_HEADER));

        headers.set(LOCK_EXPIRE_AFTER_HEADER.getName(), "");
        context.assertNull(getInteger(headers, LOCK_EXPIRE_AFTER_HEADER));

        headers.set(LOCK_EXPIRE_AFTER_HEADER.getName(), "xyz");
        context.assertNull(getInteger(headers, LOCK_EXPIRE_AFTER_HEADER));

        headers.clear();
        context.assertNull(getInteger(headers, LOCK_EXPIRE_AFTER_HEADER));

        context.assertNull(getInteger(null, LOCK_EXPIRE_AFTER_HEADER));
    }

    @Test
    public void testGetIntegerWithDefaultValue(TestContext context){

        Integer defaultValue = 1;

        headers.set(LOCK_EXPIRE_AFTER_HEADER.getName(), "99");
        context.assertEquals(99, getInteger(headers, LOCK_EXPIRE_AFTER_HEADER, defaultValue));

        headers.set(LOCK_EXPIRE_AFTER_HEADER.getName(), "444");
        context.assertEquals(444, getInteger(headers, LOCK_EXPIRE_AFTER_HEADER, defaultValue));

        headers.set(LOCK_EXPIRE_AFTER_HEADER.getName(), "0");
        context.assertEquals(0, getInteger(headers, LOCK_EXPIRE_AFTER_HEADER, defaultValue));

        headers.set(LOCK_EXPIRE_AFTER_HEADER.getName(), "9999999999999999999");
        context.assertEquals(1, getInteger(headers, LOCK_EXPIRE_AFTER_HEADER, defaultValue));

        headers.set(LOCK_EXPIRE_AFTER_HEADER.getName(), "");
        context.assertEquals(1, getInteger(headers, LOCK_EXPIRE_AFTER_HEADER, defaultValue));

        headers.set(LOCK_EXPIRE_AFTER_HEADER.getName(), "xyz");
        context.assertEquals(1, getInteger(headers, LOCK_EXPIRE_AFTER_HEADER, defaultValue));

        headers.clear();
        context.assertEquals(1, getInteger(headers, LOCK_EXPIRE_AFTER_HEADER, defaultValue));

        context.assertEquals(1, getInteger(null, LOCK_EXPIRE_AFTER_HEADER, defaultValue));
    }


    @Test
    public void testGetLong(TestContext context){
        headers.set(LOCK_EXPIRE_AFTER_HEADER.getName(), "99");
        context.assertEquals(99L, getLong(headers, LOCK_EXPIRE_AFTER_HEADER));

        headers.set(LOCK_EXPIRE_AFTER_HEADER.getName(), "444");
        context.assertEquals(444L, getLong(headers, LOCK_EXPIRE_AFTER_HEADER));

        headers.set(LOCK_EXPIRE_AFTER_HEADER.getName(), "0");
        context.assertEquals(0L, getLong(headers, LOCK_EXPIRE_AFTER_HEADER));

        headers.set(LOCK_EXPIRE_AFTER_HEADER.getName(), "9999999999999999999");
        context.assertNull(getLong(headers, LOCK_EXPIRE_AFTER_HEADER));

        headers.set(LOCK_EXPIRE_AFTER_HEADER.getName(), "");
        context.assertNull(getLong(headers, LOCK_EXPIRE_AFTER_HEADER));

        headers.set(LOCK_EXPIRE_AFTER_HEADER.getName(), "xyz");
        context.assertNull(getLong(headers, LOCK_EXPIRE_AFTER_HEADER));

        headers.clear();
        context.assertNull(getLong(headers, LOCK_EXPIRE_AFTER_HEADER));

        context.assertNull(getLong(null, LOCK_EXPIRE_AFTER_HEADER));
    }

    @Test
    public void testGetLongWithDefaultValue(TestContext context){

        Long defaultValue = 1L;

        headers.set(LOCK_EXPIRE_AFTER_HEADER.getName(), "99");
        context.assertEquals(99L, getLong(headers, LOCK_EXPIRE_AFTER_HEADER, defaultValue));

        headers.set(LOCK_EXPIRE_AFTER_HEADER.getName(), "444");
        context.assertEquals(444L, getLong(headers, LOCK_EXPIRE_AFTER_HEADER, defaultValue));

        headers.set(LOCK_EXPIRE_AFTER_HEADER.getName(), "0");
        context.assertEquals(0L, getLong(headers, LOCK_EXPIRE_AFTER_HEADER, defaultValue));

        headers.set(LOCK_EXPIRE_AFTER_HEADER.getName(), "9999999999999999999");
        context.assertEquals(1L, getLong(headers, LOCK_EXPIRE_AFTER_HEADER, defaultValue));

        headers.set(LOCK_EXPIRE_AFTER_HEADER.getName(), "");
        context.assertEquals(1L, getLong(headers, LOCK_EXPIRE_AFTER_HEADER, defaultValue));

        headers.set(LOCK_EXPIRE_AFTER_HEADER.getName(), "xyz");
        context.assertEquals(1L, getLong(headers, LOCK_EXPIRE_AFTER_HEADER, defaultValue));

        headers.clear();
        context.assertEquals(1L, getLong(headers, LOCK_EXPIRE_AFTER_HEADER, defaultValue));

        context.assertEquals(1L, getLong(null, LOCK_EXPIRE_AFTER_HEADER, defaultValue));
    }

    @Test
    public void testGetString(TestContext context){
        headers.set(ETAG_HEADER.getName(), "99");
        context.assertEquals("99", getString(headers, ETAG_HEADER));

        headers.set(ETAG_HEADER.getName(), "444");
        context.assertEquals("444", getString(headers, ETAG_HEADER));

        headers.set(ETAG_HEADER.getName(), "0");
        context.assertEquals("0", getString(headers, ETAG_HEADER));

        headers.set(ETAG_HEADER.getName(), "9999999999999999999");
        context.assertEquals("9999999999999999999", getString(headers, ETAG_HEADER));

        headers.set(ETAG_HEADER.getName(), "");
        context.assertEquals("", getString(headers, ETAG_HEADER));

        headers.set(ETAG_HEADER.getName(), "xyz");
        context.assertEquals("xyz", getString(headers, ETAG_HEADER));

        headers.clear();
        context.assertNull(getString(headers, ETAG_HEADER));

        context.assertNull(getString(null, ETAG_HEADER));
    }
}
