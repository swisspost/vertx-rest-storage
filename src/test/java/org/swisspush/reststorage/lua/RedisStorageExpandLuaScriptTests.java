/*
 * ------------------------------------------------------------------------------------------------
 * Copyright 2014 by Swiss Post, Information Technology Services
 * ------------------------------------------------------------------------------------------------
 * $Id$
 * ------------------------------------------------------------------------------------------------
 */

package org.swisspush.reststorage.lua;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import io.vertx.core.json.JsonArray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.hamcrest.MatcherAssert.assertThat;

public class RedisStorageExpandLuaScriptTests extends AbstractLuaScriptTest {

    @Test
    public void testStorageExpand() {

        // ARRANGE
        evalScriptPut(":project:server:test:item1", "{\"content\": \"content_1\"}");
        evalScriptPut(":project:server:test:item2", "{\"content\": \"content_2\"}");
        evalScriptPut(":project:server:test:item3", "{\"content\": \"content_3\"}");

        // ACT
        List<String> subResources = Arrays.asList("item2", "item1", "item3");
        List<List<String>> value = evalScriptStorageExpandAndExtract(":project:server:test", subResources);

        // ASSERT
        assertThat(value.size(), equalTo(3));
        assertThat(value.get(0).get(0), equalTo("item2"));
        assertThat(value.get(0).get(1), equalTo("{\"content\": \"content_2\"}"));
        assertThat(value.get(1).get(0), equalTo("item1"));
        assertThat(value.get(1).get(1), equalTo("{\"content\": \"content_1\"}"));
        assertThat(value.get(2).get(0), equalTo("item3"));
        assertThat(value.get(2).get(1), equalTo("{\"content\": \"content_3\"}"));
    }

    @Test
    public void testStorageExpandEmptySubresources() {

        // ARRANGE
        evalScriptPut(":project:server:test:item1", "{\"content\": \"content_1\"}");
        evalScriptPut(":project:server:test:item2", "{\"content\": \"content_2\"}");
        evalScriptPut(":project:server:test:item3", "{\"content\": \"content_3\"}");

        // ACT
        List<List<String>> value = evalScriptStorageExpandAndExtract(":project:server:test", new ArrayList<>());

        // ASSERT
        assertNotNull(value);
        assertThat(value.size(), equalTo(0));
    }

    /**
     * StorageExpand does not care about correct (json) content.
     */
    @Test
    public void testStorageExpandInvalidSubresources() {

        // ARRANGE
        evalScriptPut(":project:server:test:item1", "{\"content\": \"content_1\"}");
        evalScriptPut(":project:server:test:item2", "{\"content\": \"content_2\"}");
        evalScriptPut(":project:server:test:item3", "{\"content\"");

        // ACT
        List<String> subResources = Arrays.asList("item1", "item2", "item3");
        List<List<String>> value = evalScriptStorageExpandAndExtract(":project:server:test", subResources);

        // ASSERT
        assertThat(value.size(), equalTo(3));
        assertThat(value.get(2).get(0), equalTo("item3"));
        assertThat(value.get(2).get(1), equalTo("{\"content\""));
    }

    @Test
    public void testStorageExpandWithSubCollections() {

        // ARRANGE
        evalScriptPut(":project:server:test:item1", "{\"content\": \"content_1\"}");
        evalScriptPut(":project:server:test:item2", "{\"content\": \"content_2\"}");
        evalScriptPut(":project:server:test:item3", "{\"content\": \"content_3\"}");
        evalScriptPut(":project:server:test:sub:sub1", "{\"content\": \"content_sub_1\"}");
        evalScriptPut(":project:server:test:sub:sub2", "{\"content\": \"content_sub_2\"}");

        // ACT
        List<String> subResources = Arrays.asList("sub/", "item1", "item2", "item3");
        List<List<String>> value = evalScriptStorageExpandAndExtract(":project:server:test", subResources);

        // ASSERT
        assertThat(value.size(), equalTo(4));
        assertThat(value.get(0).get(0), equalTo("sub"));
        assertThat(value.get(0).get(1), equalTo("[\"sub1\",\"sub2\"]"));
        assertThat(value.get(1).get(0), equalTo("item1"));
        assertThat(value.get(1).get(1), equalTo("{\"content\": \"content_1\"}"));
        assertThat(value.get(2).get(0), equalTo("item2"));
        assertThat(value.get(2).get(1), equalTo("{\"content\": \"content_2\"}"));
        assertThat(value.get(3).get(0), equalTo("item3"));
        assertThat(value.get(3).get(1), equalTo("{\"content\": \"content_3\"}"));
    }

    @Test
    public void testStorageExpandWithSubSubCollections() {

        // ARRANGE
        evalScriptPut(":project:server:test:sub:subsub:item1", "{\"content\": \"content_sub_1\"}");
        evalScriptPut(":project:server:test:sub:subsub:item2", "{\"content\": \"content_sub_2\"}");
        evalScriptPut(":project:server:test:sub:othersubsub:item3", "{\"content\": \"content_sub_3\"}");
        evalScriptPut(":project:server:test:sub:othersubsub:item4", "{\"content\": \"content_sub_4\"}");

        // ACT
        List<String> subResources = Collections.singletonList("sub/");
        List<List<String>> value = evalScriptStorageExpandAndExtract(":project:server:test", subResources);

        // ASSERT
        assertThat(value.size(), equalTo(1));
        assertThat(value.get(0).get(0), equalTo("sub"));
        assertThat(value.get(0).get(1), equalTo("[\"othersubsub\\/\",\"subsub\\/\"]"));

        // ACT
        subResources = Collections.singletonList("subsub/");
        value = evalScriptStorageExpandAndExtract(":project:server:test:sub", subResources);

        // ASSERT
        assertThat(value.size(), equalTo(1));
        assertThat(value.get(0).get(0), equalTo("subsub"));
        assertThat(value.get(0).get(1), equalTo("[\"item1\",\"item2\"]"));

        // ACT
        subResources = Arrays.asList("item1", "item2");
        value = evalScriptStorageExpandAndExtract(":project:server:test:sub:subsub", subResources);

        // ASSERT
        assertThat(value.size(), equalTo(2));
        assertThat(value.get(0).get(0), equalTo("item1"));
        assertThat(value.get(0).get(1), equalTo("{\"content\": \"content_sub_1\"}"));
        assertThat(value.get(1).get(0), equalTo("item2"));
        assertThat(value.get(1).get(1), equalTo("{\"content\": \"content_sub_2\"}"));
    }

    @Test
    public void testStorageExpandWithCollectionsOnly() {

        // ARRANGE
        evalScriptPut(":project:server:test:sub:sub1", "{\"content\": \"content_sub_1\"}");
        evalScriptPut(":project:server:test:sub:sub2", "{\"content\": \"content_sub_2\"}");
        evalScriptPut(":project:server:test:anothersub:sub3", "{\"content\": \"content_sub_3\"}");
        evalScriptPut(":project:server:test:anothersub:sub4", "{\"content\": \"content_sub_4\"}");

        // ACT
        List<String> subResources = Arrays.asList("sub/", "anothersub/");
        List<List<String>> value = evalScriptStorageExpandAndExtract(":project:server:test", subResources);

        // ASSERT
        assertThat(value.size(), equalTo(2));
        assertThat(value.get(0).get(0), equalTo("sub"));
        assertThat(value.get(0).get(1), equalTo("[\"sub1\",\"sub2\"]"));
        assertThat(value.get(1).get(0), equalTo("anothersub"));
        assertThat(value.get(1).get(1), equalTo("[\"sub3\",\"sub4\"]"));
    }

    @Test
    public void testStorageExpandWithNoMatchingResources() {

        // ARRANGE
        evalScriptPut(":project:server:test:item1", "{\"content\": \"content_1\"}");
        evalScriptPut(":project:server:test:item2", "{\"content\": \"content_2\"}");
        evalScriptPut(":project:server:test:item3", "{\"content\": \"content_3\"}");

        // ACT
        List<String> subResources = Arrays.asList("item2x", "item1x", "item3x");
        List<List<String>> value = evalScriptStorageExpandAndExtract(":project:server:test", subResources);

        // ASSERT
        assertNotNull(value);
        assertThat(value.size(), equalTo(0));
    }

    @Test
    public void testStorageExpandWithInvalidSubresources() {

        // ARRANGE
        evalScriptPut(":project:server:test:item1", "{\"content\": \"content_1\"}");
        evalScriptPut(":project:server:test:item2", "{\"content\": \"content_2\"}");
        evalScriptPut(":project:server:test:item3", "{\"content\": \"content_3\"}");

        // ACT
        List<String> subResources = Arrays.asList("item3", "invalidItem", "item1");
        List<List<String>> value = evalScriptStorageExpandAndExtract(":project:server:test", subResources);

        // ASSERT
        assertThat(value.size(), equalTo(2));
        assertThat(value.get(0).get(0), equalTo("item3"));
        assertThat(value.get(0).get(1), equalTo("{\"content\": \"content_3\"}"));
        assertThat(value.get(1).get(0), equalTo("item1"));
        assertThat(value.get(1).get(1), equalTo("{\"content\": \"content_1\"}"));

        // ACT
        subResources = Arrays.asList("item3", "invalidSub/");
        value = evalScriptStorageExpandAndExtract(":project:server:test", subResources);

        // ASSERT
        assertThat(value.size(), equalTo(1));
        assertThat(value.get(0).get(0), equalTo("item3"));
        assertThat(value.get(0).get(1), equalTo("{\"content\": \"content_3\"}"));
    }

    @Test
    public void testStorageExpandExpired() throws InterruptedException {

        // ARRANGE
        String now = String.valueOf(System.currentTimeMillis());
        evalScriptPut(":project:server:test:item1", "{\"content\": \"content_1\"}", now);

        String nowPlus100 = String.valueOf(System.currentTimeMillis() + 100);
        evalScriptPut(":project:server:test:item2", "{\"content\": \"content_2\"}", nowPlus100);

        Thread.sleep(15);

        // ACT
        String timestamp = String.valueOf(System.currentTimeMillis());
        List<List<String>> value = evalScriptStorageExpandAndExtract(":project:server:test", Arrays.asList("item1", "item2"), timestamp);

        // ASSERT
        assertThat(value.size(), equalTo(1));
        assertThat(value.get(0).get(0), equalTo("item2"));
        assertThat(value.get(0).get(1), equalTo("{\"content\": \"content_2\"}"));

        Thread.sleep(100);

        // ACT
        timestamp = String.valueOf(System.currentTimeMillis());
        value = evalScriptStorageExpandAndExtract(":project:server:test", Arrays.asList("item1", "item2"), timestamp);

        // ASSERT
        assertNotNull(value);
        assertThat(value.size(), equalTo(0));
    }

    @Test
    public void testStorageExpandExpiredWithCollections() throws InterruptedException {

        // ARRANGE
        String now = String.valueOf(System.currentTimeMillis());
        String nowPlus1000 = String.valueOf(System.currentTimeMillis() + 1000);

        evalScriptPut(":project:server:test:item1", "{\"content\": \"content_1\"}", now);
        evalScriptPut(":project:server:test:item2", "{\"content\": \"content_2\"}", now);
        evalScriptPut(":project:server:test:item3", "{\"content\": \"content_3\"}", nowPlus1000);
        evalScriptPut(":project:server:test:sub:sub1", "{\"content\": \"content_sub_1\"}", now);
        evalScriptPut(":project:server:test:sub:sub2", "{\"content\": \"content_sub_2\"}", nowPlus1000);

        Thread.sleep(15);

        // ACT
        String timestamp = String.valueOf(System.currentTimeMillis());
        List<List<String>> value = evalScriptStorageExpandAndExtract(":project:server:test", Arrays.asList("sub/", "item1", "item2", "item3"), timestamp);

        // ASSERT
        assertThat(value.size(), equalTo(2));
        assertThat(value.get(0).get(0), equalTo("sub"));
        assertThat(value.get(0).get(1), equalTo("[\"sub2\"]"));
        assertThat(value.get(1).get(0), equalTo("item3"));
        assertThat(value.get(1).get(1), equalTo("{\"content\": \"content_3\"}"));
    }

    @Test
    public void testStorageExpandCompressedDataInCollection() {

        // ARRANGE
        evalScriptPut(":project:server:test:item1", "{\"content\": \"content_1\"}", AbstractLuaScriptTest.MAX_EXPIRE, "etag1", true);

        // ACT
        List<String> subResources = Collections.singletonList("item1");
        String value = (String) evalScriptStorageExpand(":project:server:test", subResources);

        // ASSERT
        assertThat(value, equalTo("compressionNotSupported"));
    }

    @Test
    public void testStorageExpandCompressedAndUncompressedDataInCollection() {

        // ARRANGE
        evalScriptPut(":project:server:test:item1", "{\"content\": \"content_1\"}", AbstractLuaScriptTest.MAX_EXPIRE, "etag1", false);
        evalScriptPut(":project:server:test:item2", "{\"content\": \"content_2\"}", AbstractLuaScriptTest.MAX_EXPIRE, "etag2", true);
        evalScriptPut(":project:server:test:item3", "{\"content\": \"content_3\"}", AbstractLuaScriptTest.MAX_EXPIRE, "etag3", false);

        // ACT
        List<String> subResources = Arrays.asList("item2", "item1", "item3");
        String value = (String) evalScriptStorageExpand(":project:server:test", subResources);

        // ASSERT
        assertThat(value, equalTo("compressionNotSupported"));

        // ACT
        List<String> subResources2 = Arrays.asList("item1", "item3");
        List<List<String>> value2 = evalScriptStorageExpandAndExtract(":project:server:test", subResources2);

        // ASSERT
        assertThat(value2.size(), equalTo(2));
        assertThat(value2.get(0).get(0), equalTo("item1"));
        assertThat(value2.get(0).get(1), equalTo("{\"content\": \"content_1\"}"));
        assertThat(value2.get(1).get(0), equalTo("item3"));
        assertThat(value2.get(1).get(1), equalTo("{\"content\": \"content_3\"}"));
    }

    private Object evalScriptStorageExpand(final String resourceName1, final List<String> subResources) {
        return evalScriptStorageExpand(resourceName1, subResources, String.valueOf(System.currentTimeMillis()));
    }

    @SuppressWarnings({"rawtypes", "unchecked", "serial"})
    private Object evalScriptStorageExpand(final String resourceName1, final List<String> subResources, final String timestamp) {
        String getScript = readScript("storageExpand.lua");
        return jedis.eval(getScript, new ArrayList() {
                    {
                        add(resourceName1);
                    }
                }, new ArrayList() {
                    {
                        add(prefixResources);
                        add(prefixCollections);
                        add(expirableSet);
                        add(timestamp);
                        add("99999999999999");
                        add(StringUtils.join(subResources, ";"));
                        add(String.valueOf(subResources.size()));
                    }
                }
        );
    }

    private List<List<String>> evalScriptStorageExpandAndExtract(String resourceName, List<String> subResources){
        return evalScriptStorageExpandAndExtract(resourceName, subResources, null);
    }

    private List<List<String>> evalScriptStorageExpandAndExtract(String resourceName, List<String> subResources, String timestamp){
        List<List<String>> result = new ArrayList<>();
        String valueStr;
        if(timestamp != null){
            valueStr = (String) evalScriptStorageExpand(resourceName, subResources, timestamp);
        } else {
            valueStr = (String) evalScriptStorageExpand(resourceName, subResources);
        }

        if(valueStr.equals("notFound")){
            return result;
        }

        JsonArray jsonArray = new JsonArray(valueStr);
        for (Object arr : jsonArray) {
            JsonArray subArr = (JsonArray) arr;
            result.add(Arrays.asList(subArr.getString(0), subArr.getString(1)));
        }
        return result;
    }
}
