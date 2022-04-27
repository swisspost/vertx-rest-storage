package org.swisspush.reststorage.util;

import io.vertx.core.Vertx;
import org.slf4j.Logger;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

/**
 * Tests for {@link GZIPUtil} class.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
@RunWith(VertxUnitRunner.class)
public class GZIPUtilTest {

    @Test
    public void testCompressResource(TestContext testContext) {
        Async async = testContext.async();
        String uncompressedString = "My uncompresed Resource";
        byte[] uncompressed = uncompressedString.getBytes(StandardCharsets.UTF_8);
        GZIPUtil.compressResource(Vertx.vertx(), Mockito.mock(Logger.class), uncompressed, compressResourceResult -> {
            testContext.assertTrue(compressResourceResult.succeeded());
            testContext.assertNotEquals(uncompressed, compressResourceResult.result(), "Compressed and uncompressed Resource should not be equal");

            try {
                InputStream input = new ByteArrayInputStream(compressResourceResult.result());
                GZIPInputStream gzipInputStream = new GZIPInputStream(input);

                byte[] buffer = new byte[1024];
                ByteArrayOutputStream out = new ByteArrayOutputStream();

                int len;
                while ((len = gzipInputStream.read(buffer)) > 0) {
                    out.write(buffer, 0, len);
                }

                gzipInputStream.close();
                out.close();
                byte[] decompressed = out.toByteArray();

                testContext.assertTrue(Arrays.equals(uncompressed, decompressed), "Compressed and decompressed Resource should be equal");
                testContext.assertEquals(uncompressedString, new String(decompressed, StandardCharsets.UTF_8));
            } catch (Exception e) {
                testContext.fail(e);
            }
            async.complete();
        });
    }

    @Test
    public void testDecompressResource(TestContext testContext) throws Exception {
        Async async = testContext.async();
        byte[] compressedData = IOUtils.toByteArray(this.getClass().getClassLoader().getResourceAsStream("testResource.gz"));
        GZIPUtil.decompressResource(Vertx.vertx(), Mockito.mock(Logger.class), compressedData, decompressResourceResult -> {
            testContext.assertEquals("This is an uncompressed content from a gzip file", new String(decompressResourceResult.result(), StandardCharsets.UTF_8));
            async.complete();
        });
    }
}
