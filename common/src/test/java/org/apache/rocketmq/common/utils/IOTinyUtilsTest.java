package org.apache.rocketmq.common.utils;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.CharArrayWriter;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.lang.reflect.Method;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IOTinyUtilsTest {

    private String testRootDir = System.getProperty("user.home") + File.separator + "iotinyutilstest";

    @Before
    public void init() {
        File dir = new File(testRootDir);
        if (dir.exists()) {
            UtilAll.deleteFile(dir);
        }

        dir.mkdirs();
    }

    @After
    public void destory() {
        File file = new File(testRootDir);
        UtilAll.deleteFile(file);
    }


    @Test
    public void testToString() throws Exception {
        byte[] b = "testToString".getBytes(RemotingHelper.DEFAULT_CHARSET);
        InputStream is = new ByteArrayInputStream(b);

        String str = IOTinyUtils.toString(is, null);
        assertEquals("testToString", str);

        is = new ByteArrayInputStream(b);
        str = IOTinyUtils.toString(is, RemotingHelper.DEFAULT_CHARSET);
        assertEquals("testToString", str);

        is = new ByteArrayInputStream(b);
        Reader isr = new InputStreamReader(is, RemotingHelper.DEFAULT_CHARSET);
        str = IOTinyUtils.toString(isr);
        assertEquals("testToString", str);
    }


    @Test
    public void testCopy() throws Exception {
        char[] arr = "testToString".toCharArray();
        Reader reader = new CharArrayReader(arr);
        Writer writer = new CharArrayWriter();

        long count = IOTinyUtils.copy(reader, writer);
        assertEquals(arr.length, count);
    }

    @Test
    public void testReadLines() throws Exception {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < 10; i++) {
            sb.append("testReadLines").append("\n");
        }

        StringReader reader = new StringReader(sb.toString());
        List<String> lines = IOTinyUtils.readLines(reader);

        assertEquals(10, lines.size());
    }

    @Test
    public void testToBufferedReader() throws Exception {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < 10; i++) {
            sb.append("testToBufferedReader").append("\n");
        }

        StringReader reader = new StringReader(sb.toString());
        Method method = IOTinyUtils.class.getDeclaredMethod("toBufferedReader", new Class[]{Reader.class});
        method.setAccessible(true);
        Object bReader = method.invoke(IOTinyUtils.class, reader);

        assertTrue(bReader instanceof BufferedReader);
    }

    @Test
    public void testWriteStringToFile() throws Exception {
        File file = new File(testRootDir, "testWriteStringToFile");
        assertTrue(!file.exists());

        IOTinyUtils.writeStringToFile(file, "testWriteStringToFile", RemotingHelper.DEFAULT_CHARSET);

        assertTrue(file.exists());
    }

    @Test
    public void testCleanDirectory() throws Exception {
        for (int i = 0; i < 10; i++) {
            IOTinyUtils.writeStringToFile(new File(testRootDir, "testCleanDirectory" + i), "testCleanDirectory", RemotingHelper.DEFAULT_CHARSET);
        }

        File dir = new File(testRootDir);
        assertTrue(dir.exists() && dir.isDirectory());
        assertTrue(dir.listFiles().length > 0);

        IOTinyUtils.cleanDirectory(new File(testRootDir));

        assertTrue(dir.listFiles().length == 0);
    }

    @Test
    public void testDelete() throws Exception {
        for (int i = 0; i < 10; i++) {
            IOTinyUtils.writeStringToFile(new File(testRootDir, "testDelete" + i), "testCleanDirectory", RemotingHelper.DEFAULT_CHARSET);
        }

        File dir = new File(testRootDir);
        assertTrue(dir.exists() && dir.isDirectory());
        assertTrue(dir.listFiles().length > 0);

        IOTinyUtils.delete(new File(testRootDir));

        assertTrue(!dir.exists());
    }

    @Test
    public void testCopyFile() throws Exception {
        File source = new File(testRootDir, "soruce");
        String target = testRootDir + File.separator + "dest";

        IOTinyUtils.writeStringToFile(source, "testCopyFile", RemotingHelper.DEFAULT_CHARSET);

        IOTinyUtils.copyFile(source.getCanonicalPath(), target);

        File dest = new File(target);
        assertTrue(dest.exists());
    }
}
