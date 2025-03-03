package org.apache.rocketmq.common;

import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;
import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConfigManagerTest {
    private static final String PATH_FILE = System.getProperty("java.io.tmpdir") + File.separator + "org.apache.rocketmq.common.ConfigManagerTest";
    private static final String CONTENT_ENCODE = "Encode content for ConfigManager";

    @Test
    public void testLoad() throws Exception {
        ConfigManager testConfigManager = buildTestConfigManager();
        File file = createAndWriteFile(testConfigManager.configFilePath());
        assertTrue(testConfigManager.load());
        file.delete();
        File fileBak = createAndWriteFile(testConfigManager.configFilePath() + ".bak");
        assertTrue(testConfigManager.load());
        fileBak.delete();
    }

    @Test
    public void testLoadBak() throws Exception {
        ConfigManager testConfigManager = buildTestConfigManager();
        File file = createAndWriteFile(testConfigManager.configFilePath() + ".bak");
        // invoke private method "loadBak()"
        Method declaredMethod = ConfigManager.class.getDeclaredMethod("loadBak");
        declaredMethod.setAccessible(true);
        Boolean loadBakResult = (Boolean) declaredMethod.invoke(testConfigManager);
        assertTrue(loadBakResult);
        file.delete();

        Boolean loadBakResult2 = (Boolean) declaredMethod.invoke(testConfigManager);
        assertTrue(loadBakResult2);
        declaredMethod.setAccessible(false);
    }

    @Test
    public void testPersist() throws Exception {
        ConfigManager testConfigManager = buildTestConfigManager();
        testConfigManager.persist();
        File file = new File(testConfigManager.configFilePath());
        assertEquals(CONTENT_ENCODE, MixAll.file2String(file));
    }

    private ConfigManager buildTestConfigManager() {
        return new ConfigManager() {
            @Override
            public String encode() {
                return encode(false);
            }

            @Override
            public String configFilePath() {
                return PATH_FILE;
            }

            @Override
            public void decode(String jsonString) {

            }

            @Override
            public String encode(boolean prettyFormat) {
                return CONTENT_ENCODE;
            }
        };
    }

    private File createAndWriteFile(String fileName) throws Exception {
        File file = new File(fileName);
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        PrintWriter out = new PrintWriter(fileName);
        out.write("TestForConfigManager");
        out.close();
        return file;
    }
}