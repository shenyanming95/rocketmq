package org.apache.rocketmq.common;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.io.IOException;

/**
 * 读取配置文件的抽象父类
 */
public abstract class ConfigManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    /**
     * 加载配置文件
     *
     * @return true-加载成功
     */
    public boolean load() {
        String fileName = null;
        try {
            // 获取文件路径
            fileName = this.configFilePath();
            // 读取文件内容, 默认都是json字符串
            String jsonString = MixAll.file2String(fileName);

            if (null == jsonString || jsonString.length() == 0) {
                // 如果数据读取不出来, 那么加载bak文件(备份文件)
                return this.loadBak();
            } else {
                // 直接将读取出来的数据编码解析
                this.decode(jsonString);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " failed, and try to load backup file", e);
            return this.loadBak();
        }
    }

    /**
     * 加载备份的配置文件
     *
     * @return true-加载成功
     */
    private boolean loadBak() {
        String fileName = null;
        try {
            // 获取配置文件的路径, 拼接成bak格式, 加载它的内容
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName + ".bak");
            // 加载成功
            if (jsonString != null && jsonString.length() > 0) {
                // 执行编码
                this.decode(jsonString);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " Failed", e);
            return false;
        }
        return true;
    }

    /**
     * 将数据写入到磁盘中
     */
    public synchronized void persist() {
        String jsonString = this.encode(true);
        if (jsonString != null) {
            String fileName = this.configFilePath();
            try {
                MixAll.string2File(jsonString, fileName);
            } catch (IOException e) {
                log.error("persist file " + fileName + " exception", e);
            }
        }
    }

    /**
     * 将配置信息编码成json字符串
     *
     * @return 编码后的数据串
     */
    public abstract String encode();

    /**
     * 指定配置文件的路径
     */
    public abstract String configFilePath();

    /**
     * 把冲配置文件解析出来的json字符串编码
     *
     * @param jsonString 从磁盘文件读取出来的数据内容
     */
    public abstract void decode(final String jsonString);

    /**
     * 将配置信息编码成json字符串
     *
     * @param prettyFormat 是否需要格式化
     * @return 编码后的数据串
     */
    public abstract String encode(final boolean prettyFormat);
}
