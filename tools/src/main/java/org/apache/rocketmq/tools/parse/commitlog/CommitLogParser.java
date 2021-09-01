package org.apache.rocketmq.tools.parse.commitlog;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author shenyanming
 * Create on 2021/08/03 20:16
 */
public class CommitLogParser {

    public static void main(String[] args) {
        // test
        String path = System.getProperty("user.dir") + File.separator + "workdir" + File.separator + "store"
                + File.separator + "commitlog" + File.separator + "00000000000000000000";
        List<CommitLogMessage> list = parse(path);
        list.forEach(System.out::println);
    }

    private final static int BORNHOST_V6_FLAG = 0x1 << 4; // 16
    private final static int STOREHOSTADDRESS_V6_FLAG = 0x1 << 5; // 32
    private final static int BLANK_MAGIC_CODE = -875286124;
    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static List<CommitLogMessage> parse(String path) {
        return read(path, null);
    }

    public static List<CommitLogMessage> parse(String path, int total) {
        return read(path, total);
    }

    private static List<CommitLogMessage> read(String path, Integer total) {
        // 返回值
        List<CommitLogMessage> retList = Objects.isNull(total) ? new ArrayList<>() : new ArrayList<>(total);
        // 读取的消息条数
        int readCount = Objects.isNull(total) ? Integer.MAX_VALUE : total;
        // 打开文件
        try (RandomAccessFile file = openFile(path)) {
            for (int i = 0; i < readCount; i++) {
                // 优先读取一条消息的总大小
                int totalSize = file.readInt();
                if (totalSize < 1) {
                    // 读到文件末尾
                    break;
                }
                // 再读取魔数, 判断是否读取到文件末尾
                int magicCode = file.readInt();
                if (Objects.equals(BLANK_MAGIC_CODE, magicCode)) {
                    // 读到文件末尾
                    break;
                }
                // 依次读取消息格式
                retList.add(doRead(totalSize, magicCode, file));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return retList;
    }

    private static RandomAccessFile openFile(String path) {
        try {
            return new RandomAccessFile(path, "r");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static CommitLogMessage doRead(int totalSize, int magicCode, RandomAccessFile file) {
        try {
            CommitLogMessage result = new CommitLogMessage();
            result.setTotalSize(totalSize);
            result.setMagicCode(magicCode);
            result.setBodyCyc(file.readInt());
            result.setQueueId(file.readInt());
            result.setFlag(file.readInt());
            result.setQueueOffset(file.readLong());
            result.setPhysicalOffset(file.readLong());
            result.setSysFlag(file.readInt());
            result.setBornTimeStamp(convertTime(file.readLong()));
            result.setBornHost(readAddress(result.getSysFlag(), true, file));
            result.setStoreTimeStamp(convertTime(file.readLong()));
            result.setStoreHost(readAddress(result.getSysFlag(), false, file));
            result.setReconsumeTimes(file.readInt());
            result.setPreparedTransactionOffset(file.readLong());

            // body length
            int bodyLength = file.readInt();
            byte[] body = readBytes(bodyLength, file);
            result.setBodyLength(bodyLength);
            result.setBody(body);

            // topic length
            byte topicLength = file.readByte();
            byte[] topic = readBytes(topicLength, file);
            result.setTopicLength(topicLength);
            result.setTopic(topic);

            // properties
            short propertyLength = file.readShort();
            byte[] properties = readBytes(propertyLength, file);
            result.setPropertiesLength(propertyLength);
            result.setPropertiesData(properties);

            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] readBytes(int length, RandomAccessFile file) throws IOException {
        byte[] bytes = new byte[length];
        file.read(bytes);
        return bytes;
    }

    private static InetSocketAddress readAddress(int sysFlag, boolean isBornHost, RandomAccessFile file) throws IOException {
        int flag = isBornHost ? BORNHOST_V6_FLAG : STOREHOSTADDRESS_V6_FLAG;
        int readBytes = (sysFlag & flag) == 0 ? 4 : 16;
        InetAddress address = InetAddress.getByAddress(readBytes(readBytes, file));
        int port = file.readInt();
        return new InetSocketAddress(address, port);
    }

    private static String convertTime(long timestamp) {
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        return formatter.format(dateTime);
    }
}
