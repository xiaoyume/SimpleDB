package org.xiaoyume.simpleDB.common;

import java.nio.ByteBuffer;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/17 17:49
 */
public class Parser {
    /**
     * 将一个长度为8字节的字节数组转换为一个long类型的整数
     * @param buf
     * @return
     */
    public static long parseLong(byte[] buf){
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 8);
        return buffer.getLong();
    }

    /**
     * 把long数据转为byte数组
     * @param value
     * @return
     */
    public static byte[] long2Byte(long value){
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }
}
