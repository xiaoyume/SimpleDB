package org.xiaoyume.simpleDB.backend.utils;

import com.google.common.primitives.Bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/17 17:49
 */
public class Parser {
    public static byte[] short2Byte(short value) {
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(value).array();
    }

    public static short parseShort(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 2);
        return buffer.getShort();
    }

    public static byte[] int2Byte(int value) {
        return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    }

    public static int parseInt(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 4);
        return buffer.getInt();
    }

    /**
     * 将一个长度为8字节的字节数组转换为一个long类型的整数
     *
     * @param buf
     * @return
     */
    public static long parseLong(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 8);
        return buffer.getLong();
    }

    /**
     * 把long数据转为byte数组
     *
     * @param value
     * @return
     */
    public static byte[] long2Byte(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }

    public static ParseStringRes parseString(byte[] raw) {
        int len = parseInt(Arrays.copyOf(raw, 4));
        String str = new String(Arrays.copyOfRange(raw, 4, 4 + len));
        return new ParseStringRes(str, len + 4);
    }

    /**
     * 长度 + 数据
     *
     * @param str
     * @return
     */
    public static byte[] string2Byte(String str) {
        byte[] l = int2Byte(str.length());
        return Bytes.concat(l, str.getBytes());
    }

    public static long str2Uid(String key) {
        long seed = 13331;
        long res = 0;
        for (byte b : key.getBytes()) {
            res = res * seed + (long) b;
        }
        return res;
    }
}
