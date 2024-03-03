package org.xiaoyume.simpleDB.backend.common;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/18 21:31
 */
public class SubArray {
    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
