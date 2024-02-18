package org.xiaoyume.simpleDB.backend.utils;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/18 22:11
 */
public class Types {
    /**
     * 根据pageNo和offset计算出uid
     * @param pageNo
     * @param offset
     * @return
     */
    public static long addressToUid(int pageNo, short offset){
        long u0 = (long)pageNo;
        long u1 = (long)offset;
        return u0 << 32 | u1;
    }
}
