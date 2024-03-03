package org.xiaoyume.simpleDB.backend.utils;

import java.security.SecureRandom;
import java.util.Random;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/18 10:48
 */
public class RandomUtil {
    /**
     * 生成随机字节数组
     *
     * @param length
     * @return
     */
    public static byte[] randomBytes(int length) {
        Random r = new SecureRandom();
        byte[] buf = new byte[length];
        r.nextBytes(buf);
        return buf;
    }
}
