package org.xiaoyume.simpleDB.backend.utils;

import java.util.Random;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/18 10:48
 */
public class RandomUtil {
    public static byte[] randomBytes(int length){
        Random r = new Random(System.nanoTime());
        byte[] buf = new byte[length];
        for(int i = 0; i < length; i++){
            int tmp = r.nextInt(Integer.MAX_VALUE) % 62;//随机一个数字，范围0-61
            if(tmp < 26){//设置为小写字母
                buf[i] = (byte)('a' + tmp);
            }else if(tmp < 52){//设置为大写字母
                buf[i] = (byte)('A' + tmp - 26);
            }else{//设置为数字
                buf[i] = (byte)('0' + tmp - 52);
            }
        }
        return buf;
    }
}
