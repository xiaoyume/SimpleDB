package org.xiaoyume.simpleDB.utils;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/17 17:55
 */
public class Panic {
    /**
     * 发生异常时打印异常堆栈跟踪信息，然后退出程序
     * @param err
     */
    public static void panic(Exception err){
        err.printStackTrace();
        System.exit(1);
    }

}
