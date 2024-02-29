package org.xiaoyume.simpleDB.backend.dm.page;

import org.xiaoyume.simpleDB.backend.dm.pageCache.PageCache;
import org.xiaoyume.simpleDB.backend.utils.RandomUtil;

import java.util.Arrays;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 第一页
 * validCheck
 * db启动时给100 - 107字节处填入一个随机字节，db关闭时将其copy到108-115字节处
 * 用于判断上一次数据库是否正常关闭
 * @date 2024/2/18 20:51
 */
public class PageOne {
    //用于验证数据库的一个变量，validCheck
    private static final int OF_VC = 100;
    private static final int LEN_VC = 8;
    public static byte[] InitRaw(){
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }
    public static void setVcOpen(Page page){
        page.setDirty(true);
        setVcOpen(page.getData());
    }

    /**
     * 设置一个随机字节到Page的100-107字节处
     * @param raw
     */
    private static void setVcOpen(byte[] raw){
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    public static void setVcClose(Page page){
        page.setDirty(true);
        setVcClose(page.getData());
    }

    /**
     * 设置随机字节到Page的108-115字节处
     * @param raw
     */
    private static void setVcClose(byte[] raw){
        System.arraycopy(raw, OF_VC, raw, OF_VC + LEN_VC, LEN_VC);
    }
    public static boolean checkVc(Page page){
        return check(page.getData());
    }
    private static boolean check(byte[] raw){
        return Arrays.compare(raw, OF_VC, OF_VC+LEN_VC, raw, OF_VC+LEN_VC, OF_VC+2*LEN_VC) == 0;
    }
}
