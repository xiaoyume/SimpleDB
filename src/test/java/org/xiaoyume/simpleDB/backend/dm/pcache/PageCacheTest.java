package org.xiaoyume.simpleDB.backend.dm.pcache;

import org.junit.Test;
import org.xiaoyume.simpleDB.backend.dm.page.Page;

import java.io.File;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/18 14:22
 */
public class PageCacheTest {
    @Test
    public void testPageCache() throws Exception {
        //50个页面作为缓存
        PageCache pc = PageCacheImpl.create("D:/db/test0.db", PageCache.PAGE_SIZE * 50);
        //写100个页面
        for(int i = 0; i < 100; i++){
            byte[] tmp = new byte[PageCache.PAGE_SIZE];
            tmp[0] = (byte)i;
            int pageNo = pc.newPage(tmp);
            Page page = pc.getPage(pageNo);
            page.setDirty(true);
            page.release();
        }
        pc.close();

        //读100个页面
        pc = PageCacheImpl.open("D:/db/test0.db", PageCache.PAGE_SIZE * 50);
        for(int i = 1; i <= 100; i++){
            Page page = pc.getPage(i);
            assert page.getData()[0] == (byte)(i - 1);
            page.release();
        }
        pc.close();

        assert new File("D:/db/test0.db").delete();
    }
}
