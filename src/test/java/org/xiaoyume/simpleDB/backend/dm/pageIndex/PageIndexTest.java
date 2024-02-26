package org.xiaoyume.simpleDB.backend.dm.pageIndex;

import junit.framework.TestCase;
import org.junit.Test;
import org.xiaoyume.simpleDB.backend.dm.pageCache.PageCache;

public class PageIndexTest extends TestCase {
    @Test
    public void testPageIndex(){
        PageIndex pageIndex = new PageIndex();
        int threshold = PageCache.PAGE_SIZE / 20;
        for(int i = 0; i < 20; i++){
            pageIndex.add(i, i*threshold);
            pageIndex.add(i, i*threshold);
            pageIndex.add(i, i*threshold);

        }
        for(int k = 0; k < 3; k++){
            for(int i = 0; i < 19; i++){
                PageInfo pi = pageIndex.select(i * threshold);
                assert pi != null;
                assert pi.pageNo == i + 1;
            }
        }
    }

}