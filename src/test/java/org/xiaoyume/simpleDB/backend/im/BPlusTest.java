package org.xiaoyume.simpleDB.backend.im;

import org.junit.Test;
import org.xiaoyume.simpleDB.backend.dm.DataManager;
import org.xiaoyume.simpleDB.backend.dm.pageCache.PageCache;
import org.xiaoyume.simpleDB.backend.tm.MockTransactionManager;
import org.xiaoyume.simpleDB.backend.tm.TransactionManager;

import java.io.File;
import java.util.List;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/28 21:08
 */
public class BPlusTest {
    @Test
    public void testTreesigle() throws Exception {
        TransactionManager tm = new MockTransactionManager();
        DataManager dm = DataManager.create("D:/db/testtree", PageCache.PAGE_SIZE*10, tm);

        long root = BPlusTree.create(dm);
        BPlusTree tree = BPlusTree.load(root, dm);
        int lim = 1000;
        for(int i = lim-1; i >= 0; i--){
            tree.insert(i, i);
        }

        for(int i = 0; i < lim; i ++) {
            List<Long> uids = tree.search(i);
            assert uids.size() == 1;
            assert uids.get(0) == i;
        }

//        assert new File("D:/db/testtree.db").delete();
//        assert new File("D:/db/testtree.log").delete();

    }
}
