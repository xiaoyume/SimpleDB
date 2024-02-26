package org.xiaoyume.simpleDB.backend.dm.pageCache;

import org.junit.Test;
import org.xiaoyume.simpleDB.backend.dm.page.Page;
import org.xiaoyume.simpleDB.backend.utils.Panic;
import org.xiaoyume.simpleDB.backend.utils.RandomUtil;

import java.io.File;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/18 14:22
 */
public class PageCacheTest {
    static Random random = new SecureRandom();
    @Test
    public void testPageCache() throws Exception {
        //50个页面作为缓存
        PageCache pc = PageCache.create("D:/db/test0", PageCache.PAGE_SIZE * 50);
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
        pc = PageCache.open("D:/db/test0", PageCache.PAGE_SIZE * 50);
        for(int i = 1; i <= 100; i++){
            Page page = pc.getPage(i);
            assert page.getData()[0] == (byte)(i - 1);
            page.release();
        }
        pc.close();

        assert new File("D:/db/test0.db").delete();
    }

    private PageCache pc1;
    private CountDownLatch cdl1;
    private AtomicInteger noPages1;
    @Test
    public void testPageCacheMultiSimple() throws Exception {
        pc1 = PageCache.create("D:/db/pcacher_simple_test1", PageCache.PAGE_SIZE * 50);
        cdl1 = new CountDownLatch(200);
        noPages1 = new AtomicInteger(0);
        for(int i = 0; i < 200; i ++) {
            int id = i;
            Runnable r = () -> worker1(id);
            new Thread(r).start();
        }
        cdl1.await();
//        assert new File("D:/db/pcacher_simple_test1").delete();
    }

    private void worker1(int id) {
        for(int i = 0; i < 80; i ++) {
            int op = Math.abs(random.nextInt() % 20);
            if(op == 0) {
                byte[] data = RandomUtil.randomBytes(PageCache.PAGE_SIZE);
                int pgno = pc1.newPage(data);
                Page pg = null;
                try {
                    pg = pc1.getPage(pgno);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                noPages1.incrementAndGet();
                pg.release();
            } else if(op < 20) {
                int mod = noPages1.intValue();
                if(mod == 0) {
                    continue;
                }
                int pgno = Math.abs(random.nextInt()) % mod + 1;
                Page pg = null;
                try {
                    pg = pc1.getPage(pgno);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                pg.release();
            }
        }
        cdl1.countDown();
    }


    private PageCache pc2, mpc;
    private CountDownLatch cdl2;
    private AtomicInteger noPages2;
    private Lock lockNew;
    @Test
    public void testPageCacheMulti() throws InterruptedException {
        pc2 = PageCache.create("D:/db/pcacher_multi_test", PageCache.PAGE_SIZE * 10);
        mpc = new MockPageCache();
        lockNew = new ReentrantLock();

        cdl2 = new CountDownLatch(30);
        noPages2 = new AtomicInteger(0);

        for(int i = 0; i < 30; i ++) {
            int id = i;
            Runnable r = () -> worker2(id);
            new Thread(r).run();
        }
        cdl2.await();

        assert new File("D:/db/pcacher_multi_test.db").delete();
    }

    private void worker2(int id) {
        for(int i = 0; i < 1000; i ++) {
            int op = Math.abs(random.nextInt() % 20);
            if(op == 0) {
                // new page
                byte[] data = RandomUtil.randomBytes(PageCache.PAGE_SIZE);
                lockNew.lock();
                int pgno = pc2.newPage(data);
                int mpgno = mpc.newPage(data);
                assert pgno == mpgno;
                lockNew.unlock();
                noPages2.incrementAndGet();
            } else if(op < 10) {
                // check
                int mod = noPages2.intValue();
                if(mod == 0) continue;
                int pgno = Math.abs(random.nextInt()) % mod + 1;
                Page pg = null, mpg = null;
                try {
                    pg = pc2.getPage(pgno);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                try {
                    mpg = mpc.getPage(pgno);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                pg.lock();
                assert Arrays.equals(mpg.getData(), pg.getData());
                pg.unlock();
                pg.release();
            } else {
                // update
                int mod = noPages2.intValue();
                if(mod == 0) continue;
                int pgno = Math.abs(random.nextInt()) % mod + 1;
                Page pg = null, mpg = null;
                try {
                    pg = pc2.getPage(pgno);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                try {
                    mpg = mpc.getPage(pgno);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                byte[] newData = RandomUtil.randomBytes(PageCache.PAGE_SIZE);

                pg.lock();
                mpg.setDirty(true);
                for(int j = 0; j < PageCache.PAGE_SIZE; j ++) {
                    mpg.getData()[j] = newData[j];
                }
                pg.setDirty(true);
                for(int j = 0; j < PageCache.PAGE_SIZE; j ++) {
                    pg.getData()[j] = newData[j];
                }
                pg.unlock();
                pg.release();
            }
        }
        cdl2.countDown();
    }
}
