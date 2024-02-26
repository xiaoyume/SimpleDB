package org.xiaoyume.simpleDB.backend.dm.pageCache;

import org.xiaoyume.simpleDB.backend.dm.page.MockPage;
import org.xiaoyume.simpleDB.backend.dm.page.Page;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/18 14:13
 */
public class MockPageCache implements PageCache{
    private Map<Integer, MockPage> cache = new HashMap<>();
    private Lock lock = new ReentrantLock();
    private AtomicInteger pageCount = new AtomicInteger(0);
    @Override
    public int newPage(byte[] initData) {
        lock.lock();
        try{
            int pageNo = pageCount.incrementAndGet();
            MockPage page = MockPage.newMockPage(pageNo, initData);
            cache.put(pageNo, page);
            return pageNo;
        }finally {
            lock.unlock();
        }
    }

    @Override
    public Page getPage(int pgNo) throws Exception {
        lock.lock();
        try{
            return cache.get(pgNo);
        }finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void release(Page page) {

    }

    @Override
    public void truncateByBgno(int maxPgno) {

    }

    @Override
    public int getPageNumber() {
        return pageCount.intValue();
    }

    @Override
    public void flushPage(Page pg) {

    }
}
