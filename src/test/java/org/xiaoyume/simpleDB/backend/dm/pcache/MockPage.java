package org.xiaoyume.simpleDB.backend.dm.pcache;

import org.xiaoyume.simpleDB.backend.common.MockCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/18 14:08
 */
public class MockPage implements Page{
    private int pageNo;
    private byte[] data;
    private Lock lock = new ReentrantLock();
    public static MockPage newMockPage(int pageNo, byte[] data){
        MockPage mockPage = new MockPage();
        mockPage.pageNo = pageNo;
        mockPage.data = data;
        return mockPage;
    }
    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void release() {

    }

    @Override
    public void setDirty(boolean dirty) {

    }

    @Override
    public boolean isDirty() {
        return false;
    }

    @Override
    public int getPageNumber() {
        return pageNo;
    }

    @Override
    public byte[] getData() {
        return data;
    }
}
