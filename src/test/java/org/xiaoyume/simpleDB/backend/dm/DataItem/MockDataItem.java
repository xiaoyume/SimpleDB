package org.xiaoyume.simpleDB.backend.dm.DataItem;

import org.xiaoyume.simpleDB.backend.common.SubArray;
import org.xiaoyume.simpleDB.backend.dm.dataItem.DataItem;
import org.xiaoyume.simpleDB.backend.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/21 16:24
 */
public class MockDataItem implements DataItem {
    private SubArray data;
    private byte[] oldData;
    private long uid;
    private Lock rLock;
    private Lock wlock;

    public static MockDataItem newMockDataItem(long uid, SubArray data) {
        MockDataItem mockDataItem = new MockDataItem();
        mockDataItem.data = data;
        mockDataItem.oldData = new byte[data.end - data.start];
        mockDataItem.uid = uid;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        mockDataItem.rLock = lock.readLock();
        mockDataItem.wlock = lock.writeLock();
        return mockDataItem;
    }

    @Override
    public SubArray data() {
        return data;
    }

    /**
     * 提前把数据存到olddata里
     */
    @Override
    public void before() {
        wlock.lock();
        System.arraycopy(data.raw, data.start, oldData, 0, oldData.length);
    }

    /**
     * 数据复制回来
     */
    @Override
    public void unBefore() {
        System.arraycopy(oldData, 0, data.raw, data.start, oldData.length);
        wlock.unlock();
    }

    @Override
    public void after(long xid) {
        wlock.unlock();
    }

    @Override
    public void release() {

    }

    @Override
    public void lock() {
        wlock.lock();
    }

    @Override
    public void unlock() {
        wlock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return null;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldData;
    }

    @Override
    public SubArray getRaw() {
        return data;
    }
}
