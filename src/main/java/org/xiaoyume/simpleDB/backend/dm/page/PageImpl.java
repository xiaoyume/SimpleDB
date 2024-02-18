package org.xiaoyume.simpleDB.backend.dm.page;

import org.xiaoyume.simpleDB.backend.dm.pcache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/18 11:01
 */
public class PageImpl implements Page{

    private int pageNo;
    private byte[] data;
    private boolean dirty;
    private Lock lock;
    private PageCache pc;
    public PageImpl(int pageNo, byte[] data, PageCache pc) {
        this.pageNo = pageNo;
        this.data = data;
        this.pc = pc;
        lock = new ReentrantLock();
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
        pc.release(this);
    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public boolean isDirty() {
        return dirty;
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
