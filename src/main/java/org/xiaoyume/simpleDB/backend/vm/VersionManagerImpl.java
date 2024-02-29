package org.xiaoyume.simpleDB.backend.vm;

import org.xiaoyume.simpleDB.backend.common.AbstractCache;
import org.xiaoyume.simpleDB.backend.dm.DataManager;
import org.xiaoyume.simpleDB.backend.tm.TransactionManager;
import org.xiaoyume.simpleDB.backend.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/26 20:00
 */
public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager{
    TransactionManager tm;
    DataManager dataManager;
    Map<Long, Transaction> activeTransaction;
    Lock lock;
    LockTable lt;

    public VersionManagerImpl(TransactionManager tm, DataManager dataManager) {
        super(0);
        this.tm = tm;
        this.dataManager = dataManager;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }
    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null){
            throw t.err;
        }

        Entry entry = null;
        try{
            entry = super.get(uid);
        }catch (Exception e){
            if("null entry".equals(e.getMessage())){
                return null;
            }else{
                throw e;
            }
        }

        try{
            if(Visibility.isVisible(tm, t, entry)){
                return entry.data();
            }else{
                return null;
            }
        }finally {
            entry.release();
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();
        if(t.err != null){
            throw t.err;
        }

        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dataManager.insert(xid, raw);
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null){
            throw t.err;
        }
        Entry entry = null;
        try{
            entry = super.get(uid);
        }catch (Exception e){
            if("null entry".equals(e.getMessage())){
                return false;
            }else{
                throw e;
            }
        }
        try{
            if(!Visibility.isVisible(tm, t, entry)){
                Lock l = null;
                try{
                    l = lt.add(xid, uid);
                }catch (Exception e){
                    t.err = new RuntimeException("concurrent update issue!");
                    internAbort(xid, true);
                    t.autoAborted = true;
                    throw t.err;
                }
            }
            lock.lock();
            lock.unlock();

            if(entry.getXmax() == xid){
                return false;
            }

            if(Visibility.isVersionSkip(tm, t, entry)){
                t.err = new RuntimeException("concurrent update issue!");
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }

            entry.setXmax(xid);
            return true;
        }finally {
            entry.release();
        }
    }

    @Override
    public long begin(int level) {
        lock.lock();
        try{
            long xid = tm.begin();
            Transaction t = Transaction.newTransaction(xid, level, activeTransaction);
            activeTransaction.put(xid, t);
            return xid;
        }finally {
            lock.unlock();
        }
    }

    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null){
            throw t.err;
        }
        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();
        lt.remove(xid);
        tm.commit(xid);
    }

    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    private void internAbort(long xid, boolean autoAborted){
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        if(!autoAborted){
            activeTransaction.remove(xid);
        }
        lock.unlock();

        if(t.autoAborted) return;
        lt.remove(xid);
        tm.abort(xid);
    }
    public void releaseEntry(Entry entry){
        super.release(entry.getUID());
    }

    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = null;
        try{
            entry = Entry.loadEntry(this, uid);
        }catch (Exception e){
            throw e;
        }

        if(entry == null){
            throw new RuntimeException("null entry");
        }
        return entry;
    }

    @Override
    protected void releaseForCache(Entry entry){
        entry.remove();
    }
}