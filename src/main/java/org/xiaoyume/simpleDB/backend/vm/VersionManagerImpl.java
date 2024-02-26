package org.xiaoyume.simpleDB.backend.vm;

import org.xiaoyume.simpleDB.backend.dm.DataManager;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/26 20:00
 */
public class VersionManagerImpl implements VersionManager{
    DataManager dataManager;
    @Override
    public byte[] read(long xid, long uid) throws Exception {
        return new byte[0];
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        return 0;
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        return false;
    }

    @Override
    public long begin(int level) {
        return 0;
    }

    @Override
    public void commit(long xid) throws Exception {

    }

    @Override
    public void abort(long xid) throws Exception {

    }
    public void releaseEntry(Entry entry){

    }
}
