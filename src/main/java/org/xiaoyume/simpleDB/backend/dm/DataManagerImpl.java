package org.xiaoyume.simpleDB.backend.dm;

import org.xiaoyume.simpleDB.backend.dm.dataItem.DataItem;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/18 21:39
 */
public class DataManagerImpl implements DataManager{
    @Override
    public DataItem read(long uid) throws Exception {
        return null;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        return 0;
    }

    @Override
    public void close() {

    }
    public void logDataItem(long xid, DataItem di){

    }
    public void releaseDataItem(DataItem di){

    }
}
