package org.xiaoyume.simpleDB.backend.dm;

import org.xiaoyume.simpleDB.backend.dm.dataItem.DataItem;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/18 21:33
 */
public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();
}
