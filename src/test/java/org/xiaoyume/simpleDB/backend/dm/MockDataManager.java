package org.xiaoyume.simpleDB.backend.dm;


import org.xiaoyume.simpleDB.backend.common.SubArray;
import org.xiaoyume.simpleDB.backend.dm.DataItem.MockDataItem;
import org.xiaoyume.simpleDB.backend.dm.dataItem.DataItem;

import javax.xml.crypto.Data;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/21 16:35
 */
public class MockDataManager implements DataManager {
    private Map<Long, DataItem> cache;
    private Lock lock;
    public static MockDataManager newMockDataManager() {
        MockDataManager mockDataManager = new MockDataManager();
        mockDataManager.cache = new HashMap<>();
        mockDataManager.lock = new ReentrantLock();
        return mockDataManager;
    }
    @Override
    public DataItem read(long uid) throws Exception {
        lock.lock();
        try{
            return cache.get(uid);
        }finally {
            lock.unlock();
        }
    }

    /**
     * 插入一个数据，返回uid
     * @param xid
     * @param data
     * @return
     * @throws Exception
     */
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        try{
            //获取一个不重复uid
            long uid = 0;
            while(true){
                uid = Math.abs(new Random(System.nanoTime()).nextInt(Integer.MAX_VALUE));
                if(uid == 0) continue;
                if(cache.containsKey(uid)) continue;
                break;
            }

            DataItem dataItem = MockDataItem.newMockDataItem(uid, new SubArray(data, 0, data.length));
            cache.put(uid, dataItem);
            return uid;
        }finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {

    }
}
