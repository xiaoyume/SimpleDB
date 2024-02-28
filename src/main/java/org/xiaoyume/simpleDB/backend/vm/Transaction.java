package org.xiaoyume.simpleDB.backend.vm;

import org.xiaoyume.simpleDB.backend.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 事务，事务快照
 * @date 2024/2/26 19:58
 */
public class Transaction {
    public long xid;
    //事务的隔离级别
    public int level;
    public Map<Long, Boolean> snapshot;
    public Exception err;
    public boolean autoAborted;
    public static Transaction newTransaction(long xid, int level, Map<Long, Boolean> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        if(level != 0){
            t.snapshot = new HashMap<>();
            //遍历活跃事务操作，将活跃事务操作记录到快照中
            for(Long x : active.keySet()){
                t.snapshot.put(x, true);
            }
        }
        return t;
    }
    public boolean isInSnapshot(long xid){
        if(xid == TransactionManagerImpl.SUPER_XID){
            return false;
        }
        return snapshot.containsKey(xid);
    }
}
