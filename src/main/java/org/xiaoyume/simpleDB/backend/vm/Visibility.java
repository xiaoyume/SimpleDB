package org.xiaoyume.simpleDB.backend.vm;

import org.xiaoyume.simpleDB.backend.tm.TransactionManager;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/28 15:38
 */
public class Visibility {
    /**
     * 判断事务是否跳过版本
     *
     * @param tm
     * @param t
     * @param e
     * @return
     */
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e){
        long xmax = e.getXmax();
        if(t.level == 0){
            return false;
        }else{
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e){
        if(t.level == 0){
            return readCommitted(tm, t, e);
        }else{
            return repeatableRead(tm, t, e);
        }
    }

    /**
     * 读已提交 隔离级别判断数据可见性
     * @param tm
     * @param t
     * @param e
     * @return
     */
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e){
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        //是当前事务，可见
        if(xmin == xid && xmax == 0) return true;
        //如果xmin提交了
        if(tm.isCommitted(xmin)){
            //
            if(xmax == 0) return true;
            //如果xmax不是当前事务，需要进一步判断xmax是否提交
            if(xmax != xid){
                if(!tm.isCommitted(xmax)){
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e){
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if(xmin == xid && xmax == 0) return true;

        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)){
            if(xmax == 0) return true;
            if(xmax != xid){
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)){
                    return true;
                }
            }
        }
        return false;
    }
}
