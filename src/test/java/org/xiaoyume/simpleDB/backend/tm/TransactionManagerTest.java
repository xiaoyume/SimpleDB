package org.xiaoyume.simpleDB.backend.tm;

import junit.framework.TestCase;
import org.junit.Test;
import org.xiaoyume.simpleDB.backend.tm.TransactionManager;

import java.io.File;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManagerTest extends TestCase {
    static Random random = new SecureRandom();
    //事务数量
    private int transCnt = 0;
    //线程数量
    private int noWorkers = 1;
    //每个线程要执行的工作数量
    private int noworks = 5;
    //用于线程同步
    private Lock lock = new ReentrantLock();
    //事务管理器
    private TransactionManager tm;
    //事务id和状态的映射
    private Map<Long, Byte> transMap;
    //用于等待所有工作线完成
    private CountDownLatch cdl;

    @Test
    public void testMultiThread() {
        tm = TransactionManager.create("D:/db/tranmger_test");
        transMap = new ConcurrentHashMap<>();
        cdl = new CountDownLatch(noWorkers);
        for (int i = 0; i < noWorkers; i++) {
            Runnable r = () -> woker();
            new Thread(r).start();
        }
        try {
            cdl.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void woker() {
        boolean inTrans = false;
        long transXID = 0;
        for (int i = 0; i < noworks; i++) {
            //生成一个随机0-5的整数
            int op = random.nextInt(6);
            //如果op = 0
            if (op == 0) {
                lock.lock();
                //如果不在事务中,开启一个事务，存储事务id和状态0表示活跃
                if (inTrans == false) {
                    long xid = tm.begin();
                    transMap.put(xid, (byte) 0);
                    transCnt++;
                    transXID = xid;
                    inTrans = true;

                } else {//已经开启了事务
                    //随机一个1，2的数，然后根据这个数来决定是否提交或者回滚
                    int status = (random.nextInt(Integer.MAX_VALUE) % 2) + 1;
                    switch (status) {
                        case 1:
                            tm.commit(transXID);
                            break;
                        case 2:
                            tm.abort(transXID);
                            break;
                    }
                    //更新状态
                    transMap.put(transXID, (byte) status);
                    inTrans = false;
                }
                lock.unlock();
            } else {
                //随机检查，事务状态一致
                lock.lock();
                if (transCnt > 0) {
                    long xid = (long) ((random.nextInt(Integer.MAX_VALUE) % transCnt) + 1);
                    byte status = transMap.get(xid);
                    boolean ok = false;
                    switch (status) {
                        case 0:
                            ok = tm.isActive(xid);
                            break;
                        case 1:
                            ok = tm.isCommitted(xid);
                            break;
                        case 2:
                            ok = tm.isAborted(xid);
                            break;

                    }
                    assert ok;
                }
                lock.unlock();
            }
        }
        cdl.countDown();
    }

}