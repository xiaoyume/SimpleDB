package org.xiaoyume.simpleDB.backend.dm;

import org.junit.Test;
import org.xiaoyume.simpleDB.backend.common.SubArray;
import org.xiaoyume.simpleDB.backend.dm.dataItem.DataItem;
import org.xiaoyume.simpleDB.backend.dm.pageCache.PageCache;
import org.xiaoyume.simpleDB.backend.tm.MockTransactionManager;
import org.xiaoyume.simpleDB.backend.tm.TransactionManager;
import org.xiaoyume.simpleDB.backend.tm.TransactionManagerImpl;
import org.xiaoyume.simpleDB.backend.utils.Panic;
import org.xiaoyume.simpleDB.backend.utils.RandomUtil;

import java.io.File;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/21 16:51
 */
public class DataManagerTest {

    static List<Long> uids0, uids1;
    static Lock uidsLock;

    static Random random = new SecureRandom();
    private void initUids(){
        uids0 = new ArrayList<>();
        uids1 = new ArrayList<>();
        uidsLock = new ReentrantLock();
    }
    @Test
    public void testDMSingle() throws InterruptedException {
        TransactionManager tm0 = new MockTransactionManager();
        DataManager dm0 = DataManager.create("D:/db/TESTDMSingle", PageCache.PAGE_SIZE*10, tm0);
        DataManager mdm = MockDataManager.newMockDataManager();
        int tasksNum = 10000;
        CountDownLatch cdl = new CountDownLatch(1);
        initUids();
        Runnable r = () -> worker(dm0, mdm, tasksNum, 50, cdl);
        new Thread(r).run();
        cdl.await();
        dm0.close();
        mdm.close();
//        new File("D:/db/TESTDMSingle.db").delete();
//        new File("D:/db/TESTDMSingle.log").delete();
    }

    @Test
    public void testDMMulti() throws InterruptedException {
        TransactionManager tm0 = new MockTransactionManager();
        DataManager dm0 = DataManager.create("D:/db/testDMMulti", PageCache.PAGE_SIZE*10, tm0);
        DataManager mdm = MockDataManager.newMockDataManager();
        int tasksNum = 500;
        CountDownLatch cdl = new CountDownLatch(10);
        initUids();
        for(int i = 0; i < 10; i++){
            Runnable r = () -> worker(dm0, mdm, tasksNum, 50, cdl);
            new Thread(r).run();
        }
        cdl.await();
        dm0.close();
        mdm.close();

    }

    @Test
    public void testRecoverSimple() throws InterruptedException {
        TransactionManager tm0 = TransactionManager.create("D:/db/testrecover");
        DataManager dm0 = DataManager.create("D:/db/testrecover", PageCache.PAGE_SIZE*30, tm0);
        DataManager mdm = MockDataManager.newMockDataManager();
        dm0.close();

        initUids();
        int workerNums = 10;
        for(int i = 0; i < 8; i++){
            dm0 = DataManager.open("D:/db/testrecover", PageCache.PAGE_SIZE*10, tm0);
            CountDownLatch cdl = new CountDownLatch(workerNums);
            for(int k = 0; k < workerNums; k++){
                final DataManager dm = dm0;
                Runnable r = () -> worker(dm, mdm, 100, 50, cdl);
                new Thread(r).run();
            }
            cdl.await();
        }
        dm0.close();
        mdm.close();
    }

    private void worker(DataManager dm0, DataManager dm1, int tastsNum, int insertRation, CountDownLatch cdl){
        int dataLen = 60;
        try{
            for(int i = 0; i < tastsNum; i++){
                int op = Math.abs(random.nextInt()) % 100;
                if(op < insertRation){//向两个dm里插入相同的数据，生成不同的uid
                    //生成随机datalen长度的随机字节数据
                    byte[] data = RandomUtil.randomBytes(dataLen);
                    long u0, u1 = 0;
                    try{
                        u0 = dm0.insert(0, data);
                    }catch (Exception e){
                        continue;
                    }
                    try{
                        u1 = dm1.insert(0, data);
                    }catch (Exception e){
                        Panic.panic(e);
                    }
                    uidsLock.lock();
                    uids0.add(u0);
                    uids1.add(u1);
                    uidsLock.unlock();
                }else{//从两个不同的dm里取出数据，比较是否相同
                    uidsLock.lock();
                    //还没有插入数据
                    if(uids0.size() == 0){
                        uidsLock.unlock();
                        continue;
                    }
                    int tmp = Math.abs(random.nextInt()) % uids0.size();//随机一个已存在的uid
                    long u0 = uids0.get(tmp);
                    long u1 = uids1.get(tmp);
                    DataItem data0 = null, data1 = null;
                    try{
                        data0 = dm0.read(u0);
                    }catch (Exception e){
                        Panic.panic(e);
                        continue;
                    }
                    if(data0 == null){
                        continue;
                    }
                    try {
                        data1 = dm1.read(u1);
                    }catch (Exception e){

                    }
                    data0.rLock();
                    data1.rLock();
                    SubArray s0 = data0.data();
                    SubArray s1 = data1.data();
                    assert Arrays.compare(Arrays.copyOfRange(s0.raw, s0.start, s0.end), Arrays.copyOfRange(s1.raw, s1.start, s1.end)) == 0;
                    data0.rUnLock();
                    data1.rUnLock();

                    byte[] newData = RandomUtil.randomBytes(dataLen);
                    //更改数据，先使用before复制到olddata里
                    data0.before();
                    data1.before();
                    System.arraycopy(newData, 0, s0.raw, s0.start, dataLen);
                    System.arraycopy(newData, 0, s1.raw, s1.start, dataLen);
                    data0.after(0); data1.after(0);
                    data0.release(); data1.release();


                }
            }
        }finally {
            cdl.countDown();
        }
    }
}
