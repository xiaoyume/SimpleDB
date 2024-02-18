package org.xiaoyume.simpleDB.backend.common;

import org.junit.Test;
import org.xiaoyume.simpleDB.backend.utils.Panic;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/17 21:57
 */
public class CacheTest {
    private CountDownLatch cdl;
    private MockCache cache;

    @Test
    public void testCache(){
        cache = new MockCache();
        cdl = new CountDownLatch(1000);
        for(int i = 0; i < 1000; i++){
            Runnable r = () -> work();
            new Thread(r).run();
        }
        try{
            cdl.await();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    private void work(){
        for(int i = 0; i < 1000; i++){
            long uid = new Random(System.nanoTime()).nextInt();
            long h = 0;
            try{
                h = cache.get(uid);
            }catch (Exception e){
                Panic.panic(e);
            }
            assert h == uid;
        }
        cdl.countDown();
    }
}
