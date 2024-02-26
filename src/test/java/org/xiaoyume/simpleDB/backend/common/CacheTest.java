package org.xiaoyume.simpleDB.backend.common;

import org.junit.Test;
import org.xiaoyume.simpleDB.backend.utils.Panic;

import java.security.SecureRandom;
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

    static Random random = new SecureRandom();

    @Test
    public void testCache(){
        cache = new MockCache();
        cdl = new CountDownLatch(200);
        for(int i = 0; i < 200; i++){
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
            long uid = random.nextInt();
            long h = 0;
            try{
                h = cache.get(uid);
            }catch (Exception e){
                if(e.getMessage().equals("cache is full!")) continue;
                Panic.panic(e);
            }
            assert h == uid;
//            cache.release(h);
        }
        cdl.countDown();
    }
}
