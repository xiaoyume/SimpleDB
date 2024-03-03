package org.xiaoyume.simpleDB.backend.common;

import org.xiaoyume.simpleDB.common.Error;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 实现了一个引用计数策略的缓存
 * @date 2024/2/17 21:33
 */
public abstract class AbstractCache<T> {
    //实际缓存数据
    private HashMap<Long, T> cache;
    //元素引用的个数
    private HashMap<Long, Integer> references;
    //正在获取资源的线程
    private HashMap<Long, Boolean> getting;

    private int maxResource;
    //缓存中的元素个数
    private int count = 0;
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 逻辑：
     * 1.判断是否该资源正在被其它线程获取，如果是就循环等待
     * 2.判断资源是否在缓存中，如果在直接返回，不在就需要从外部获取，读取数据需要时间，所以需要表示资源正在被线程获取
     * 3.从外部读取数据,
     *
     * @param key
     * @return
     * @throws Exception
     */
    protected T get(long key) throws Exception {
        while (true) {
            lock.lock();
            if (getting.containsKey(key)) {
                //请求的资源正在被其它线程获取，等待一段时间
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }
            //如果资源在缓存中，直接返回
            if (cache.containsKey(key)) {
                //资源在缓存中，直接返回
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);

                lock.unlock();
                return obj;
            }
            //资源不在缓存中，正在请求资源，直接终端循环，需要读取资源
            if (maxResource > 0 && count >= maxResource) {
                lock.unlock();
                throw Error.CacheFullException;
            }
            count++;
            getting.put(key, true);
            lock.unlock();
            break;
        }

        T obj = null;
        try {
            //不在缓存,获取资源
            obj = getForCache(key);
        } catch (Exception e) {//资源获取失败，释放资源获取状态
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        lock.lock();
        //移除正在获取资源的状态，入缓存，返回资源
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);
        lock.unlock();
        return obj;
    }

    /**
     * 资源被release一次就计数减1
     *
     * @param key
     */
    protected void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key) - 1;
            if (ref == 0) {
                T obj = cache.get(key);
                //资源被驱逐，释放资源
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count--;
            } else {
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                release(key);
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 当资源不在缓存时获取
     *
     * @param key
     * @return
     * @throws Exception
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 当资源被驱逐时写回
     *
     * @param obj
     */
    protected abstract void releaseForCache(T obj);

}
