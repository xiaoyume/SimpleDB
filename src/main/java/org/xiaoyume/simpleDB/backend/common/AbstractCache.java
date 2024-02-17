package org.xiaoyume.simpleDB.backend.common;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 实现了一个执行LRU驱逐策略的通用缓存
 * @date 2024/2/17 21:33
 */
public abstract class AbstractCache<T> {
    //实际缓存数据
    private HashMap<Long, T> cache;
    //使用时间
    private LinkedList<Long> cacheKeysList;
    //正在获取资源的线程,线程安全实现
    private ConcurrentHashMap<Long, Boolean> getting;

    private int maxResource;
    private Lock lock;

    public AbstractCache(int maxResource){
        this.maxResource = maxResource;
        cache = new HashMap<>();
        cacheKeysList = new LinkedList<>();
        getting = new ConcurrentHashMap<>();
        lock = new ReentrantLock();
    }
    public T get(long key) throws Exception {
        while(true){
            lock.lock();
            if(getting.containsKey(key)){
                //请求的资源正在被其它线程获取，等待一段时间
                lock.unlock();
                try{
                    Thread.sleep(1);
                }catch (InterruptedException e){
                    e.printStackTrace();
                    continue;
                }
                continue;
            }
            //如果资源在缓存中，直接返回
            if(cache.containsKey(key)){
                //资源在缓存中，直接返回
                T obj = cache.get(key);
                //更新使用时间
                cacheKeysList.remove(key);
                //最新使用的放到前面
                cacheKeysList.addFirst(key);
                lock.unlock();
                return obj;
            }
            //资源不在缓存中，正在请求资源，直接终端循环，需要读取资源
            getting.put(key, true);
            lock.unlock();
            break;
        }

        T obj = null;
        try{
            //不在缓存获取资源
            obj = getForCache(key);
        }catch (Exception e){
            lock.lock();
            getting.remove(key);
            lock.lock();
            throw e;
        }

        lock.lock();
        //移除正在获取资源的状态，入缓存，返回资源
        getting.remove(key);
        if(cache.size() == maxResource){
            //缓存已满释放一个资源
            release(cacheKeysList.getLast());
        }
        cache.put(key, obj);
        cacheKeysList.addFirst(key);
        lock.unlock();
        return obj;
    }

    /**
     * 释放一个缓存
     * @param key
     */
    public void release(long key){
        lock.lock();
        try{
            T obj = cache.get(key);
            if(obj == null) return;
            releaseForCache(obj);
            cache.remove(key);
            cacheKeysList.remove(key);
        }finally {
            lock.unlock();
        }
    }

    public void close(){
        lock.lock();
        try{
            Set<Long> keys = cache.keySet();
            for(long key : keys){
                release(key);
                cache.remove(key);
                cacheKeysList.remove(key);
            }
        }finally {
            lock.unlock();
        }
    }

    /**
     * 当资源不在缓存时获取
     * @param key
     * @return
     * @throws Exception
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 当资源被驱逐时写回
     * @param obj
     */
    protected abstract void releaseForCache(T obj);

}
