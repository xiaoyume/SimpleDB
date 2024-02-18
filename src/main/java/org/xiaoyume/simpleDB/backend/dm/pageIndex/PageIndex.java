package org.xiaoyume.simpleDB.backend.dm.pageIndex;

import org.xiaoyume.simpleDB.backend.dm.pcache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/18 20:12
 */
public class PageIndex {
    //一个页划分为40个区间
    private static final int INTERVALS_NO = 40;
    //阈值,
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    private List<PageInfo>[] lists;

    public PageIndex(){
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO + 1];
        for(int i = 0; i < INTERVALS_NO + 1; i++){
            lists[i] = new ArrayList<>();
        }
    }

    /**
     * 把页面信息添加到对应的列表中
     * @param pageNo
     * @param freeSpace
     */
    public void add(int pageNo, int freeSpace){
        lock.lock();
        try{
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pageNo, freeSpace));
        }finally {
            lock.unlock();
        }
    }
    public PageInfo select(int spaceSize){
        lock.lock();
        try{
            int number = spaceSize / THRESHOLD;
            if(number < INTERVALS_NO) number ++;
            while(number <= INTERVALS_NO){
                if(lists[number].size() == 0){
                    number ++;
                    continue;
                }
                return lists[number].remove(0);
            }
        }finally {
            lock.unlock();
        }
        return null;
    }
}
