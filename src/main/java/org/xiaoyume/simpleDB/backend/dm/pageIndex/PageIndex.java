package org.xiaoyume.simpleDB.backend.dm.pageIndex;

import org.xiaoyume.simpleDB.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 简单的页面管理，就是把1个页分为40区间，0-阈值  阈值-2阈值  添加了一个页面有一个freespace，freespace / 区间阈值
 * 得到一个区间值，就放到这个区间对应的链表里
 * 如果要取一个页信息，根据需要的空间大小，除以阈值得到一个区间值，这个值加1之后的区间里的链表里所有的页信息的空闲空间都是可以满足大小的
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
     * 首先，根据空闲空间大小计算出区间编号 number，然后将页面信息添加到对应的 lists 数组元素（对应的区间）中。
     * @param pageNo
     * @param freeSpace
     */
    public void add(int pageNo, int freeSpace){
        lock.lock();
        try{
            //空闲空间不会超过页面大小，所以除一个阈值得到分配到哪个区间里
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pageNo, freeSpace));
        }finally {
            lock.unlock();
        }
    }

    /**
     * 根据空间大小选择一个页面
     * @param spaceSize
     * @return
     */
    public PageInfo select(int spaceSize){
        lock.lock();
        try{
            int number = spaceSize / THRESHOLD;
            //往前加1个，保证freespace空间大于需求的空间
            if(number < INTERVALS_NO) number ++;
            while(number <= INTERVALS_NO){
                if(lists[number].size() == 0){
                    number ++;
                    continue;
                }
                return lists[number].remove(0);
            }
            return null;
        }finally {
            lock.unlock();
        }
    }
}
