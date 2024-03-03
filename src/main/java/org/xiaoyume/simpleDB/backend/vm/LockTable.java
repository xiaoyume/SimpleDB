package org.xiaoyume.simpleDB.backend.vm;

import org.xiaoyume.simpleDB.common.Error;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 维护一个依赖等待图，死锁检测
 * @date 2024/2/26 21:17
 */
public class LockTable {
    private Map<Long, List<Long>> x2u;//1个事务对应的资源列表
    private Map<Long, Long> u2x;//资源对应的事务
    private Map<Long, List<Long>> wait;//资源 对应 等待事务列表
    private Map<Long, Lock> waitLock;//xid事务等待对应资源的锁
    private Map<Long, Long> waitU;//事务 等待的 资源uid
    private Lock lock;

    public LockTable (){
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     *
     * @param xid
     * @param uid
     * @return
     * @throws Exception
     */
    public Lock add(long xid, long uid) throws Exception{
        lock.lock();
        try{
            //已经在事务对应的资源列表中，说明已经获取了资源，直接返回null
            if(isInList(x2u, xid, uid)){
                return null;
            }
            //如果资源没有事务获取
            if(!u2x.containsKey(uid)){
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                return null;
            }
            //资源已经被某个事务获取，等待
            waitU.put(xid, uid);
            //放入资源被哪些事务等待列表
            putIntoList(wait, xid, uid);
            //如果有死锁
            if(hasDeadLock()){
                //移除当前事务等待资源
                waitU.remove(xid);
                //从事务等待列表中移除事务
                removeFromList(wait, uid, xid);
                throw Error.DeadlockException;
            }
            //没有死锁，创建一个锁，锁住资源，表示等待资源
            Lock l = new ReentrantLock();
            l.lock();
            //记录事务对应的锁
            waitLock.put(xid, l);
            return l;
        }finally {
            lock.unlock();
        }
    }
    public void remove(long xid){
        lock.lock();
        try{
            List<Long> list = x2u.get(xid);
            if(list != null){
                while(list.size() > 0){
                    Long uid = list.remove(0);
                    selectNewXID(uid);
                }
            }
            waitU.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);
        }finally {
            lock.unlock();
        }
    }

    /**
     * 从等待队列中选择一个xid来占用uid
     * @param uid
     */
    private void selectNewXID(long uid){
        u2x.remove(uid);
        List<Long> list = wait.get(uid);
        if(list == null) return;
        assert list.size() > 0;

        while(list.size() > 0){
            //从资源等待的事务里取一个出来
            long xid = list.remove(0);
            //如果事务没有锁，说明这个事务没有等待的资源
            if(!waitLock.containsKey(xid)){
                continue;
            }else{
                //
                u2x.put(uid, xid);
                //获得事务等待资源的锁，移除等待资源，解锁
                Lock lo = waitLock.remove(xid);
                waitU.remove(xid);
                lo.unlock();
                break;
            }
        }
        //uid的事务等待队列空了，移除
        if(list.size() == 0) wait.remove(uid);
    }

    //记录事务的次序
    private Map<Long, Integer> xidStamp;
    //标记当前遍历的次序
    private int stamp;

    /**
     * 判断是否死锁
     *
     * @return
     */
    private boolean hasDeadLock(){
        xidStamp = new HashMap<>();
        stamp = 1;
        //遍历所有事务
        for(long xid : x2u.keySet()){
            Integer s = xidStamp.get(xid);
            if(s != null && s > 0){
                //当前事务已经被标记了
                continue;
            }
            stamp ++;
            if(dfs(xid)){
                return true;
            }
        }
        return false;
    }

    /**
     * 输入一个事务，判断事务的时间遍历层次，如果和当前一致，死锁
     * 标记当前事务
     * 获得事务正在等待的资源，获得资源对应的事务，递归判断死锁
     * @param xid
     * @return
     */
    private boolean dfs(long xid){
        Integer stp = xidStamp.get(xid);
        //判断事务在该次遍历中是否已经被标记,循环了，说明是死锁
        if(stp != null && stp == stamp){
            return true;
        }
        //stp < stamp 说明不是本次遍历
        if(stp != null && stp < stamp){
            return false;
        }
        //标记当前事务
        xidStamp.put(xid, stamp);

        //获取该事务在等待的资源
        Long uid = waitU.get(xid);
        //如果没有正在等待的资源，说明没有了死锁
        if(uid == null) return false;
        //获取资源对应的事务
        Long x = u2x.get(uid);
        assert x != null;
        //递归判断
        return dfs(x);
    }

    /**
     * 移除对应事务的资源
     * @param x2u
     * @param xid
     * @param uid
     */
    private void removeFromList(Map<Long, List<Long>> x2u, long xid, long uid){
        List<Long> list = x2u.get(xid);
        if(list == null) return;
        Iterator<Long> i = list.iterator();
        while(i.hasNext()){
            long e = i.next();
            if(e == uid){
                i.remove();
                break;
            }
        }
        if(list.size() == 0){
            x2u.remove(xid);
        }
    }

    /**
     * 把资源和事务添加到事务对应资源的列表中
     * @param x2u
     * @param xid
     * @param uid
     */
    private void putIntoList(Map<Long, List<Long>> x2u, long xid, long uid){
        if(!x2u.containsKey(xid)){
            x2u.put(xid, new ArrayList<>());
        }
        x2u.get(xid).add(0, uid);
    }

    /**
     * 判断事务和资源是否在列表中，根据xid对应的资源列表查找
     * @param x2u
     * @param xid
     * @param uid
     * @return
     */
    private boolean isInList(Map<Long, List<Long>> x2u, long xid, long uid){
        List<Long> list = x2u.get(xid);
        if(list == null){
            return false;
        }
        Iterator<Long> i = list.iterator();
        while(i.hasNext()){
            long e = i.next();
            if(e == uid){
                return true;
            }
        }
        return false;
    }
}
