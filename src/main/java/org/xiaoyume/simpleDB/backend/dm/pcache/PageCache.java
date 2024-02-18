package org.xiaoyume.simpleDB.backend.dm.pcache;

public interface PageCache {
    public static final int PAGE_SIZE = 1 << 13;//4096
    int newPage(byte[] initData);
    Page getPage(int pgNo) throws Exception;
    void close();
    void release(Page page);
    //按照最大页号截断页面缓存
    void truncateByBgno(int maxPgno);
    int getPageNumber();
    void flushPage(Page pg);
}
