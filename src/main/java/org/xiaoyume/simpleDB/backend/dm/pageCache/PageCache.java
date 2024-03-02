package org.xiaoyume.simpleDB.backend.dm.pageCache;

import org.xiaoyume.simpleDB.common.Error;
import org.xiaoyume.simpleDB.backend.dm.page.Page;
import org.xiaoyume.simpleDB.backend.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

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

    /**
     * 根据文件路径和内存大小创建PageCache
     * @param path
     * @param memory
     * @return
     */
    public static PageCacheImpl create(String path, long memory){
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        try{
            if(!f.createNewFile()){
                Panic.panic(Error.FileNotExistsException);
            }
        }catch (Exception e){
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()){
            Panic.panic(Error.FileCannotRWException);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try{
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        }catch (FileNotFoundException e){
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory / PAGE_SIZE);
    }
    public static PageCacheImpl open(String path, long memory){
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        if(!f.exists()){
            Panic.panic(Error.FileExistsException);
        }
        if(!f.canRead() || !f.canWrite()){
            Panic.panic(Error.FileCannotRWException);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try{
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        }catch (FileNotFoundException e){
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory / PAGE_SIZE);
    }
}
