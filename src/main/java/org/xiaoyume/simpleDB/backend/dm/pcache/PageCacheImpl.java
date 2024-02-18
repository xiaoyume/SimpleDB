package org.xiaoyume.simpleDB.backend.dm.pcache;

import org.xiaoyume.simpleDB.backend.common.AbstractCache;
import org.xiaoyume.simpleDB.backend.dm.page.Page;
import org.xiaoyume.simpleDB.backend.dm.page.PageImpl;
import org.xiaoyume.simpleDB.backend.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/18 11:04
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache{
    //最新缓存限制
    private static int MEM_MIN_LIM = 10;
    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;
    private AtomicInteger pageNos;

    /**
     * 根据文件路径和内存大小创建PageCache
     * @param path
     * @param memory
     * @return
     */
    public static PageCacheImpl create(String path, long memory){
        File f = new File(path);
        try{
            if(!f.createNewFile()){
                Panic.panic(new RuntimeException("file already exists!"));
            }
        }catch (Exception e){
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()){
            Panic.panic(new RuntimeException("file cannot read or write!"));
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
        File f = new File(path);
        if(!f.exists()){
            Panic.panic(new RuntimeException("file not exists!"));
        }
        if(!f.canRead() || !f.canWrite()){
            Panic.panic(new RuntimeException("file cannot read or write!"));
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

    public PageCacheImpl(RandomAccessFile file, FileChannel fc, int maxResource){
        super(maxResource);
        if(maxResource < MEM_MIN_LIM){
            Panic.panic(new RuntimeException("memory too small!"));
        }
        long length = 0;
        try{
            length = file.length();
        }catch (IOException e){
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fc;
        this.fileLock = new ReentrantLock();
        this.pageNos = new AtomicInteger((int)length/PAGE_SIZE);
    }
    @Override
    public int newPage(byte[] initData){
        int pageNo = pageNos.incrementAndGet();
        Page page = new PageImpl(pageNo, initData, null);
        flush(page);
        return pageNo;
    }
    @Override
    public Page getPage(int pageNo) throws Exception {
        Page page;
        try{
            page = get((long)pageNo);
        }catch (Exception e){
            throw e;
        }
        return page;
    }

    /**
     * 根据pageNo从数据库文件里读取页数据，并转为Page
     * @param key
     * @return
     * @throws Exception
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pageNo = (int)key;
        long offset = PageCacheImpl.pageOffset(pageNo);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try{
            fc.position(offset);
            fc.read(buf);
        }catch (IOException e){
            Panic.panic(new RuntimeException(" "));
        }
        fileLock.unlock();
        return new PageImpl(pageNo, buf.array(), this);
    }

    @Override
    protected void releaseForCache(Page page) {
        if(page.isDirty()){
            flush(page);
            page.setDirty(false);
        }
    }
    public void release(Page page){
        release((long)page.getPageNumber());
    }
    public void flushPage(Page page){
        flush(page);
    }

    /**
     *强制刷新页面
     * @param page
     */
    private void flush(Page page){
        int pageNo = page.getPageNumber();
        long offset = pageOffset(pageNo);

        fileLock.lock();
        try{
            ByteBuffer buf = ByteBuffer.wrap(page.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(true);
        }catch (IOException e){
            Panic.panic(e);
        }finally {
            fileLock.unlock();
        }
    }

    @Override
    public void truncateByBgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);
        try{
            file.setLength(size);
        }catch (IOException e){
            Panic.panic(e);
        }
        pageNos.set(maxPgno);
    }
    @Override
    public void close() {
        super.close();
        try{
            fc.close();
            file.close();
        }catch (IOException e){
            Panic.panic(e);
        }
    }

    @Override
    public int getPageNumber() {
        return pageNos.intValue();
    }

    /**
     * 根据页号获取页偏移
     * @param pageNo
     * @return
     */
    private static long pageOffset(int pageNo){
        return (pageNo - 1) * PAGE_SIZE;
    }


}
