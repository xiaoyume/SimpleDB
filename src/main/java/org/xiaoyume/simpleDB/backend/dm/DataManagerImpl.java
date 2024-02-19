package org.xiaoyume.simpleDB.backend.dm;

import org.xiaoyume.simpleDB.backend.common.AbstractCache;
import org.xiaoyume.simpleDB.backend.dm.dataItem.DataItem;
import org.xiaoyume.simpleDB.backend.dm.dataItem.DataItemImpl;
import org.xiaoyume.simpleDB.backend.dm.logger.Logger;
import org.xiaoyume.simpleDB.backend.dm.page.Page;
import org.xiaoyume.simpleDB.backend.dm.page.PageOne;
import org.xiaoyume.simpleDB.backend.dm.page.PageX;
import org.xiaoyume.simpleDB.backend.dm.pageIndex.PageIndex;
import org.xiaoyume.simpleDB.backend.dm.pageIndex.PageInfo;
import org.xiaoyume.simpleDB.backend.dm.pcache.PageCache;
import org.xiaoyume.simpleDB.backend.tm.TransactionManager;
import org.xiaoyume.simpleDB.backend.utils.Panic;
import org.xiaoyume.simpleDB.backend.utils.Types;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/18 21:39
 */
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager{
    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pageIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger Logger, TransactionManager tm){
        super(0);
        this.pc = pc;
        this.logger = Logger;
        this.tm = tm;
        this.pageIndex = new PageIndex();
    }
    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl dataItem = null;
        try{
            dataItem = (DataItemImpl) super.get(uid);
        }catch (Exception e){
            throw e;
        }
        if(!dataItem.isValid()){
            dataItem.release();
            return null;
        }
        return dataItem;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        //数据打宝成dataitem格式
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if(raw.length > PageX.MAX_FREE_SPACE){
            throw new RuntimeException("data too long");
        }

        PageInfo pageInfo = null;
        for(int i = 0; i < 5; i++){
            pageInfo = pageIndex.select(raw.length);
            if(pageInfo != null){
                break;
            }else{
                int newPageNo = pc.newPage(PageX.initRaw());
                //页索引，页号和空闲空间
                pageIndex.add(newPageNo, PageX.MAX_FREE_SPACE);
            }
        }
        if(pageInfo == null){
            throw new RuntimeException("database is busy");
        }

        Page pg = null;
        int freeSpace = 0;
        try{
            try{
                pg = pc.getPage(pageInfo.pageNo);
            }catch (Exception e){
                throw e;
            }

            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);

            short offset = PageX.insert(pg, raw);

            pg.release();
            return Types.addressToUid(pageInfo.pageNo, offset);
        }finally {
            //将取出的page插入到pageindex
            if(pg != null){
                pageIndex.add(pageInfo.pageNo, PageX.getFreeSpace(pg));
            }else{
                pageIndex.add(pageInfo.pageNo, freeSpace);
            }
        }


    }

    @Override
    public void close() {
        super.close();
        logger.close();
        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }
    public void logDataItem(long xid, DataItem dataItem){
        byte[] log = Recover.updateLog(xid, dataItem);
        logger.log(log);
    }
    public void releaseDataItem(DataItem dataItem){
        super.release(dataItem.getUid());
    }
    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short)(uid & ((1 << 16) - 1));
        uid >>>= 32;
        int pageNo = (int)(uid & ((1 << 32) - 1));
        Page page = null;
        try{
            page = pc.getPage(pageNo);
        }catch (Exception e){
            throw e;
        }
        return DataItem.parseDataItem(page, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem dataItem) {
        dataItem.page().release();
    }

    /**
     * 创建文件时初始化PageOne
     */
    void initPageOne(){
        int pageNo = pc.newPage(PageOne.InitRaw());
        assert pageNo == 1;
        try{
            pageOne = pc.getPage(pageNo);
        }catch (Exception e){
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    boolean loadCheckPageOne(){
        try{
            pageOne = pc.getPage(1);
        }catch (Exception e){
            Panic.panic(e);
        }
        //页面的有效性检查，两个字段匹配
        return PageOne.checkVc(pageOne);
    }

    void fillPageIndex(){
        int pageNo = pc.getPageNumber();
        for(int i = 2; i <= pageNo; i++){
            Page page = null;
            try{
                page = pc.getPage(i);
            }catch (Exception e){
                Panic.panic(e);
            }
            pageIndex.add(page.getPageNumber(), PageX.getFreeSpace(page));
            page.release();
        }
    }

}
