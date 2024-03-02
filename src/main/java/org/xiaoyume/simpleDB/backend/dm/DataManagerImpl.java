package org.xiaoyume.simpleDB.backend.dm;

import org.xiaoyume.simpleDB.backend.common.AbstractCache;
import org.xiaoyume.simpleDB.common.Error;
import org.xiaoyume.simpleDB.backend.dm.dataItem.DataItem;
import org.xiaoyume.simpleDB.backend.dm.dataItem.DataItemImpl;
import org.xiaoyume.simpleDB.backend.dm.logger.Logger;
import org.xiaoyume.simpleDB.backend.dm.page.Page;
import org.xiaoyume.simpleDB.backend.dm.page.PageOne;
import org.xiaoyume.simpleDB.backend.dm.page.PageX;
import org.xiaoyume.simpleDB.backend.dm.pageIndex.PageIndex;
import org.xiaoyume.simpleDB.backend.dm.pageIndex.PageInfo;
import org.xiaoyume.simpleDB.backend.dm.pageCache.PageCache;
import org.xiaoyume.simpleDB.backend.tm.TransactionManager;
import org.xiaoyume.simpleDB.backend.utils.Panic;
import org.xiaoyume.simpleDB.backend.utils.Types;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 关联事务，页缓存，日志，页信息
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
        DataItemImpl dataItem = (DataItemImpl) super.get(uid);

        if(!dataItem.isValid()){
            dataItem.release();
            return null;
        }
        return dataItem;
    }

    /**
     * 根据事务id和数据，插入数据
     * @param xid
     * @param data
     * @return
     * @throws Exception
     */
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        //数据打宝成dataitem格式
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if(raw.length > PageX.MAX_FREE_SPACE){
            throw Error.DataTooLargeException;
        }

        PageInfo pageInfo = null;
        for(int i = 0; i < 5; i++){
            //获取空闲页
            pageInfo = pageIndex.select(raw.length);
            if(pageInfo != null){
                break;
            }else{//如果没有找到空闲页面，就新建页面
                int newPageNo = pc.newPage(PageX.initRaw());
                //页索引，页号和空闲空间
                pageIndex.add(newPageNo, PageX.MAX_FREE_SPACE);
            }
        }
        if(pageInfo == null){
            throw Error.DatabaseBusyException;
        }

        Page pg = null;
        int freeSpace = 0;
        try{
            pg = pc.getPage(pageInfo.pageNo);

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

    /**
     * 更新数据，写入日志
     * @param xid
     * @param dataItem
     */
    public void logDataItem(long xid, DataItem dataItem){
        byte[] log = Recover.updateLog(xid, dataItem);
        logger.log(log);
    }
    public void releaseDataItem(DataItem dataItem){
        super.release(dataItem.getUid());
    }

    /**
     * uid是pageNO和offset运算的得来得
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    protected DataItem getForCache(long uid) throws Exception {
        //uid的低16位就是offset, 与运算提取出低16的1值就是offset
        short offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pageNo = (int)(uid & ((1L << 32) - 1));
        Page page = pc.getPage(pageNo);;

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
