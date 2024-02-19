package org.xiaoyume.simpleDB.backend.dm;

import com.google.common.primitives.Bytes;
import org.xiaoyume.simpleDB.backend.common.SubArray;
import org.xiaoyume.simpleDB.backend.dm.dataItem.DataItem;
import org.xiaoyume.simpleDB.backend.dm.logger.Logger;
import org.xiaoyume.simpleDB.backend.dm.page.Page;
import org.xiaoyume.simpleDB.backend.dm.page.PageX;
import org.xiaoyume.simpleDB.backend.dm.pcache.PageCache;
import org.xiaoyume.simpleDB.backend.tm.TransactionManager;
import org.xiaoyume.simpleDB.backend.utils.Panic;
import org.xiaoyume.simpleDB.backend.utils.Parser;
import java.util.Map.Entry;

import java.util.*;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/19 15:49
 */
public class Recover {
    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    private static final int REDO = 0;
    private static final int UNDO = 1;

    static class InsertLogInfo{
        long xid;
        int pageNo;
        short offset;
        byte[] raw;
    }
    static class UpdateLogInfo{
        long xid;
        int pageNo;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    public static void recover(TransactionManager tm, Logger logger, PageCache pageCache){
        System.out.println("Recovering ...........");
        logger.rewind();

        int maxPageNo = 0;
        while(true){
            //获取下一条日志
            byte[] log = logger.next();
            if(log == null) break;
            int pageNo;
            if(isInsertLog(log)){
                InsertLogInfo insertLogInfo = parseInsertLog(log);
                pageNo = insertLogInfo.pageNo;
            }else{
                UpdateLogInfo updateLogInfo = parseUpdateLog(log);
                pageNo = updateLogInfo.pageNo;
            }
            if(pageNo > maxPageNo){
                maxPageNo = pageNo;
            }
        }
        if(maxPageNo == 0){
            maxPageNo = 1;
        }
        pageCache.truncateByBgno(maxPageNo);
        System.out.println("Trucate to " + maxPageNo + " pages.");

        redoTransactions(tm, logger, pageCache);
        System.out.println("Redo transactions over.");

        undoTransactions(tm, logger, pageCache);
        System.out.println("Undo transactions over.");

        System.out.println("Recover over.");
    }

    /**
     * 根据log redo恢复
     * @param transactionManager
     * @param logger
     * @param pageCache
     */
    private static void redoTransactions(TransactionManager transactionManager, Logger logger, PageCache pageCache){
        logger.rewind();
        while(true){
            byte[] log = logger.next();
            if(log == null) break;
            if(isInsertLog(log)){
                InsertLogInfo insertLogInfo = parseInsertLog(log);
                long xid = insertLogInfo.xid;
                if(!transactionManager.isActive(xid)){
                    doInsertLog(pageCache, log, REDO);
                }
            }else{
                UpdateLogInfo updateLogInfo = parseUpdateLog(log);
                long xid = updateLogInfo.xid;
                if(!transactionManager.isActive(xid)){
                    doUpdateLog(pageCache, log, REDO);
                }
            }
        }
    }

    private static void undoTransactions(TransactionManager transactionManager, Logger logger, PageCache pageCache){
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        logger.rewind();
        while(true){
            byte[] log = logger.next();
            if(log == null) break;
            if(isInsertLog(log)){
                InsertLogInfo insertLogInfo = parseInsertLog(log);
                long xid = insertLogInfo.xid;
                if(transactionManager.isActive(xid)){
                    if(!logCache.containsKey(xid)){
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }else{
                UpdateLogInfo updateLogInfo = parseUpdateLog(log);
                long xid = updateLogInfo.xid;
                if(transactionManager.isActive(xid)){
                    if(!logCache.containsKey(xid)){
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }

        //对所有的active log进行倒序undo
        for(Entry<Long, List<byte[]>> entry : logCache.entrySet()){
            List<byte[]> logs = entry.getValue();
            for(int i = logs.size() - 1; i >= 0; i--){
                byte[] log = logs.get(i);
                if(isInsertLog(log)){
                    doInsertLog(pageCache, log, UNDO);
                }else{
                    doUpdateLog(pageCache, log, UNDO);
                }
            }
            transactionManager.abort(entry.getKey());
        }
    }

    private static boolean isInsertLog(byte[] log){
        return log[0] == LOG_TYPE_INSERT;
    }

    //[logtype][xid][uid][oldraw][newraw]
    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE + 1;
    private static final int OF_UPDATE_UID = OF_XID + 8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;

    /**
     * 生成update日志
     * @param xid
     * @param dataItem
     * @return
     */
    public static byte[] updateLog(long xid, DataItem dataItem){
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(dataItem.getUid());
        byte[] oldRaw = dataItem.getOldRaw();
        SubArray raw = dataItem.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    /**
     * 根据日志解析出update日志信息
     * @param log
     * @return
     */
    private static UpdateLogInfo parseUpdateLog(byte[] log){
        UpdateLogInfo updateLogInfo = new UpdateLogInfo();
        updateLogInfo.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        updateLogInfo.offset = (short) (uid & ((1 << 16) - 1));
        uid >>>= 32;
        updateLogInfo.pageNo = (int) (uid & ((1 << 32) - 1));
        //分两半，前面存旧数据，
        int length = (log.length - OF_UPDATE_RAW) / 2;
        updateLogInfo.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW + length);
        updateLogInfo.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW + length, OF_UPDATE_RAW + length*2);
        return updateLogInfo;
    }

    private static void doUpdateLog(PageCache pageCache, byte[] log, int flag){
        int pageNo;
        short offset;
        byte[] raw;
        if(flag == REDO){
            UpdateLogInfo updateLogInfo = parseUpdateLog(log);
            pageNo = updateLogInfo.pageNo;
            offset = updateLogInfo.offset;
            raw = updateLogInfo.newRaw;
        }else{
            UpdateLogInfo updateLogInfo = parseUpdateLog(log);
            pageNo = updateLogInfo.pageNo;
            offset = updateLogInfo.offset;
            raw = updateLogInfo.oldRaw;
        }

        Page page = null;
        try{
            page = pageCache.getPage(pageNo);
        }catch (Exception e){
            Panic.panic(e);
        }
        try{
            PageX.recoverUpdate(page, raw, offset);
        }finally {
            page.release();
        }
    }

    //插入日志
    //[logtype][xid][pageno][offset][raw]
    private static final int OF_INSERT_PAGENO = OF_XID + 8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PAGENO + 4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;

    public static byte[] insertLog(long xid, Page page, byte[] raw){
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pageNoRaw = Parser.int2Byte(page.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(page));
        return Bytes.concat(logTypeRaw, xidRaw, pageNoRaw, offsetRaw, raw);
    }

    /**
     * 解析处insert日志信息
     * @param log
     * @return
     */
    private static InsertLogInfo parseInsertLog(byte[] log){
        InsertLogInfo insertLogInfo = new InsertLogInfo();
        insertLogInfo.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PAGENO));
        insertLogInfo.pageNo = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PAGENO, OF_INSERT_OFFSET));
        insertLogInfo.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        insertLogInfo.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return insertLogInfo;
    }

    /**
     * 执行insert日志
     * @param pageCache
     * @param log
     * @param flag
     */
    private static void doInsertLog(PageCache pageCache, byte[] log, int flag){
        InsertLogInfo insertLogInfo = parseInsertLog(log);
        Page page = null;
        try{
            page = pageCache.getPage(insertLogInfo.pageNo);
        }catch (Exception e){
            Panic.panic(e);
        }
        try{
            if(flag == UNDO) {
                DataItem.setDataItemRawInvalid(insertLogInfo.raw);
            }
            PageX.recoverInsert(page, insertLogInfo.raw, insertLogInfo.offset);
        }finally {
            page.release();
        }
    }
}
