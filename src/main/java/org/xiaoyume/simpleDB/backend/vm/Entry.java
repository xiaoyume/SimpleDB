package org.xiaoyume.simpleDB.backend.vm;

import com.google.common.primitives.Bytes;
import org.xiaoyume.simpleDB.backend.common.SubArray;
import org.xiaoyume.simpleDB.backend.dm.dataItem.DataItem;
import org.xiaoyume.simpleDB.backend.utils.Parser;

import java.util.Arrays;

/**
 * @author xiaoy
 * @version 1.0
 * @description: vm版本控制,向上抽象出entry
 * entry结构, mvcc里的readview结构，xmin和xmax表示生成这个entry时活跃的事务id
 * [XMIN][XMAX][data]
 * @date 2024/2/26 19:58
 */
public class Entry {
    //偏移量
    private static final int OF_XMIN = 0;
    private static final int OF_XMAX = OF_XMIN + 8;
    private static final int OF_DATA = OF_XMAX + 8;
    private long uid;
    private DataItem dataItem;
    private VersionManager vm;
    public static Entry newEntry(VersionManager vm, DataItem dataItem, long uid){
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = dataItem;
        entry.vm = vm;
        return entry;
    }

    public static Entry loadEntry(VersionManager vm, long uid) throws Exception{
        DataItem dataItem = ((VersionManagerImpl)vm).dataManager.read(uid);
        return newEntry(vm, dataItem, uid);
    }

    /**
     * 数据打包成entry结构
     * @param xid
     * @param data
     * @return
     */
    public static byte[] wrapEntryRaw(long xid, byte[] data){
        byte[] xmin = Parser.long2Byte(xid);
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin, xmax, data);
    }
    public void release(){
        ((VersionManagerImpl)vm).releaseEntry(this);
    }
    public void remove(){
        dataItem.release();
    }
    public byte[] data(){
        dataItem.rLock();
        try{
            SubArray sa = dataItem.data();
            byte[] data = new byte[sa.end - sa.start - OF_DATA];
            System.arraycopy(sa.raw, sa.start+OF_DATA, data, 0, data.length);
            return data;
        }finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 获取xmin的值
     * @return
     */
    public long getXmin(){
        dataItem.rLock();
        try{
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start+OF_XMIN, sa.start+OF_XMAX));
        }finally {
            dataItem.rUnLock();
        }
    }
    public long getXmax(){
        dataItem.rLock();
        try{
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start+OF_XMAX, sa.start+OF_DATA));
        }finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 设置xmax的值
     * @param xid
     */
    public void setXmax(long xid){
        dataItem.before();
        try{
            SubArray sa = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid), 0, sa.raw, sa.start+OF_XMAX, 8);
        }finally {
            dataItem.after(xid);
        }
    }

    public long getUID(){
        return uid;
    }
}
