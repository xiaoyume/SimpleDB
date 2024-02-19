package org.xiaoyume.simpleDB.backend.dm.dataItem;

import com.google.common.primitives.Bytes;
import org.xiaoyume.simpleDB.backend.common.SubArray;
import org.xiaoyume.simpleDB.backend.dm.DataManagerImpl;
import org.xiaoyume.simpleDB.backend.dm.page.Page;
import org.xiaoyume.simpleDB.backend.utils.Parser;
import org.xiaoyume.simpleDB.backend.utils.Types;

import java.util.Arrays;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/18 21:33
 */
public interface DataItem {
    SubArray data();
    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();

    public static byte[] wrapDataItemRaw(byte[] raw){
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short) raw.length);
        return Bytes.concat(valid, size, raw);
    }
    /**
     * 从页面的offset处解析dataItem
     */
    public static DataItem parseDataItem(Page page, short offset, DataManagerImpl dm){
        byte[] raw = page.getData();
        //获取dataitem size
        short size = Parser.parseShort(Arrays.copyOfRange(raw, DataItemImpl.OF_SIZE+offset, offset+DataItemImpl.OF_DATA));
        //dataitem占用长度
        short length = (short)(size + DataItemImpl.OF_DATA);
        long uid = Types.addressToUid(page.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset+length), new byte[length], page, uid, dm);
    }

    public static void setDataItemRawInvalid(byte[] raw){
        raw[DataItemImpl.OF_VALID] = (byte)1;
    }
}
