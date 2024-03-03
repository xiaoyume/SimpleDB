package org.xiaoyume.simpleDB.backend.dm.page;

import org.xiaoyume.simpleDB.backend.dm.pageCache.PageCache;
import org.xiaoyume.simpleDB.backend.utils.Parser;

import java.util.Arrays;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 普通页
 * 页结构：
 * [freespaceoffset][data]
 * freespaceoffset: 2 bytes 空闲位置开始偏移
 * @date 2024/2/18 21:03
 */
public class PageX {
    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        //开始的空闲位置偏移是2
        setFSO(raw, OF_DATA);
        return raw;
    }

    /**
     * 设置空闲位置偏移
     *
     * @param raw
     * @param offset
     */
    private static void setFSO(byte[] raw, short offset) {
        System.arraycopy(Parser.short2Byte(offset), 0, raw, OF_FREE, OF_DATA);
    }

    /**
     * 获取page的空闲offset可用空间
     *
     * @param page
     * @return
     */
    public static short getFSO(Page page) {
        return getFSO(page.getData());
    }

    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    /**
     * 把raw插入到page中，返回插入位置末尾
     */
    public static short insert(Page page, byte[] raw) {
        page.setDirty(true);
        short offset = getFSO(page.getData());
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
        setFSO(page.getData(), (short) (offset + raw.length));
        return offset;
    }

    /**
     * 获取页面空闲空间大小
     */
    public static int getFreeSpace(Page page) {
        return PageCache.PAGE_SIZE - (int) getFSO(page.getData());
    }

    /**
     * 将raw插入到page中的offset位置，把page的offset设置为大的offset
     */
    public static void recoverInsert(Page page, byte[] raw, short offset) {
        page.setDirty(true);
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);

        short rawFSO = getFSO(page.getData());
        if (rawFSO < offset + raw.length) {
            setFSO(page.getData(), (short) (offset + raw.length));
        }
    }

    /**
     * 把raw插入到page的offset，并更新(数据量不变，没有改变占用空间大小)
     */
    public static void recoverUpdate(Page page, byte[] raw, short offset) {
        page.setDirty(true);
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
    }
}
