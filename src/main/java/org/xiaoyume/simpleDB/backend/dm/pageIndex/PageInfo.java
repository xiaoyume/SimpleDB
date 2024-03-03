package org.xiaoyume.simpleDB.backend.dm.pageIndex;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 页面信息索引，页号加剩余空间
 * @date 2024/2/18 20:12
 */
public class PageInfo {
    public int pageNo;
    public int freeSpace;

    public PageInfo(int pageNo, int freeSpace) {
        this.pageNo = pageNo;
        this.freeSpace = freeSpace;
    }
}
