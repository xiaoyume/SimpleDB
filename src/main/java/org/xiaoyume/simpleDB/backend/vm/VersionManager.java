package org.xiaoyume.simpleDB.backend.vm;

import javax.swing.text.StyledEditorKit;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/26 19:58
 */
public interface VersionManager {
    byte[] read(long xid, long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    boolean delete(long xid, long uid) throws Exception;
    long begin(int level);
    void commit(long xid) throws Exception;
    void abort(long xid) throws Exception;
}
