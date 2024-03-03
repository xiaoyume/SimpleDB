package org.xiaoyume.simpleDB.backend.tbm;

import org.xiaoyume.simpleDB.backend.dm.DataManager;
import org.xiaoyume.simpleDB.backend.parser.statement.*;
import org.xiaoyume.simpleDB.backend.utils.Parser;
import org.xiaoyume.simpleDB.backend.vm.VersionManager;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/29 21:06
 */
public interface TableManager {
    BeginRes begin(Begin begin);

    byte[] commit(long xid) throws Exception;

    byte[] abort(long xid);

    byte[] show(long xid);

    byte[] create(long xid, Create create) throws Exception;

    byte[] insert(long xid, Insert insert) throws Exception;

    byte[] read(long xid, Select select) throws Exception;

    byte[] update(long xid, Update update) throws Exception;

    byte[] delete(long xid, Delete delete) throws Exception;

    /**
     * 创建表管理
     *
     * @param path
     * @param vm
     * @param dm
     * @return
     */
    public static TableManager create(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.create(path);
        booter.update(Parser.long2Byte(0));
        return new TableManagerImpl(vm, dm, booter);
    }

    public static TableManager open(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm, dm, booter);
    }
}
