package org.xiaoyume.simpleDB.backend.tbm;

import org.xiaoyume.simpleDB.common.Error;
import org.xiaoyume.simpleDB.backend.dm.DataManager;
import org.xiaoyume.simpleDB.backend.parser.statement.*;
import org.xiaoyume.simpleDB.backend.utils.Parser;
import org.xiaoyume.simpleDB.backend.vm.VersionManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 管理数据库中的表，包括表的创建、插入、读取、更新、删除等操作
 * @date 2024/2/29 21:06
 */
public class TableManagerImpl implements TableManager{
    //版本管理器
    VersionManager vm;
    //数据管理器
    DataManager dm;
    //启动器
    private Booter booter;
    //表缓存
    private Map<String, Table> tableCache;
    //事务-表缓存
    private Map<Long, List<Table>> xidTableCache;
    private Lock lock;

    TableManagerImpl(VersionManager vm, DataManager dm, Booter booter) {
        this.vm = vm;
        this.dm = dm;
        this.booter = booter;
        this.tableCache = new HashMap<>();
        this.xidTableCache = new HashMap<>();
        lock = new ReentrantLock();
        loadTables();
    }

    /**
     *加载所有的表到缓存里
     */
    private void loadTables() {
        long uid = firstTableUid();
        while(uid != 0) {
            Table tb = Table.loadTable(this, uid);
            uid = tb.nextUid;
            tableCache.put(tb.name, tb);
        }
    }

    /**
     * booter里取出第一个表的uid
     * @return
     */
    private long firstTableUid() {
        byte[] raw = booter.load();
        return Parser.parseLong(raw);
    }

    /**
     * 更新第一个表id
     * @param uid
     */
    private void updateFirstTableUid(long uid) {
        byte[] raw = Parser.long2Byte(uid);
        booter.update(raw);
    }

    /**
     * 事务启动
     * @param begin
     * @return
     */
    @Override
    public BeginRes begin(Begin begin) {
        BeginRes res = new BeginRes();
        //是否是可重复读级别
        int level = begin.isRepeatableRead?1:0;
        res.xid = vm.begin(level);
        res.result = "begin".getBytes();
        return res;
    }

    /**
     * 事务提交
     * @param xid
     * @return
     * @throws Exception
     */
    @Override
    public byte[] commit(long xid) throws Exception {
        vm.commit(xid);
        return "commit".getBytes();
    }

    /**
     * 事务回滚
     * @param xid
     * @return
     */
    @Override
    public byte[] abort(long xid) {
        vm.abort(xid);
        return "abort".getBytes();
    }

    /**
     * 所有表信息
     * @param xid
     * @return
     */
    @Override
    public byte[] show(long xid) {
        lock.lock();
        try {
            StringBuilder sb = new StringBuilder();
            for (Table tb : tableCache.values()) {
                sb.append(tb.toString()).append("\n");
            }
            List<Table> t = xidTableCache.get(xid);
            for (Table tb : t) {
                sb.append(tb.toString()).append("\n");
            }
            return sb.toString().getBytes();
        } finally {
            lock.unlock();
        }
    }

    /**
     *创建新表
     * @param xid
     * @param create
     * @return 返回一个create + 表名的字符的字节数组
     * @throws Exception
     */
    @Override
    public byte[] create(long xid, Create create) throws Exception {
        lock.lock();
        try {
            if(tableCache.containsKey(create.tableName)) {
                throw Error.DuplicatedTableException;
            }
            //新建一个表，就把新建表的uid放到第一个表
            Table table = Table.createTable(this, firstTableUid(), xid, create);
            updateFirstTableUid(table.uid);
            tableCache.put(create.tableName, table);
            if(!xidTableCache.containsKey(xid)) {
                xidTableCache.put(xid, new ArrayList<>());
            }
            xidTableCache.get(xid).add(table);
            return ("create " + create.tableName).getBytes();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 值插入到表中
     * @param xid
     * @param insert
     * @return
     * @throws Exception
     */
    @Override
    public byte[] insert(long xid, Insert insert) throws Exception {
        lock.lock();
        Table table = tableCache.get(insert.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        table.insert(xid, insert);
        return "insert".getBytes();
    }

    /**
     * 根据表名，字段和条件来读取数据
     * @param xid
     * @param read
     * @return
     * @throws Exception
     */
    @Override
    public byte[] read(long xid, Read read) throws Exception {
        lock.lock();
        Table table = tableCache.get(read.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        return table.read(xid, read).getBytes();
    }

    /**
     * 更新表中特定字段的值
     * @param xid
     * @param update
     * @return
     * @throws Exception
     */
    @Override
    public byte[] update(long xid, Update update) throws Exception {
        lock.lock();
        Table table = tableCache.get(update.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.update(xid, update);
        return ("update " + count).getBytes();
    }

    /**
     * 删除
     * @param xid
     * @param delete
     * @return
     * @throws Exception
     */
    @Override
    public byte[] delete(long xid, Delete delete) throws Exception {
        lock.lock();
        Table table = tableCache.get(delete.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.delete(xid, delete);
        return ("delete " + count).getBytes();
    }
}
