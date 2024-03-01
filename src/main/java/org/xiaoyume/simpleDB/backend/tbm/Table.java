package org.xiaoyume.simpleDB.backend.tbm;

import com.google.common.primitives.Bytes;
import org.xiaoyume.simpleDB.backend.common.Error;
import org.xiaoyume.simpleDB.backend.parser.statement.*;
import org.xiaoyume.simpleDB.backend.tm.TransactionManagerImpl;
import org.xiaoyume.simpleDB.backend.utils.ParseStringRes;
import org.xiaoyume.simpleDB.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 表结构
 * [tablename][nexttable]
 *
 * [field1uid][field2uid]  [fieldNuid]
 * @date 2024/2/29 21:12
 */
public class Table {
    TableManager tbm;//表管理器，用于管理表的创建，加载等操作
    long uid;//表的唯一标识符
    String name;//表名
    byte status;//表状态位
    long nextUid;//下一个表的uid
    List<Field> fields = new ArrayList<>();//表的字段列表

    /**
     * 加载表
     * @param tbm
     * @param uid
     * @return
     * @throws Exception
     */
    public static Table loadTable(TableManager tbm, long uid) throws Exception {
        byte[] raw = ((TableManagerImpl)tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        assert raw != null;
        Table tb = new Table(tbm, uid);
        return tb.parseSelf(raw);
    }

    /**
     * 创建表
     * @param tbm
     * @param nextUid
     * @param xid
     * @param create
     * @return
     * @throws Exception
     */
    public static Table createTable(TableManager tbm, long nextUid, long xid, Create create) throws Exception {
        Table tb = new Table(tbm, create.tableName, nextUid);
        for(int i = 0; i < create.fieldName.length; i ++) {
            String fieldName = create.fieldName[i];
            String fieldType = create.fieldType[i];
            boolean indexed = false;
            for(int j = 0; j < create.index.length; j ++) {
                //字段名和索引一致了，索引该字段
                if(fieldName.equals(create.index[j])) {
                    indexed = true;
                    break;
                }
            }
            tb.fields.add(Field.createField(tb, xid, fieldName, fieldType, indexed));
        }

        return tb.persistSelf(xid);
    }

    public Table(TableManager tbm, long uid) {
        this.tbm = tbm;
        this.uid = uid;
    }

    public Table(TableManager tbm, String tableName, long nextUid) {
        this.tbm = tbm;
        this.name = tableName;
        this.nextUid = nextUid;
    }

    /**
     * 解析成表
     * @param raw
     * @return
     */
    private Table parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);
        name = res.str;
        position += res.next;
        nextUid = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
        position += 8;

        while(position < raw.length) {
            long uid = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
            position += 8;
            fields.add(Field.loadField(this, uid));
        }
        return this;
    }

    /**
     * 存储起来
     * [tablename][nextuid][0][fielduid]...[0][fielduid]
     * @param xid
     * @return
     * @throws Exception
     */
    private Table persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(name);
        byte[] nextRaw = Parser.long2Byte(nextUid);
        byte[] fieldRaw = new byte[0];
        for(Field field : fields) {
            fieldRaw = Bytes.concat(fieldRaw, Parser.long2Byte(field.uid));
        }
        uid = ((TableManagerImpl)tbm).vm.insert(xid, Bytes.concat(nameRaw, nextRaw, fieldRaw));
        return this;
    }

    /**
     *
     * @param xid
     * @param delete
     * @return
     * @throws Exception
     */
    public int delete(long xid, Delete delete) throws Exception {
        List<Long> uids = parseWhere(delete.where);
        int count = 0;
        for (Long uid : uids) {
            if(((TableManagerImpl)tbm).vm.delete(xid, uid)) {
                count ++;
            }
        }
        return count;
    }

    /**
     * 用于更新表中符合指定条件的数据
     * @param xid
     * @param update
     * @return
     * @throws Exception
     */
    public int Update(long xid, Update update) throws Exception {
        //要更新的uid
        List<Long> uids = parseWhere(update.where);
        Field fd = null;
        //遍历查找字段
        for (Field f : fields) {
            if(f.fieldName.equals(update.fieldName)) {
                fd = f;
                break;
            }
        }
        if(fd == null) {
            throw Error.FieldNotFoundException;
        }
        Object value = fd.string2Value(update.value);
        int count = 0;
        for (Long uid : uids) {
            byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid);
            if(raw == null) continue;
            //删除
            ((TableManagerImpl)tbm).vm.delete(xid, uid);
            //解析键值对，更新value
            Map<String, Object> entry = parseEntry(raw);
            entry.put(fd.fieldName, value);
            //还原为raw
            raw = entry2Raw(entry);
            //插入
            long uuid = ((TableManagerImpl)tbm).vm.insert(xid, raw);

            count ++;
            //更新索引
            for (Field field : fields) {
                if(field.isIndexed()) {
                    field.insert(entry.get(field.fieldName), uuid);
                }
            }
        }
        return count;
    }

    /**
     * 读取表中符合指定条件的数据
     * @param xid
     * @param read
     * @return
     * @throws Exception
     */
    public String read(long xid, Read read) throws Exception {
        List<Long> uids = parseWhere(read.where);
        StringBuilder sb = new StringBuilder();
        for (Long uid : uids) {
            byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid);
            if(raw == null) continue;
            Map<String, Object> entry = parseEntry(raw);
            sb.append(printEntry(entry)).append("\n");
        }
        return sb.toString();
    }

    /**
     * 解析删除更新读取操作的条件
     * @param where
     * @return
     * @throws Exception
     */
    private List<Long> parseWhere(Where where) throws Exception {
        return null;
    }

    private String printEntry(Map<String, Object> entry) {
        return null;
    }

    private Map<String, Object> parseEntry(byte[] raw) {
        return null;
    }

    /**
     * 取出字段的值，打包成字节数组
     * @param entry 字段名，数据
     * @return
     */
    private byte[] entry2Raw(Map<String, Object> entry) {
        byte[] raw = new byte[0];
        for (Field field : fields) {
            raw = Bytes.concat(raw, field.value2Raw(entry.get(field.fieldName)));
        }
        return raw;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        sb.append(name).append(": ");
        for(Field field : fields) {
            sb.append(field.toString());
            if(field == fields.get(fields.size()-1)) {
                sb.append("}");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
