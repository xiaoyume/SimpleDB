package org.xiaoyume.simpleDB.backend.tbm;

import com.google.common.primitives.Bytes;
import org.xiaoyume.simpleDB.common.Error;
import org.xiaoyume.simpleDB.backend.parser.statement.*;
import org.xiaoyume.simpleDB.backend.tm.TransactionManagerImpl;
import org.xiaoyume.simpleDB.backend.utils.Panic;
import org.xiaoyume.simpleDB.backend.utils.ParseStringRes;
import org.xiaoyume.simpleDB.backend.utils.Parser;
import org.xiaoyume.simpleDB.backend.tbm.Field.*;

import java.util.*;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 表结构
 * [tablename][nexttable]
 * <p>
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
     *
     * @param tbm
     * @param uid
     * @return
     * @throws Exception
     */
    public static Table loadTable(TableManager tbm, long uid) {
        byte[] raw = null;
        try {
            raw = ((TableManagerImpl) tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }

        assert raw != null;
        Table tb = new Table(tbm, uid);
        return tb.parseSelf(raw);
    }

    /**
     * 创建表
     *
     * @param tbm
     * @param nextUid
     * @param xid
     * @param create
     * @return
     * @throws Exception
     */
    public static Table createTable(TableManager tbm, long nextUid, long xid, Create create) throws Exception {
        Table tb = new Table(tbm, create.tableName, nextUid);
        for (int i = 0; i < create.fieldName.length; i++) {
            String fieldName = create.fieldName[i];
            String fieldType = create.fieldType[i];
            boolean indexed = false;
            for (int j = 0; j < create.index.length; j++) {
                //字段名和索引一致了，索引该字段
                if (fieldName.equals(create.index[j])) {
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
     *
     * @param raw
     * @return
     */
    private Table parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);
        name = res.str;
        position += res.next;
        nextUid = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
        position += 8;

        while (position < raw.length) {
            long uid = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
            position += 8;
            fields.add(Field.loadField(this, uid));
        }
        return this;
    }

    /**
     * 存储起来
     * [tablename][nextuid][0][fielduid]...[0][fielduid]
     *
     * @param xid
     * @return
     * @throws Exception
     */
    private Table persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(name);
        byte[] nextRaw = Parser.long2Byte(nextUid);
        byte[] fieldRaw = new byte[0];
        for (Field field : fields) {
            fieldRaw = Bytes.concat(fieldRaw, Parser.long2Byte(field.uid));
        }
        uid = ((TableManagerImpl) tbm).vm.insert(xid, Bytes.concat(nameRaw, nextRaw, fieldRaw));
        return this;
    }

    /**
     * @param xid
     * @param delete
     * @return
     * @throws Exception
     */
    public int delete(long xid, Delete delete) throws Exception {
        List<Long> uids = parseWhere(delete.where);
        int count = 0;
        for (Long uid : uids) {
            if (((TableManagerImpl) tbm).vm.delete(xid, uid)) {
                count++;
            }
        }
        return count;
    }

    /**
     * 用于更新表中符合指定条件的数据
     *
     * @param xid
     * @param update
     * @return
     * @throws Exception
     */
    public int update(long xid, Update update) throws Exception {
        //要更新的uid
        List<Long> uids = parseWhere(update.where);
        Field fd = null;
        //遍历查找字段
        for (Field f : fields) {
            if (f.fieldName.equals(update.fieldName)) {
                fd = f;
                break;
            }
        }
        if (fd == null) {
            throw Error.FieldNotFoundException;
        }
        Object value = fd.string2Value(update.value);
        int count = 0;
        for (Long uid : uids) {
            byte[] raw = ((TableManagerImpl) tbm).vm.read(xid, uid);
            if (raw == null) continue;
            //删除
            ((TableManagerImpl) tbm).vm.delete(xid, uid);
            //解析键值对，更新value
            Map<String, Object> entry = parseEntry(raw);
            entry.put(fd.fieldName, value);
            //还原为raw
            raw = entry2Raw(entry);
            //插入
            long uuid = ((TableManagerImpl) tbm).vm.insert(xid, raw);

            count++;
            //更新索引
            for (Field field : fields) {
                if (field.isIndexed()) {
                    field.insert(entry.get(field.fieldName), uuid);
                }
            }
        }
        return count;
    }

    /**
     * 读取表中符合指定条件的数据
     *
     * @param xid
     * @param select
     * @return
     * @throws Exception
     */
    public String read(long xid, Select select) throws Exception {
        List<Long> uids = parseWhere(select.where);
        StringBuilder sb = new StringBuilder();
        for (Long uid : uids) {
            byte[] raw = ((TableManagerImpl) tbm).vm.read(xid, uid);
            if (raw == null) continue;
            Map<String, Object> entry = parseEntry(raw);
            sb.append(printEntry(entry)).append("\n");
        }
        return sb.toString();
    }

    /**
     * 向表中插入数据
     *
     * @param xid
     * @param insert
     * @throws Exception
     */
    public void insert(long xid, Insert insert) throws Exception {
        Map<String, Object> entry = string2Entry(insert.values);
        byte[] raw = entry2Raw(entry);
        //插入并得到数据uid
        long uid = ((TableManagerImpl) tbm).vm.insert(xid, raw);
        for (Field field : fields) {
            if (field.isIndexed()) {
                //插到b+树索引里
                field.insert(entry.get(field.fieldName), uid);
            }
        }
    }

    /**
     * values数组转成键值对，values插入的值和字段的顺序一致， 一一对应
     *
     * @return
     * @throws Exception
     */
    private Map<String, Object> string2Entry(String[] values) throws Exception {
        if (values.length != fields.size()) {
            throw Error.InvalidValuesException;
        }
        Map<String, Object> entry = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            Object v = f.string2Value(values[i]);
            entry.put(f.fieldName, v);
        }
        return entry;
    }

    /**
     * 解析删除更新读取操作的条件
     *
     * @param where
     * @return
     * @throws Exception
     */
    private List<Long> parseWhere(Where where) throws Exception {
        //查询条件边界值
        long l0 = 0, r0 = 0, l1 = 0, r1 = 0;
        boolean single = false;
        Field fd = null;
        //where为空，表示没有明确的查询条件
        if (where == null) {
            //遍历表中所有字段，如果某个字段有索引，作为查询条件字段
            for (Field field : fields) {
                if (field.isIndexed()) {
                    fd = field;
                    break;
                }
            }
            //左右边界设置为最大
            l0 = 0;
            r0 = Long.MAX_VALUE;
            single = true;
        } else {
            //where不空，遍历找到查询条件匹配的字段
            for (Field field : fields) {
                if (field.fieldName.equals(where.singleExp1.field)) {
                    if (!field.isIndexed()) {
                        throw Error.FieldNotIndexedException;
                    }
                    fd = field;
                    break;
                }
            }
            if (fd == null) {
                throw Error.FieldNotFoundException;
            }
            //计算左右边界
            CalWhereRes res = calWhere(fd, where);
            l0 = res.l0;
            r0 = res.r0;
            l1 = res.l1;
            r1 = res.r1;
            single = res.single;
        }
        //字段范围查找，b+树查找
        List<Long> uids = fd.search(l0, r0);
        if (!single) {
            List<Long> tmp = fd.search(l1, r1);
            uids.addAll(tmp);
        }
        return uids;
    }

    class CalWhereRes {
        long l0, r0, l1, r1;
        boolean single;
    }

    /**
     * 根据字段（b+树索引）和查询条件计算出对应的边界值
     *
     * @param fd
     * @param where
     * @return
     * @throws Exception
     */
    private CalWhereRes calWhere(Field fd, Where where) throws Exception {
        CalWhereRes res = new CalWhereRes();
        switch (where.logicOp) {
            case ""://空, 单一条件， 就只比较 第一个表达式
                res.single = true;
                FieldCalRes r = fd.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                break;
            case "or"://or, 两个表达式都满足
                res.single = false;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left;
                res.r1 = r.right;
                break;
            case "and"://and可以合并为单个查询,lo,ro表示边界     < 10 and > 5  (0,10)(5,max)->(5,10)
                res.single = true;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left;
                res.r1 = r.right;
                if (res.l1 > res.l0) res.l0 = res.l1;
                if (res.r1 < res.r0) res.r0 = res.r1;
                break;
            default:
                throw Error.InvalidLogOpException;
        }
        return res;
    }

    /**
     * @param entry 字段名 -> 数据
     * @return
     */
    private String printEntry(Map<String, Object> entry) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < fields.size(); i++) {
            //取字段
            Field field = fields.get(i);
            //取字段对应的值
            sb.append(field.printValue(entry.get(field.fieldName)));
            if (i == fields.size() - 1) {
                sb.append("]");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    /**
     * 字节数据解析出字段，数据的映射
     *
     * @param raw
     * @return
     */
    private Map<String, Object> parseEntry(byte[] raw) {
        int pos = 0;
        Map<String, Object> entry = new HashMap<>();
        for (Field field : fields) {
            ParseValueRes r = field.parserValue(Arrays.copyOfRange(raw, pos, raw.length));
            entry.put(field.fieldName, r.v);
            pos += r.shift;
        }
        return entry;
    }

    /**
     * 取出字段的值，打包成字节数组
     *
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
        for (Field field : fields) {
            sb.append(field.toString());
            if (field == fields.get(fields.size() - 1)) {
                sb.append("}");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
