package org.xiaoyume.simpleDB.backend.tbm;

import com.google.common.primitives.Bytes;
import org.xiaoyume.simpleDB.backend.common.Error;
import org.xiaoyume.simpleDB.backend.im.BPlusTree;
import org.xiaoyume.simpleDB.backend.parser.statement.SingleExpression;
import org.xiaoyume.simpleDB.backend.tm.TransactionManagerImpl;
import org.xiaoyume.simpleDB.backend.utils.Panic;
import org.xiaoyume.simpleDB.backend.utils.ParseStringRes;
import org.xiaoyume.simpleDB.backend.utils.Parser;

import java.util.Arrays;
import java.util.List;

/**
 * @author xiaoy
 * @version 1.0
 * @description: filed表示字段信息
 * 格式
 * [fieldName][typename][indexuid]
 * 如果filed无索引，indexuid为0
 * @date 2024/2/29 21:12
 */
public class Field {
    //字段唯一标识符
    long uid;
    //字段所属表
    private Table tb;
    //字段名
    String fieldName;
    //字段类型 可以是 "int32"、"int64" 或 "string"。
    String fieldType;
    //字段索引
    private long index;
    private BPlusTree bt;

    /**
     * 加载字段信息，
     * @param tb
     * @param uid
     * @return
     */
    public static Field loadField(Table tb, long uid) {
        byte[] raw = null;
        try {
            raw = ((TableManagerImpl)tb.tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        return new Field(uid, tb).parseSelf(raw);
    }
    public Field(long uid, Table tb) {
        this.uid = uid;
        this.tb = tb;
    }
    public Field(Table tb, String fieldName, String fieldType, long index) {
        this.tb = tb;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.index = index;
    }

    /**
     * 解析字节数组，恢复字段信息
     * @param raw
     * @return
     */
    private Field parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);
        fieldName = res.str;
        position += res.next;
        res = Parser.parseString(Arrays.copyOfRange(raw, position, raw.length));
        fieldType = res.str;
        position += res.next;
        this.index = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
        if(index != 0) {
            try {
                bt = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm);
            } catch(Exception e) {
                Panic.panic(e);
            }
        }
        return this;
    }

    /**
     * 创建字段
     * @param tb
     * @param xid
     * @param fieldName
     * @param fieldType
     * @param indexed
     * @return
     * @throws Exception
     */
    public static Field createField(Table tb, long xid, String fieldName, String fieldType, boolean indexed) throws Exception {
        typeCheck(fieldType);
        Field f = new Field(tb, fieldName, fieldType, 0);
        if(indexed) {
            long index = BPlusTree.create(((TableManagerImpl)tb.tbm).dm);
            BPlusTree bt = BPlusTree.load(f.index, ((TableManagerImpl)tb.tbm).dm);
            f.index = index;
            f.bt = bt;
        }
        f.persistSelf(xid);
        return f;
    }

    /**
     * 存储字段
     * @param xid
     * @throws Exception
     */
    private void persistSelf(long xid) throws Exception {
        //长度+数据
        byte[] nameRaw = Parser.string2Byte(fieldName);
        //长度+数据
        byte[] typeRaw = Parser.string2Byte(fieldType);
        byte[] indexRaw = Parser.long2Byte(index);
        //存储之后才获得字段uid
        this.uid = ((TableManagerImpl)tb.tbm).vm.insert(xid, Bytes.concat(nameRaw, typeRaw, indexRaw));
    }

    /**
     * 检查字段是否合法
     * @param fieldType
     * @throws Exception
     */
    private static void typeCheck(String fieldType) throws Exception {
        if(!"int32".equals(fieldType) && !"int64".equals(fieldType) && !"string".equals(fieldType)) {
            throw Error.InvalidFieldException;
        }
    }

    public boolean isIndexed() {
        return index != 0;
    }

    /**
     * b+树索引中插入键值对
     * @param key
     * @param uid
     * @throws Exception
     */
    public void insert(Object key, long uid) throws Exception {
        //根据key获取uid
        long uKey = value2Uid(key);
        bt.insert(uKey, uid);
    }

    public List<Long> search(long left, long right) throws Exception {
        return bt.searchRange(left, right);
    }
    /**
     * 根据字段类型，将字符串转换为对应的值
     */
    public Object string2Value(String str){
        switch(fieldType) {
            case "int32":
                return Integer.parseInt(str);
            case "int64":
                return Long.parseLong(str);
            case "string":
                return str;
        }
        return null;
    }

    /**
     *
     * @param key
     * @return
     */
    public long value2Uid(Object key) {
        long uid = 0;
        switch(fieldType) {
            case "string":
                uid = Parser.str2Uid((String)key);
                break;
            default:
                uid = (long)key;
                break;
        }
        return uid;
    }

    public byte[] value2Raw(Object v) {
        byte[] raw = null;
        switch(fieldType) {
            case "int32":
                raw = Parser.int2Byte((int)v);
                break;
            case "int64":
                raw = Parser.long2Byte((long)v);
                break;
            case "string":
                raw = Parser.string2Byte((String)v);
                break;
        }
        return raw;
    }

    //存储解析结果
    class ParseValueRes {
        Object v;
        int shift;
    }

    /**
     *解析字段对应的value,如果是int32类型就取4个字节
     * @param raw
     * @return
     */
    public ParseValueRes parserValue(byte[] raw) {
        ParseValueRes res = new ParseValueRes();
        switch(fieldType) {
            case "int32":
                res.v = Parser.parseInt(Arrays.copyOf(raw, 4));
                res.shift = 4;
                break;
            case "int64":
                res.v = Parser.parseLong(Arrays.copyOf(raw, 8));
                res.shift = 8;
                break;
            case "string":
                ParseStringRes r = Parser.parseString(raw);
                res.v = r.str;
                res.shift = r.next;
                break;
        }
        return res;
    }

    public String printValue(Object v) {
        String str = null;
        switch(fieldType) {
            case "int32":
                str = String.valueOf((int)v);
                break;
            case "int64":
                str = String.valueOf((long)v);
                break;
            case "string":
                str = (String)v;
                break;
        }
        return str;
    }

    @Override
    public String toString() {
        return new StringBuilder("(")
                .append(fieldName)
                .append(", ")
                .append(fieldType)
                .append(index!=0?", Index":", NoIndex")
                .append(")")
                .toString();
    }

    /**
     * 计算单个表达式，返回计算结果
     * @param exp
     * @return
     * @throws Exception
     */
    public FieldCalRes calExp(SingleExpression exp) throws Exception {
        Object v = null;
        FieldCalRes res = new FieldCalRes();
        switch(exp.compareOp) {
            case "<":
                //右边界为表达式值对应的唯一标识符减 1。
                res.left = 0;
                v = string2Value(exp.value);
                res.right = value2Uid(v);
                if(res.right > 0) {
                    res.right --;
                }
                break;
            case "=":
                v = string2Value(exp.value);
                res.left = value2Uid(v);
                res.right = res.left;
                break;
            case ">":
                //左边界设为表达式值对应的唯一标识符加 1，右边界设为long的最大值
                res.right = Long.MAX_VALUE;
                v = string2Value(exp.value);
                res.left = value2Uid(v) + 1;
                break;
        }
        return res;
    }
}
