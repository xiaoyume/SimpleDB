package org.xiaoyume.simpleDB.backend.parser;

import org.xiaoyume.simpleDB.backend.common.Error;
import org.xiaoyume.simpleDB.backend.parser.statement.*;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/3/2 13:28
 */
public class Parser {
    /**
     * 输入一个sql语句字节数组，解析成对应的语句对象
     * @param statement
     * @return
     * @throws Exception
     */
    public static Object Parse(byte[] statement) throws Exception {
        Tokenizer tokenizer = new Tokenizer(statement);
        //先解析一个token，看看是哪种类型，然后根据类型解析
        String token = tokenizer.peek();
        tokenizer.pop();

        Object stat = null;
        Exception statErr = null;
        try {
            //sql
            switch(token) {
                case "begin":
                    stat = parseBegin(tokenizer);
                    break;
                case "commit":
                    stat = parseCommit(tokenizer);
                    break;
                case "abort":
                    stat = parseAbort(tokenizer);
                    break;
                case "create":
                    stat = parseCreate(tokenizer);
                    break;
                case "drop":
                    stat = parseDrop(tokenizer);
                    break;
                case "read":
                    stat = parseRead(tokenizer);
                    break;
                case "insert":
                    stat = parseInsert(tokenizer);
                    break;
                case "delete":
                    stat = parseDelete(tokenizer);
                    break;
                case "update":
                    stat = parseUpdate(tokenizer);
                    break;
                case "show":
                    stat = parseShow(tokenizer);
                    break;
                default:
                    throw Error.InvalidCommandException;
            }
        } catch(Exception e) {
            statErr = e;
        }
        //解析是否有剩余的部分，如果有说明语句不完整或者存在多余吗，转为错误状态
        try {
            String next = tokenizer.peek();
            //不为""
            if(!"".equals(next)) {
                byte[] errStat = tokenizer.errStat();
                statErr = new RuntimeException("Invalid statement: " + new String(errStat));
            }
        } catch(Exception e) {
            e.printStackTrace();
            byte[] errStat = tokenizer.errStat();
            statErr = new RuntimeException("Invalid statement: " + new String(errStat));
        }
        if(statErr != null) {
            throw statErr;
        }
        return stat;
    }

    /**
     *
     * @param tokenizer
     * @return
     * @throws Exception
     */
    private static Show parseShow(Tokenizer tokenizer) throws Exception {
        String tmp = tokenizer.peek();
        if("".equals(tmp)) {
            return new Show();
        }
        throw Error.InvalidCommandException;
    }

    /**
     *update table_name set field_name = value where field_name = value
     * @param tokenizer
     * @return
     * @throws Exception
     */
    private static Update parseUpdate(Tokenizer tokenizer) throws Exception {
        Update update = new Update();
        //table_name
        update.tableName = tokenizer.peek();
        tokenizer.pop();
        //后面必须跟set
        if(!"set".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();
        //field_name
        update.fieldName = tokenizer.peek();
        tokenizer.pop();
        //==
        if(!"=".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();
        //value
        update.value = tokenizer.peek();
        tokenizer.pop();
        //where
        String tmp = tokenizer.peek();
        //如果没有where表示全部更新
        if("".equals(tmp)) {
            update.where = null;
            return update;
        }
        //
        update.where = parseWhere(tokenizer);
        return update;
    }

    /**
     * delete from table_name where field_name = value
     * @param tokenizer
     * @return
     * @throws Exception
     */
    private static Delete parseDelete(Tokenizer tokenizer) throws Exception {
        Delete delete = new Delete();

        if(!"from".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();
        //table_name
        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        delete.tableName = tableName;
        tokenizer.pop();

        delete.where = parseWhere(tokenizer);
        return delete;
    }

    /**
     * insert into table_name values value1, value2, value3
     * 只支持全部插入，不支持选定列插入
     * @param tokenizer
     * @return
     * @throws Exception
     */
    private static Insert parseInsert(Tokenizer tokenizer) throws Exception {
        Insert insert = new Insert();

        if(!"into".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        insert.tableName = tableName;
        tokenizer.pop();

        if(!"values".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        //values
        List<String> values = new ArrayList<>();
        while(true) {
            tokenizer.pop();
            String value = tokenizer.peek();
            if("".equals(value)) {
                break;
            } else {
                values.add(value);
            }
        }
        insert.values = values.toArray(new String[values.size()]);

        return insert;
    }

    /**
     * read * /(field) from table_name where field_name = value
     * @param tokenizer
     * @return
     * @throws Exception
     */
    private static Read parseRead(Tokenizer tokenizer) throws Exception {
        Read read = new Read();

        List<String> fields = new ArrayList<>();
        String asterisk = tokenizer.peek();
        //所有列
        if("*".equals(asterisk)) {
            fields.add(asterisk);
            tokenizer.pop();
        } else {
            //部分列
            while(true) {
                String field = tokenizer.peek();
                if(!isName(field)) {
                    throw Error.InvalidCommandException;
                }
                fields.add(field);
                tokenizer.pop();
                if(",".equals(tokenizer.peek())) {
                    tokenizer.pop();
                } else {
                    break;
                }
            }
        }
        read.fields = fields.toArray(new String[fields.size()]);

        if(!"from".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        read.tableName = tableName;
        tokenizer.pop();

        String tmp = tokenizer.peek();
        if("".equals(tmp)) {
            read.where = null;
            return read;
        }

        read.where = parseWhere(tokenizer);
        return read;
    }

    /**
     * 解析where
     * where field_name op value logic_op field_name op value
     * 最多支持两个条件比较
     * @param tokenizer
     * @return
     * @throws Exception
     */
    private static Where parseWhere(Tokenizer tokenizer) throws Exception {
        Where where = new Where();

        if(!"where".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        SingleExpression exp1 = parseSingleExp(tokenizer);
        where.singleExp1 = exp1;

        //and or
        String logicOp = tokenizer.peek();
        if("".equals(logicOp)) {
            where.logicOp = logicOp;
            return where;
        }
        if(!isLogicOp(logicOp)) {
            throw Error.InvalidCommandException;
        }
        where.logicOp = logicOp;
        tokenizer.pop();

        SingleExpression exp2 = parseSingleExp(tokenizer);
        where.singleExp2 = exp2;

        if(!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return where;
    }

    /**
     * 解析单个条件  fild_name op value
     * @param tokenizer
     * @return
     * @throws Exception
     */
    private static SingleExpression parseSingleExp(Tokenizer tokenizer) throws Exception {
        SingleExpression exp = new SingleExpression();

        String field = tokenizer.peek();
        if(!isName(field)) {
            throw Error.InvalidCommandException;
        }
        exp.field = field;
        tokenizer.pop();

        String op = tokenizer.peek();
        if(!isCmpOp(op)) {
            throw Error.InvalidCommandException;
        }
        exp.compareOp = op;
        tokenizer.pop();

        exp.value = tokenizer.peek();
        tokenizer.pop();
        return exp;
    }

    /**
     * 比较符
     * @param op
     * @return
     */
    private static boolean isCmpOp(String op) {
        return ("=".equals(op) || ">".equals(op) || "<".equals(op));
    }

    /**
     * 逻辑运算符
     * @param op
     * @return
     */
    private static boolean isLogicOp(String op) {
        return ("and".equals(op) || "or".equals(op));
    }

    /**
     * drop table table_name
     * @param tokenizer
     * @return
     * @throws Exception
     */
    private static Drop parseDrop(Tokenizer tokenizer) throws Exception {
        if(!"table".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        if(!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }

        Drop drop = new Drop();
        drop.tableName = tableName;
        return drop;
    }

    /**
     * create table table_name field_name field_type, field_name field_type ...(index field_name,）
     * @param tokenizer
     * @return
     * @throws Exception
     */
    private static Create parseCreate(Tokenizer tokenizer) throws Exception {
        if(!"table".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        Create create = new Create();
        String name = tokenizer.peek();
        if(!isName(name)) {
            throw Error.InvalidCommandException;
        }
        create.tableName = name;

        //字段名类类型
        List<String> fNames = new ArrayList<>();
        List<String> fTypes = new ArrayList<>();
        while(true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            if("(".equals(field)) {
                break;
            }

            if(!isName(field)) {
                throw Error.InvalidCommandException;
            }

            tokenizer.pop();
            String fieldType = tokenizer.peek();
            if(!isType(fieldType)) {
                throw Error.InvalidCommandException;
            }
            fNames.add(field);
            fTypes.add(fieldType);
            tokenizer.pop();

            //如果后面跟了一个，则继续
            String next = tokenizer.peek();
            if(",".equals(next)) {
                continue;
            } else if("".equals(next)) {//
                throw Error.TableNoIndexException;
            } else if("(".equals(next)) {
                break;
            } else {
                throw Error.InvalidCommandException;
            }
        }
        create.fieldName = fNames.toArray(new String[fNames.size()]);
        create.fieldType = fTypes.toArray(new String[fTypes.size()]);
        //索引
        tokenizer.pop();
        if(!"index".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }

        List<String> indexes = new ArrayList<>();
        while(true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            if(")".equals(field)) {
                break;
            }
            if(!isName(field)) {
                throw Error.InvalidCommandException;
            } else {
                indexes.add(field);
            }
        }
        create.index = indexes.toArray(new String[indexes.size()]);
        tokenizer.pop();

        if(!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return create;
    }

    /**
     * 判断是不是规定的3种字段类型
     * @param tp
     * @return
     */
    private static boolean isType(String tp) {
        return ("int32".equals(tp) || "int64".equals(tp) ||
                "string".equals(tp));
    }

    /**
     *
     * @param tokenizer
     * @return
     * @throws Exception
     */
    private static Abort parseAbort(Tokenizer tokenizer) throws Exception {
        if(!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return new Abort();
    }

    private static Commit parseCommit(Tokenizer tokenizer) throws Exception {
        if(!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return new Commit();
    }

    /**
     *begin isolation level read committed / repeatable read
     * @param tokenizer
     * @return
     * @throws Exception
     */
    private static Begin parseBegin(Tokenizer tokenizer) throws Exception {
        String isolation = tokenizer.peek();
        Begin begin = new Begin();
        //默认begin
        if("".equals(isolation)) {
            return begin;
        }
        if(!"isolation".equals(isolation)) {
            throw Error.InvalidCommandException;
        }

        tokenizer.pop();
        String level = tokenizer.peek();
        if(!"level".equals(level)) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tmp1 = tokenizer.peek();
        if("read".equals(tmp1)) {
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if("committed".equals(tmp2)) {
                tokenizer.pop();
                if(!"".equals(tokenizer.peek())) {
                    throw Error.InvalidCommandException;
                }
                return begin;
            } else {
                throw Error.InvalidCommandException;
            }
            //可重复读级别
        } else if("repeatable".equals(tmp1)) {
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if("read".equals(tmp2)) {
                begin.isRepeatableRead = true;
                tokenizer.pop();
                if(!"".equals(tokenizer.peek())) {
                    throw Error.InvalidCommandException;
                }
                return begin;
            } else {
                throw Error.InvalidCommandException;
            }
        } else {
            throw Error.InvalidCommandException;
        }
    }

    /**
     * name长度大于1且第一个必须是字母
     * @param name
     * @return
     */
    private static boolean isName(String name) {
        return !(name.length() == 1 && !Tokenizer.isAlphaBeta(name.getBytes()[0]));
    }
}
