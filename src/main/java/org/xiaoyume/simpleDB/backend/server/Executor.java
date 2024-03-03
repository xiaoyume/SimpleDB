package org.xiaoyume.simpleDB.backend.server;

import org.xiaoyume.simpleDB.backend.parser.Parser;
import org.xiaoyume.simpleDB.backend.parser.statement.*;
import org.xiaoyume.simpleDB.backend.tbm.BeginRes;
import org.xiaoyume.simpleDB.backend.tbm.TableManager;
import org.xiaoyume.simpleDB.common.Error;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 执行器
 * @date 2024/3/2 16:45
 */
public class Executor {
    private long xid;
    TableManager tbm;

    public Executor(TableManager tbm) {
        this.tbm = tbm;
        this.xid = 0;
    }

    public void close() {
        if(xid != 0) {
            System.out.println("Abnormal Abort: " + xid);
            tbm.abort(xid);
        }
    }

    /**
     * 执行语句
     * @param sql sql字节字节数据
     * @return
     * @throws Exception
     */
    public byte[] execute(byte[] sql) throws Exception {
        System.out.println("Execute: " + new String(sql));
        Object stat = Parser.Parse(sql);
        //类检查，stat是不是一个Begin类的实例
        if(Begin.class.isInstance(stat)) {
            //xid必须是0
            if(xid != 0) {
                throw Error.NestedTransactionException;
            }
            BeginRes r = tbm.begin((Begin)stat);
            xid = r.xid;
            return r.result;
        } else if(Commit.class.isInstance(stat)) {//提交
            if(xid == 0) {
                throw Error.NoTransactionException;
            }
            byte[] res = tbm.commit(xid);
            xid = 0;
            return res;
        } else if(Abort.class.isInstance(stat)) {
            if(xid == 0) {
                throw Error.NoTransactionException;
            }
            byte[] res = tbm.abort(xid);
            xid = 0;
            return res;
        } else {
            //执行sql语句
            return execute2(stat);
        }
    }

    /**
     * 执行具体的sql
     * @param stat
     * @return
     * @throws Exception
     */
    private byte[] execute2(Object stat) throws Exception {
        //标记是否开启了事务
        boolean tmpTransaction = false;
        Exception e = null;
        //tbm.begin
        if(xid == 0) {
            //
            tmpTransaction = true;
            BeginRes r = tbm.begin(new Begin());
            xid = r.xid;
        }
        try {
            byte[] res = null;
            if(Show.class.isInstance(stat)) {
                res = tbm.show(xid);
            } else if(Create.class.isInstance(stat)) {
                res = tbm.create(xid, (Create)stat);
            } else if(Read.class.isInstance(stat)) {
                res = tbm.read(xid, (Read)stat);
            } else if(Insert.class.isInstance(stat)) {
                res = tbm.insert(xid, (Insert)stat);
            } else if(Delete.class.isInstance(stat)) {
                res = tbm.delete(xid, (Delete)stat);
            } else if(Update.class.isInstance(stat)) {
                res = tbm.update(xid, (Update)stat);
            }
            return res;
        } catch(Exception e1) {
            e = e1;
            throw e;
        } finally {
            //开启了事务
            if(tmpTransaction) {
                //有异常，直接回滚
                if(e != null) {
                    tbm.abort(xid);
                } else {
                    tbm.commit(xid);
                }
                xid = 0;
            }
        }
    }
}
