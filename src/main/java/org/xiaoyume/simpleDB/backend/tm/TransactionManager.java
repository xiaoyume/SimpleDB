package org.xiaoyume.simpleDB.backend.tm;

import org.xiaoyume.simpleDB.backend.common.Parser;
import org.xiaoyume.simpleDB.backend.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/17 17:58
 */
public class TransactionManager {
    //XID文件头长度
    private static final int LEN_XID_HEADER_LENGTH = 8;
    //xid字段长度，一个字节
    private static final int XID_FIELD_SIZE = 1;

    //事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIEDL_TRAN_ABORTED = 2;
    //超级事务,永远处于committed状态
    public static final long SUPER_XID = 0;
    private RandomAccessFile file;
    private FileChannel fc;
    private long xidCounter;
    private Lock counterLock;

    public static TransactionManager create(String path) {
        File f = new File(path);
        //创建文件
        try {
            if (!f.createNewFile()) {
                Panic.panic(new RuntimeException("file already exists!"));
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(new RuntimeException("file cannot read or write"));
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        //写空XID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[LEN_XID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return new TransactionManager(raf, fc);
    }

    public static TransactionManager open(String path) {
        File f = new File(path);
        if (!f.exists()) {
            Panic.panic(new RuntimeException("File does not exists!"));
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(new RuntimeException("File cannot read or write"));
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new TransactionManager(raf, fc);
    }

    public TransactionManager(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * 检查xid是否合法
     * 读取XID_FILE_HEADER中的xidcounter，根据计算文件的理论长度，对比实际长度
     */
    private void checkXIDCounter() {
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e) {
            Panic.panic(new RuntimeException("invalid xid file"));
        }
        if (fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(new RuntimeException("invaild xid file!"));
        }

        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.xidCounter = Parser.parseLong(buf.array());
        long end = getXidPosition(this.xidCounter + 1);
        if (end != fileLen) {
            Panic.panic(new RuntimeException("invalid xid file!"));
        }
    }

    /**
     * 根据事务xid取得其在xid文件中对应的位置
     *
     * @param xid
     * @return
     */
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }

    /**
     * 更新事务状态
     *
     * @param xid
     * @param status
     */
    private void updateXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(true);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * xid加1，更新xid header
     */
    private void incrXIDCounter() {
        xidCounter++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(true);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }


    /**
     * 开启事务,返回xid
     *
     * @return
     */
    public long begin() {
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            updateXID(xid, FIELD_TRAN_ACTIVE);
            incrXIDCounter();
            return xid;
        }finally {
            counterLock.unlock();
        }
    }

    /**
     * 提交事务
     * @param xid
     */
    public void commit(long xid){
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }
    /**
     * 回滚事务
     */
    public void abort(long xid){
        updateXID(xid, FIEDL_TRAN_ABORTED);
    }

    /**
     * 检测文件中xid事务状态是否是status
     * @param xid
     * @param status
     * @return
     */
    private boolean checkXID(long xid, byte status){
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try{
            fc.position(offset);
            fc.read(buf);
        }catch (IOException e){
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }
    public boolean isActive(long xid){
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }
    public boolean isCommitted(long xid){
        if(xid == SUPER_XID) return true;
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }
    public boolean isAborted(long xid){
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIEDL_TRAN_ABORTED);
    }
    public void close(){
        try{
            fc.close();
            file.close();
        }catch (IOException e){
            Panic.panic(e);
        }
    }

}