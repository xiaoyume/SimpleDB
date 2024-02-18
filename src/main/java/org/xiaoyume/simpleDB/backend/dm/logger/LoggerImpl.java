package org.xiaoyume.simpleDB.backend.dm.logger;

import com.google.common.primitives.Bytes;
import org.xiaoyume.simpleDB.backend.common.Parser;
import org.xiaoyume.simpleDB.backend.utils.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 日志文件读写
 * 日志文件标准格式为：
 * [XCachesum] [log1][log2]...[logN][BadTail]
 * XCachesum 为后续所有日志计算的Checksum, int 类型
 *
 * 每条正确日志的格式为：
 * [size][Chechsum][data]
 * size int data长度
 * checksum int 数据的校验和
 * @date 2024/2/18 15:31
 */
public class LoggerImpl implements Logger{
    private static final int SEED = 13331;
    //日志 记录大小的位置
    private static final int OF_SIZE = 0;
    //日志记录的校验和的位置
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    //日志记录数据的位置
    private static final int OF_DATA = OF_CHECKSUM + 4;

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;
    private long position;//当前日志指针的位置
    private long fileSize;//文件大小，初始化才操作
    private int xChecksum;
    LoggerImpl(RandomAccessFile raf, FileChannel fc){
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }
    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum){
        this.file = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    /**
     * open 后初始化lg
     */
    void init(){
        long size = 0;
        try{
            size = file.length();
        }catch (Exception e){
            Panic.panic(e);
        }
        if(size < 4){
            Panic.panic(new RuntimeException("Bad log file!"));
        }

        ByteBuffer raw = ByteBuffer.allocate(4);
        try{
            fc.position(0);
            fc.read(raw);
        }catch (IOException e){
            Panic.panic(e);
        }
        //读文件头的int数据
        int checksum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xChecksum = checksum;

        checkAndRemoveTail();
    }

    /**
     * 检查并移除bad tail
     */
    private void checkAndRemoveTail() {
        rewind();//回退pos,也就是第一条日志之前的位置
        int xCheck = 0;
        while(true){
            byte[] log = internNext();
            if(log == null){
                break;
            }
            xCheck = calChecksum(xCheck, log);
        }
        if(xCheck != xChecksum){
            Panic.panic(new RuntimeException("bad log file"));
        }
        //从position位置把文件截断
        try{
            truncate(position);
        }catch (Exception e){
            Panic.panic(e);
        }
        try{
            file.seek(position);
        }catch (Exception e){
            Panic.panic(e);
        }
        rewind();
    }

    /**
     * 下一条日志
     * @return
     */
    private byte[] internNext(){
        //当前位置加两个记录数
        if(position + OF_DATA >= fileSize){
            return null;
        }
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try{
            fc.position(position);
            fc.read(tmp);
        }catch (IOException e){
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());
        //在上面的基础上加了实际数据的长度
        if(position + size + OF_DATA > fileSize){
            return null;
        }

        //存放日志
        ByteBuffer buf = ByteBuffer.allocate(size + OF_DATA);
        try{
            fc.position(position);
            fc.read(buf);
        }catch (IOException e){
            Panic.panic(e);
        }

        byte[] log = buf.array();
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        if(checkSum2 != checkSum1){
            return null;
        }
        position += log.length;
        return log;
    }

    /**
     * 输入之前的检验和以及log计算现在的检验和
     * @param xCheck
     * @param log 实际数据，非整条日志
     * @return
     */
    private int calChecksum(int xCheck, byte[] log){
        for(byte b : log){
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    /**
     * 追加日志
     * @param data
     */
    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try{
            //追加日志
            fc.position(fc.size());
            fc.write(buf);
        }catch (IOException e){
            Panic.panic(e);
        }finally {
            lock.unlock();
        }
        //更新checksum
        updateXChecksum(log);
    }

    /**
     * 更新检验和
     * @param log
     */
    private void updateXChecksum(byte[] log){
        this.xChecksum = calChecksum(this.xChecksum, log);
        try{
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            fc.force(true);
        }catch (IOException e){
            Panic.panic(e);
        }
    }

    /**
     * 封装日志 size + checksum + data
     * @param data
     * @return
     */
    private byte[] wrapLog(byte[] data) {
        //校验和的int数据的byte数组
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size, checksum, data);
    }

    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try{
            //截断文件到指定大小
            fc.truncate(x);
        }finally {
            lock.unlock();
        }
    }

    /**
     * 返回下一条日志的实际数据
     * @return
     */
    @Override
    public byte[] next() {
        lock.lock();
        try{
            byte[] log = internNext();
            if(log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        }finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try{
            fc.close();
            file.close();
        }catch (IOException e){
            Panic.panic(e);
        }
    }
}
