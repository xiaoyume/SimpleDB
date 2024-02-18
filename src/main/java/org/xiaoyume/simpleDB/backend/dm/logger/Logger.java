package org.xiaoyume.simpleDB.backend.dm.logger;

import org.xiaoyume.simpleDB.backend.common.Parser;
import org.xiaoyume.simpleDB.backend.utils.Panic;

import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/18 14:55
 */
public interface Logger {
    void log(byte[] data);
    void truncate(long x) throws Exception;
    byte[] next();
    void rewind();
    void close();
    public static Logger create(String path){
        File f = new File(path);
        try{
            if(!f.createNewFile()){
                Panic.panic(new RuntimeException("file already exists!"));
            }
        }catch (Exception e){
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()){
            Panic.panic(new RuntimeException("file can not be read or write!"));
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try{
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        }catch (Exception e){
            Panic.panic(e);
        }

        ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(0));
        try{
            fc.position(0);
            fc.write(buf);
            fc.force(true);
        }catch (IOException e){
            Panic.panic(e);
        }
        return new LoggerImpl(raf, fc, 0);
    }
    public static Logger open(String path){
        File f = new File(path);
        if(!f.exists()){
            Panic.panic(new RuntimeException("file doesnot exists!"));
        }
        if(!f.canRead() || !f.canWrite()){
            Panic.panic(new RuntimeException("file can not be read or write!"));
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try{
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        }catch (Exception e){
            Panic.panic(e);
        }

        LoggerImpl lg = new LoggerImpl(raf, fc);
        lg.init();
        return lg;
    }
}
