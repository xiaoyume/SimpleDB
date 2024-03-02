package org.xiaoyume.simpleDB.backend.tbm;

import org.xiaoyume.simpleDB.backend.common.Error;
import org.xiaoyume.simpleDB.backend.utils.Panic;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 启动文件 记录第一个表的uid
 * @date 2024/3/1 20:22
 */

//记录第一个表的uid
public class Booter {
    public static final String BOOTER_SUFFIX = ".bt";
    public static final String BOOTER_TMP_SUFFIX = ".bt_tmp";

    String path;
    File file;

    /**
     * 创建一个新的引导文件
     * @param path
     * @return
     */
    public static Booter create(String path) {
        removeBadTmp(path);
        File f = new File(path+BOOTER_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        return new Booter(path, f);
    }

    /**
     * 打开一个现有的引导文件
     * @param path
     * @return
     */
    public static Booter open(String path) {
        removeBadTmp(path);
        File f = new File(path+BOOTER_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        return new Booter(path, f);
    }

    /**
     * 删除临时引导文件
     * @param path
     */
    private static void removeBadTmp(String path) {
        new File(path+BOOTER_TMP_SUFFIX).delete();
    }

    private Booter(String path, File file) {
        this.path = path;
        this.file = file;
    }

    //读取文件里所有的字节数据
    public byte[] load() {
        byte[] buf = null;
        try {
            buf = Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf;
    }

    /**
     *更新引导文件中的数据
     * 创建一个临时文件，并检查其是否可读和可写。
     * 然后将数据写入临时文件中。接着，将临时文件重命名为引导文件，
     * 并更新 Booter 对象的 file 属性。
     * @param data
     */
    public void update(byte[] data) {
        //新建一个新的临时文件
        File tmp = new File(path + BOOTER_TMP_SUFFIX);
        if(!tmp.canRead() || !tmp.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        try(FileOutputStream out = new FileOutputStream(tmp)) {
            //写入数据
            out.write(data);
            out.flush();
        } catch(IOException e) {
            Panic.panic(e);
        }
        //把临时引导文件命名为引导文件
        tmp.renameTo(new File(path+BOOTER_SUFFIX));
        file = new File(path+BOOTER_SUFFIX);
        if(!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
    }
}
