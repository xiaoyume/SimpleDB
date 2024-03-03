package org.xiaoyume.simpleDB.backend;

import org.apache.commons.cli.*;
import org.xiaoyume.simpleDB.backend.dm.DataManager;
import org.xiaoyume.simpleDB.backend.server.Server;
import org.xiaoyume.simpleDB.backend.tbm.TableManager;
import org.xiaoyume.simpleDB.backend.tm.TransactionManager;
import org.xiaoyume.simpleDB.backend.utils.Panic;
import org.xiaoyume.simpleDB.backend.vm.VersionManager;
import org.xiaoyume.simpleDB.backend.vm.VersionManagerImpl;
import org.xiaoyume.simpleDB.common.Error;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 启动类，解析启动参数
 * @date 2024/3/3 14:35
 */
public class Launcher {
    public static final int port = 9999;
    //默认大小是64M
    public static final long DEFAULT_MEM = (1 << 20) * 64;
    public static final long KB = 1 << 10;
    public static final long MB = 1 << 20;
    public static final long GB = 1 << 30;

    public static void main(String[] args) throws ParseException {
        Options opts = new Options();
        //3个选项，开启，创建数据库，指定内存大小
        opts.addOption("open", true, "-openDBPath");
        opts.addOption("create", true, "-createDBPath");
        opts.addOption("mem", true, "-mem 64MB");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(opts, args);

        if (cmd.hasOption("open")) {
            openDB(cmd.getOptionValue("oepn"), parseMem(cmd.getOptionValue("mem")));
            return;
        }
        if (cmd.hasOption("create")) {
            createDB(cmd.getOptionValue("create"));
            return;
        }
        System.out.println("Usage : laucher (open|create) DBPath");
    }

    private static void createDB(String path) {
        TransactionManager tm = TransactionManager.create(path);
        DataManager dm = DataManager.create(path, DEFAULT_MEM, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager.create(path, vm, dm);
        tm.close();
        dm.close();
    }

    private static void openDB(String path, long mem) {
        TransactionManager tm = TransactionManager.open(path);
        DataManager dm = DataManager.open(path, mem, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);

        TableManager tbm = TableManager.open(path, vm, dm);
        new Server(port, tbm).start();
    }

    /**
     * 解析用户输入的内存大小 "100000KB",最后两位是单位，前面是数字
     *
     * @param memStr
     * @return
     */
    private static long parseMem(String memStr) {
        if (memStr == null || "".equals(memStr)) {
            return DEFAULT_MEM;
        }
        if (memStr.length() < 2) {
            Panic.panic(Error.InvalidMemException);
        }
        String unit = memStr.substring(memStr.length() - 1);
        long memNum = Long.parseLong(memStr.substring(0, memStr.length() - 2));
        switch (unit) {
            case "KB":
                return memNum * KB;
            case "MB":
                return memNum * MB;
            case "GB":
                return memNum * GB;
            default:
                Panic.panic(Error.InvalidMemException);
        }
        return DEFAULT_MEM;
    }


}
