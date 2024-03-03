package org.xiaoyume.simpleDB.client;

import org.xiaoyume.simpleDB.transport.Package;
import org.xiaoyume.simpleDB.transport.Packager;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 客户端类，负责与服务器的交互
 * @date 2024/3/3 14:12
 */
public class Client {
    private RoundTripper rt;
    public Client(Packager packager) {
        this.rt = new RoundTripper(packager);
    }

    /**
     * 客户端处理，
     * 打包，发送，接收，获得返回数据
     * @param stat
     * @return
     * @throws Exception
     */
    public byte[] execute(byte[] stat) throws Exception {
        Package pkg = new Package(stat, null);
        Package resPkg = rt.roundTrip(pkg);
        if(resPkg.getErr() != null) {
            throw resPkg.getErr();
        }
        return resPkg.getData();
    }

    public void close() {
        try {
            rt.close();
        } catch (Exception e) {
        }
    }
}
