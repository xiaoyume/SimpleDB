package org.xiaoyume.simpleDB.client;

import org.xiaoyume.simpleDB.transport.Package;
import org.xiaoyume.simpleDB.transport.Packager;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 处理包数据的往返传输
 * @date 2024/3/3 14:13
 */
public class RoundTripper {
    //包传输器
    private Packager packager;
    public RoundTripper(Packager packager){
        this.packager = packager;
    }

    /**
     * 发送包，接收包
     * @param pkg
     * @return
     * @throws Exception
     */
    public Package roundTrip(Package pkg) throws Exception {
        packager.send(pkg);
        return packager.receive();
    }

    public void close() throws Exception {
        packager.close();
    }
}
