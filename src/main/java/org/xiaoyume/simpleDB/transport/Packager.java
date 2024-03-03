package org.xiaoyume.simpleDB.transport;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 打包和解包数据
 * @date 2024/3/2 15:38
 */
public class Packager {
    private Transpoter transpoter;
    private Encoder encoder;

    public Packager(Transpoter transpoter, Encoder encoder) {
        this.transpoter = transpoter;
        this.encoder = encoder;
    }

    public void send(Package pkg) throws Exception {
        byte[] data = encoder.encode(pkg);
        transpoter.send(data);
    }

    public Package receive() throws Exception {
        byte[] data = transpoter.receive();
        return encoder.decode(data);
    }

    public void close() throws Exception {
        transpoter.close();
    }
}
