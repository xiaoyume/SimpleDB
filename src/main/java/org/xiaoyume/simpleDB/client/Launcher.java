package org.xiaoyume.simpleDB.client;

import org.xiaoyume.simpleDB.transport.Encoder;
import org.xiaoyume.simpleDB.transport.Packager;
import org.xiaoyume.simpleDB.transport.Transpoter;

import java.io.IOException;
import java.net.Socket;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/3/3 14:12
 */
public class Launcher {
    public static void main(String[] args) throws IOException {
        Socket socket = new Socket("127.0.0.1", 9999);
        Encoder e = new Encoder();
        Transpoter t = new Transpoter(socket);
        Packager packager = new Packager(t, e);

        Client client = new Client(packager);
        Shell shell = new Shell(client);
        shell.run();
    }
}
