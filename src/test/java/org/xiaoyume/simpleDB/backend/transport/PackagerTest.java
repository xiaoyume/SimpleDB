package org.xiaoyume.simpleDB.backend.transport;

import org.junit.Test;
import org.xiaoyume.simpleDB.backend.utils.Panic;
import org.xiaoyume.simpleDB.transport.Encoder;
import org.xiaoyume.simpleDB.transport.Package;
import org.xiaoyume.simpleDB.transport.Packager;
import org.xiaoyume.simpleDB.transport.Transpoter;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/3/2 15:59
 */
public class PackagerTest {
    @Test
    public void testPackager() throws Exception {
        //新建一个线程
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    //创建socket.接收连接
                    ServerSocket ss = new ServerSocket(10365);
                    Socket socket = ss.accept();
                    Transpoter t = new Transpoter(socket);
                    Encoder e = new Encoder();
                    Packager p = new Packager(t, e);
                    //接收一个package
                    Package one = p.receive();
                    assert "pkg1 test".equals(new String(one.getData()));
                    //再接收一个package
                    Package two = p.receive();
                    assert "pkg2 test".equals(new String(two.getData()));
                    p.send(new Package("pkg3 test".getBytes(), null));
                    ss.close();
                } catch (Exception e) {
                    Panic.panic(e);
                }
            }
        }).start();
        Thread.sleep(1000);
        Socket socket = new Socket("127.0.0.1", 10365);
        Transpoter t = new Transpoter(socket);
        Encoder e = new Encoder();
        Packager p = new Packager(t, e);
        p.send(new Package("pkg1 test".getBytes(), null));
        p.send(new Package("pkg2 test".getBytes(), null));
        Package three = p.receive();
        assert "pkg3 test".equals(new String(three.getData()));
    }

    @Test
    public void testSocket() throws Exception {
        try {
            ServerSocket ss = new ServerSocket(8888);
            System.out.println("启动服务器....");
            Socket s = ss.accept();
            System.out.println("客户端:"+s.getInetAddress().getLocalHost()+"已连接到服务器");

            BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
            //读取客户端发送来的消息
            String mess = br.readLine();
            System.out.println("客户端："+mess);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()));
            bw.write(mess+"\n");
            bw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void testSocket2() throws Exception {
        try {
            Socket s = new Socket("127.0.0.1",8888);

            //构建IO
            InputStream is = s.getInputStream();
            OutputStream os = s.getOutputStream();

            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os));
            //向服务器端发送一条消息
            bw.write("测试客户端和服务器通信，服务器接收到消息返回到客户端\n");
            bw.flush();

            //读取服务器返回的消息
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String mess = br.readLine();
            System.out.println("服务器："+mess);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
