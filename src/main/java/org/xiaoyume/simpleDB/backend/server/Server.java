package org.xiaoyume.simpleDB.backend.server;

import org.xiaoyume.simpleDB.backend.parser.Parser;
import org.xiaoyume.simpleDB.backend.parser.statement.*;
import org.xiaoyume.simpleDB.backend.tbm.BeginRes;
import org.xiaoyume.simpleDB.backend.tbm.TableManager;
import org.xiaoyume.simpleDB.common.Error;
import org.xiaoyume.simpleDB.transport.Encoder;
import org.xiaoyume.simpleDB.transport.Package;
import org.xiaoyume.simpleDB.transport.Packager;
import org.xiaoyume.simpleDB.transport.Transpoter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 数据库服务器
 * @date 2024/3/2 16:45
 */
public class Server {
    //端口号
    private int port;
    //表管理器
    TableManager tbm;

    public Server(int port, TableManager tbm) {
        this.port = port;
        this.tbm = tbm;
    }

    public void start() {
        ServerSocket ss = null;
        try {
            //新建一个serversocket对象
            ss = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        System.out.println("Server listen to port: " + port);
        //创建线程池
        //核心线程是10，没有任务需要执行时
        //最大线程 20
        //线程空闲时间，线程数大于核心数时后，空闲线程的最长等待时间
        //保存等待执行任务的阻塞队列
        //饱和策略
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(10, 20, 1L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());
        //循环监听，当有客户端连接时，创建一个线程处理
        while(true) {
            try {
                Socket socket = ss.accept();
                //可以作为线程执行的任务
                Runnable worker = new HandleSocket(socket, tbm);
                tpe.execute(worker);
            } catch(IOException e) {
                e.printStackTrace();
                continue;
            } finally {
                try {
                    ss.close();
                } catch (IOException e) {}
            }
        }
    }
}

class HandleSocket implements Runnable {
    private Socket socket;
    private TableManager tbm;

    public HandleSocket(Socket socket, TableManager tbm) {
        this.socket = socket;
        this.tbm = tbm;
    }

    @Override
    public void run() {
        //获取客户端的地址和端口信息
        InetSocketAddress address = (InetSocketAddress)socket.getRemoteSocketAddress();
        System.out.println("Establish connection: " + address.getAddress().getHostAddress()+":"+address.getPort());
        Packager packager = null;
        try {
            Transpoter t = new Transpoter(socket);
            Encoder e = new Encoder();
            packager = new Packager(t, e);
        } catch(IOException e) {
            e.printStackTrace();
            try {
                socket.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            return;
        }
        Executor exe = new Executor(tbm);
        //不断接收客户端发送的数据包
        while(true) {
            Package pkg = null;
            try {
                pkg = packager.receive();
            } catch(Exception e) {
                break;
            }
            byte[] sql = pkg.getData();
            byte[] result = null;
            Exception e = null;
            try {
                //执行sql
                result = exe.execute(sql);
            } catch (Exception e1) {
                e = e1;
            }
            //发送结果
            pkg = new Package(result, e);
            try {
                packager.send(pkg);
            } catch (Exception e1) {
                break;
            }
        }
        exe.close();
        try {
            packager.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
