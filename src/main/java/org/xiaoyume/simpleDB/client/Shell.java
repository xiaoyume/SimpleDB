package org.xiaoyume.simpleDB.client;

import java.util.Scanner;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 命令行交互数据
 * @date 2024/3/3 14:13
 */
public class Shell {
    private Client client;
    public Shell(Client client){
        this.client = client;
    }
    public void run() {
        Scanner sc = new Scanner(System.in);
        try {
            while(true) {
                System.out.print(":> ");
                //只接收一行数据，没有实现多行处理逻辑
                String line = sc.nextLine();
                String statStr = line.substring(0, line.length()-1);
                if("exit".equals(statStr) || "quit".equals(statStr)) {
                    break;
                }
                try {
                    //给客户端和服务器交互
                    byte[] res = client.execute(statStr.getBytes());
                    System.out.println(new String(res));
                } catch(Exception e) {
                    System.out.println(e.getMessage());
                }

            }
        } finally {
            sc.close();
            client.close();
        }
    }
}
