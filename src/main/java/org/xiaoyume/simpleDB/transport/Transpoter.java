package org.xiaoyume.simpleDB.transport;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.io.*;
import java.net.Socket;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 在socket上发送和接收数据
 * @date 2024/3/2 15:38
 */
public class Transpoter {
    //建立和远程主机的连接
    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;

    public Transpoter(Socket socket) throws IOException {
        this.socket = socket;
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    /**
     * 写入输出流中
     *
     * @param data
     * @throws Exception
     */
    public void send(byte[] data) throws Exception {
        String raw = hexEncode(data);
        writer.write(raw);
        writer.flush();
    }

    /**
     * 发送数据
     *
     * @return
     * @throws Exception
     */
    public byte[] receive() throws Exception {
        String line = reader.readLine();
        if (line == null) {
            close();
        }
        return hexDecode(line);
    }

    public void close() throws IOException {
        writer.close();
        reader.close();
        socket.close();
    }

    /**
     * 将字节数组进行十六进制编码
     * 使用 Apache Commons Codec 库中的 Hex.encodeHexString 方法进行编码，
     * 并在编码后的字符串末尾添加换行符。
     *
     * @param buf
     * @return
     */
    private String hexEncode(byte[] buf) {
        return Hex.encodeHexString(buf, true) + "\n";
    }

    /**
     * 解码为字节数组
     *
     * @param buf
     * @return
     * @throws DecoderException
     */
    private byte[] hexDecode(String buf) throws DecoderException {
        return Hex.decodeHex(buf);
    }
}
