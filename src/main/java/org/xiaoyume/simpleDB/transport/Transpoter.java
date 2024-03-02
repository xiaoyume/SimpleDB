package org.xiaoyume.simpleDB.transport;

import com.google.common.primitives.Bytes;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 在socket上发送和接收数据
 * @date 2024/3/2 15:38
 */
public class Transpoter {
    //建立和远程主机的连接
    private Socket socket;
    private BufferedInputStream reader;
    private BufferedOutputStream writer;

    public Transpoter(Socket socket) throws IOException {
        this.socket = socket;
        this.reader = new BufferedInputStream(socket.getInputStream());
        this.writer = new BufferedOutputStream(socket.getOutputStream());
    }

    /**
     * 写入输出流中
     * @param data
     * @throws Exception
     */
    public void send(byte[] data) throws Exception {
        byte[] raw = hexEncode(data);
        writer.write(raw);
        writer.flush();
    }

    /**
     * 发送数据
     * @return
     * @throws Exception
     */
    public byte[] receive() throws Exception {
        List<Byte> bytes = new ArrayList<>();
        //循环读入字节
        while(true) {
            byte b = (byte)reader.read();
            bytes.add(b);
            if(b == '\n') {
                break;
            }
        }
        //
        return hexDecode(Bytes.toArray(bytes));
    }

    public void close() throws IOException {
        socket.close();
    }

    /**
     * 将字节数组进行十六进制编码
     * 使用 Apache Commons Codec 库中的 Hex.encodeHexString 方法进行编码，
     * 并在编码后的字符串末尾添加换行符，并将编码后的字符串转换为字节数组返回。
     * @param buf
     * @return
     */
    private byte[] hexEncode(byte[] buf) {
        return (Hex.encodeHexString(buf, true)+"\n").getBytes();
    }

    private byte[] hexDecode(byte[] buf) throws DecoderException {
        return Hex.decodeHex(new String(Arrays.copyOf(buf, buf.length-1)));
    }
}
