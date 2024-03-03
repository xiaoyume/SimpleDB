package org.xiaoyume.simpleDB.transport;

import com.google.common.primitives.Bytes;
import org.xiaoyume.simpleDB.common.Error;

import java.util.Arrays;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 编码器
 * @date 2024/3/2 15:38
 */
public class Encoder {
    //编码包数据
    public byte[] encode(Package pkg) {
        if (pkg.getErr() != null) {
            Exception err = pkg.getErr();
            String msg = "Intern server error!";
            if (err.getMessage() != null) {
                msg = err.getMessage();
            }
            return Bytes.concat(new byte[]{1}, msg.getBytes());
        } else {
            return Bytes.concat(new byte[]{0}, pkg.getData());
        }
    }

    /**
     * 解码
     *
     * @param data
     * @return
     * @throws Exception
     */
    public Package decode(byte[] data) throws Exception {
        if (data.length < 1) {
            throw Error.InvalidPkgDataException;
        }
        if (data[0] == 0) {
            return new Package(Arrays.copyOfRange(data, 1, data.length), null);
        } else if (data[0] == 1) {
            return new Package(null, new RuntimeException(new String(Arrays.copyOfRange(data, 1, data.length))));
        } else {
            throw Error.InvalidPkgDataException;
        }
    }
}
