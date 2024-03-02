package org.xiaoyume.simpleDB.transport;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/3/2 15:38
 */
public class Package {
    byte[] data;
    Exception err;
    public Package(byte[] data, Exception err){
        this.data = data;
        this.err = err;
    }
    public byte[] getData(){
        return data;
    }
    public Exception getErr(){
        return err;
    }
}
