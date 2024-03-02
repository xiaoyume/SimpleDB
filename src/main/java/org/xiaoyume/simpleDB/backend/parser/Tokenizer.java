package org.xiaoyume.simpleDB.backend.parser;

import org.xiaoyume.simpleDB.backend.common.Error;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 简单的词法分析器
 * 数组的字节数组解析为token
 * @date 2024/3/2 13:29
 */
public class Tokenizer {
    private byte[] stat;
    private int pos;
    private String currentToken;
    //表示是否需要解析下一个标记，也就是表示当前标记是不是存在
    private boolean flushToken;
    private Exception err;

    public Tokenizer(byte[] stat) {
        this.stat = stat;
        this.pos = 0;
        this.currentToken = "";
        this.flushToken = true;
    }

    /**
     *获取当前的token，如果当前没有就解析下一个token
     * @return
     * @throws Exception
     */
    public String peek() throws Exception {
        if(err != null) {
            throw err;
        }
        if(flushToken) {
            String token = null;
            try {
                token = next();
            } catch(Exception e) {
                err = e;
                throw e;
            }
            currentToken = token;
            flushToken = false;
        }
        return currentToken;
    }

    /**
     * 移除当前token，把刷新token置为true，表示当前token没用
     */
    public void pop() {
        flushToken = true;
    }

    /**
     * 获取解析过程的出错
     * @return
     */
    public byte[] errStat() {
        byte[] res = new byte[stat.length+3];
        //复制pos之前的部分
        System.arraycopy(stat, 0, res, 0, pos);
        //插入"<< "3个字节
        System.arraycopy("<< ".getBytes(), 0, res, pos, 3);
        //插入剩余的部分
        System.arraycopy(stat, pos, res, pos+3, stat.length-pos);
        return res;
    }

    /**
     * 指针后移动一位
     */
    private void popByte() {
        pos ++;
        if(pos > stat.length) {
            pos = stat.length;
        }
    }

    /**
     * 查看当前字节
     * @return
     */
    private Byte peekByte() {
        if(pos == stat.length) {
            return null;
        }
        return stat[pos];
    }

    /**
     * 查看下一个token
     * @return
     * @throws Exception
     */
    private String next() throws Exception {
        if(err != null) {
            throw err;
        }
        return nextMetaState();
    }

    /**
     * 解析下一个token  遇到符号，自动后移
     * @return
     * @throws Exception
     */
    private String nextMetaState() throws Exception {
        while(true) {
            //排除空格
            Byte b = peekByte();
            //为空表示字节流结束了
            if(b == null) {
                return "";
            }
            if(!isBlank(b)) {
                break;
            }
            popByte();
        }
        byte b = peekByte();
        //符号
        if(isSymbol(b)) {
            popByte();
            return new String(new byte[]{b});
        } else if(b == '"' || b == '\'') {
            //解析出带引号的标记
            return nextQuoteState();
        } else if(isAlphaBeta(b) || isDigit(b)) {
            //下一个token数据
            return nextTokenState();
        } else {
            err = Error.InvalidCommandException;
            throw err;
        }
    }

    /**
     *
     * @return
     * @throws Exception
     */
    private String nextTokenState() throws Exception {
        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte();
            //为空，或者不是字母或者数字
            if(b == null || !(isAlphaBeta(b) || isDigit(b))) {
                //是空格
                if(b != null && isBlank(b)) {
                    popByte();
                }
                return sb.toString();
            }
            //字符或者数字直接添加到sb里
            sb.append(new String(new byte[]{b}));
            popByte();
        }
    }

    static boolean isDigit(byte b) {
        return (b >= '0' && b <= '9');
    }

    static boolean isAlphaBeta(byte b) {
        return ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z'));
    }

    /**
     *解析出带引号的字符常量
     * "ssss"
     * @return
     * @throws Exception
     */
    private String nextQuoteState() throws Exception {
        byte quote = peekByte();
        popByte();
        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte();
            if(b == null) {
                err = Error.InvalidCommandException;
                throw err;
            }
            //引号结束
            if(b == quote) {
                popByte();
                break;
            }
            //引号里的内容加入到这里
            sb.append(new String(new byte[]{b}));
            popByte();
        }
        return sb.toString();
    }

    static boolean isSymbol(byte b) {
        return (b == '>' || b == '<' || b == '=' || b == '*' ||
                b == ',' || b == '(' || b == ')');
    }

    static boolean isBlank(byte b) {
        return (b == '\n' || b == ' ' || b == '\t');
    }
}
