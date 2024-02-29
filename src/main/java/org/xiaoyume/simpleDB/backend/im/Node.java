package org.xiaoyume.simpleDB.backend.im;

import org.xiaoyume.simpleDB.backend.common.SubArray;
import org.xiaoyume.simpleDB.backend.dm.dataItem.DataItem;
import org.xiaoyume.simpleDB.backend.tm.TransactionManagerImpl;
import org.xiaoyume.simpleDB.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author xiaoy
 * @version 1.0
 * @description: Node结构：
 * [LeafFlag][KeyNumber][SiblingUid]
 * 是否是叶子节点 1  关键字数 2  uid 8
 * [son0][key0][son1][key1]  [sonN][keyN]
 * @date 2024/2/28 19:43
 */
public class Node {
    //叶子节点标志位，占1字节
    static final int IS_LEAF_OFFSET = 0;
    //键数量，2字节
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET + 1;
    //兄弟节点的偏移，8字节
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET + 2;
    //节点头大小，11字节
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET + 8;

    //每个节点键的数量
    static final int BALANCE_NUMBER = 32;
    //节点总大小
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2*8)*(BALANCE_NUMBER*2+2);

    BPlusTree tree;
    DataItem dataItem;
    SubArray raw;
    long uid;

    /**设置是否是叶子节点标志位
     * @param raw
     * @param isLeaf
     */
    static void setRawIsLeaf(SubArray raw, boolean isLeaf){
        if(isLeaf){
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 1;
        }else{
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 0;
        }
    }

    /**
     * 获得是否是叶子节点标志位
     * @param raw
     * @return
     */
    static boolean getRawIsLeaf(SubArray raw){
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte) 1;
    }

    /**
     * 设置关键字数量
     * @param raw
     * @param noKeys
     */
    static void setRawNoKeys(SubArray raw, int noKeys) {
        System.arraycopy(Parser.short2Byte((short)noKeys), 0, raw.raw, raw.start+NO_KEYS_OFFSET, 2);
    }

    static int getRawNoKeys(SubArray raw) {
        return (int)Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start+NO_KEYS_OFFSET, raw.start+NO_KEYS_OFFSET+2));
    }

    static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(Parser.long2Byte(sibling), 0, raw.raw, raw.start+SIBLING_OFFSET, 8);
    }

    static long getRawSibling(SubArray raw) {
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start+SIBLING_OFFSET, raw.start+SIBLING_OFFSET+8));
    }

    /**
     * 设置第几个子节点的uid
     * @param raw
     * @param uid
     * @param kth
     */
    static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
    }

    /**
     * 获得第几个子节点的uid
     * @param raw
     * @param kth
     * @return
     */
    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    /**
     * 设置第几个子节点的key
     * @param raw
     * @param key
     * @param kth
     */
    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
    }

    static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    /**
     * 从第几个子节点开始复制到新的节点的头部
     * 用于将节点中一部分键值和子节点指针复制到另一个节点的头部。
     * @param from
     * @param to
     * @param kth
     */
    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        int offset = from.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(from.raw, offset, to.raw, to.start+NODE_HEADER_SIZE, from.end-offset);
    }

    /**
     *将节点 raw 中第 kth 个子节点指针及其后的内容整体向后移动。
     * 用于在插入新键值或子节点指针时调整节点的内容。
     * @param raw
     * @param kth
     */
    static void shiftRawKth(SubArray raw, int kth) {
        int begin = raw.start+NODE_HEADER_SIZE+(kth+1)*(8*2);
        int end = raw.start+NODE_SIZE-1;
        for(int i = end; i >= begin; i --) {
            raw.raw[i] = raw.raw[i-(8*2)];
        }
    }

    /**
     * 创建一个新的根节点的字节数组。
     * 根据传入的左右子节点uid和键值创建一个新的根节点字节数组。
     * @param left
     * @param right
     * @param key
     * @return
     */
    static byte[] newRootRaw(long left, long right, long key)  {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, false);
        setRawNoKeys(raw, 2);
        setRawSibling(raw, 0);
        setRawKthSon(raw, left, 0);
        setRawKthKey(raw, key, 0);
        setRawKthSon(raw, right, 1);
        setRawKthKey(raw, Long.MAX_VALUE, 1);

        return raw.raw;
    }

    /**
     * 创建一个新的空根节点的字节数组。
     * 用于在树中没有任何键值时创建根节点。
     * @return
     */
    static byte[] newNilRootRaw()  {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, true);
        setRawNoKeys(raw, 0);
        setRawSibling(raw, 0);

        return raw.raw;
    }

    /**
     * 加载一个节点。
     * 根据给定的树和节点的唯一标识符，从数据管理器中读取节点数据项，并创建一个新的节点对象。
     * @param bTree
     * @param uid
     * @return
     * @throws Exception
     */
    static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        DataItem di = null;
        try {
            di = bTree.dm.read(uid);//获取根节点的dataitem
        } catch(Exception e) {
            throw e;
        }
        assert di != null;
        Node n = new Node();//创建一个新节点
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;
        return n;
    }

    /**
     * 释放节点。释放节点的数据项。
     */
    public void release() {
        dataItem.release();
    }

    /**
     * 判断节点是否为叶子节点
     * @return
     */
    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawIsLeaf(raw);
        } finally {
            dataItem.rUnLock();
        }
    }

    //封装搜索下一个节点的结果
    class SearchNextRes {
        long uid;
        long siblingUid;
    }

    public SearchNextRes searchNext(long key) {
        dataItem.rLock();
        try {

            SearchNextRes res = new SearchNextRes();
            //获取key数量
            int noKeys = getRawNoKeys(raw);
            //遍历key
            for(int i = 0; i < noKeys; i ++) {
                long ik = getRawKthKey(raw, i);
                //当前key小于给定的key，则将当前key的子节点uid赋值给搜索结果的uid，并将siblingUid设置为0
                if(key < ik) {
                    res.uid = getRawKthSon(raw, i);
                    res.siblingUid = 0;
                    return res;
                }
            }
            //如果遍历完所有key后仍未找到小于给定key的key，
            // 则将最后一个key的兄弟节点uid赋值给搜索结果的siblingUid，并将uid设置为0
            res.uid = 0;
            res.siblingUid = getRawSibling(raw);
            return res;

        } finally {
            dataItem.rUnLock();
        }
    }
    //搜索叶子节点键值范围
    class LeafSearchRangeRes {
        List<Long> uids;
        long siblingUid;
    }

    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rLock();
        try {
            //
            int noKeys = getRawNoKeys(raw);
            int kth = 0;
            //找到第一个大于等于leftkey所在位置
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik >= leftKey){
                    break;
                }
                kth ++;
            }
            List<Long> uids = new ArrayList<>();
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik <= rightKey) {
                    uids.add(getRawKthSon(raw, kth));
                    kth ++;
                } else {
                    break;
                }
            }
            long siblingUid = 0;
            if(kth == noKeys) {
                //右相邻的叶子节点
                siblingUid = getRawSibling(raw);
            }
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            res.siblingUid = siblingUid;
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }

    //插入节点
    class InsertAndSplitRes {
        long siblingUid, newSon, newKey;
    }

    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        boolean success = false;
        Exception err = null;
        InsertAndSplitRes res = new InsertAndSplitRes();

        dataItem.before();
        try {
            success = insert(uid, key);
            if(!success) {
                res.siblingUid = getRawSibling(raw);
                return res;
            }
            if(needSplit()) {
                try {
                    SplitRes r = split();
                    res.newSon = r.newSon;
                    res.newKey = r.newKey;
                    return res;
                } catch(Exception e) {
                    err = e;
                    throw e;
                }
            } else {
                return res;
            }
        } finally {
            if(err == null && success) {
                dataItem.after(TransactionManagerImpl.SUPER_XID);
            } else {
                dataItem.unBefore();
            }
        }
    }

    private boolean insert(long uid, long key) {
        int noKeys = getRawNoKeys(raw);
        int kth = 0;
        while(kth < noKeys) {
            long ik = getRawKthKey(raw, kth);
            if(ik < key) {
                kth ++;
            } else {
                break;
            }
        }
        //遍历完了还没找到，且存在兄弟节点，则插入失败
        if(kth == noKeys && getRawSibling(raw) != 0) return false;

        if(getRawIsLeaf(raw)) {
            shiftRawKth(raw, kth);
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth);
            setRawNoKeys(raw, noKeys+1);
        } else {
            //不是叶子节点，需要继续往下搜索
            long kk = getRawKthKey(raw, kth);
            setRawKthKey(raw, key, kth);
            shiftRawKth(raw, kth+1);
            setRawKthKey(raw, kk, kth+1);
            setRawKthSon(raw, uid, kth+1);
            setRawNoKeys(raw, noKeys+1);
        }
        return true;
    }

    private boolean needSplit() {
        return BALANCE_NUMBER*2 == getRawNoKeys(raw);
    }

    class SplitRes {
        long newSon, newKey;
    }

    private SplitRes split() throws Exception {
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(nodeRaw, getRawIsLeaf(raw));
        setRawNoKeys(nodeRaw, BALANCE_NUMBER);
        setRawSibling(nodeRaw, getRawSibling(raw));
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);
        long son = 0;
        try {
            son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw);
        } catch(Exception e) {
            throw e;
        }
        setRawNoKeys(raw, BALANCE_NUMBER);
        setRawSibling(raw, son);

        SplitRes res = new SplitRes();
        res.newSon = son;
        res.newKey = getRawKthKey(nodeRaw, 0);
        return res;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIsLeaf(raw)).append("\n");
        int KeyNumber = getRawNoKeys(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for(int i = 0; i < KeyNumber; i ++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }
}
