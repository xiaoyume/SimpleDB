package org.xiaoyume.simpleDB.backend.im;

import org.xiaoyume.simpleDB.backend.common.SubArray;
import org.xiaoyume.simpleDB.backend.dm.DataManager;
import org.xiaoyume.simpleDB.backend.dm.dataItem.DataItem;
import org.xiaoyume.simpleDB.backend.tm.TransactionManagerImpl;
import org.xiaoyume.simpleDB.backend.utils.Parser;
import org.xiaoyume.simpleDB.backend.im.Node.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author xiaoy
 * @version 1.0
 * @description: b+
 * @date 2024/2/28 19:43
 */
public class BPlusTree {
    //管理数据
    DataManager dm;
    //保存树根节点uid的数据项的 数据uid,需要一个数据项来专门保存根节点uid的，这个uid
    long bootUid;
    //根节点数据
    DataItem bootDataItem;
    //bootDataItem的锁
    Lock bootLock;

    public static long create(DataManager dm) throws Exception {
        //创建一个根节点
        byte[] rawRoot = Node.newNilRootRaw();
        long rootUid = 0;
        try {
            //插入根节点
            rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);
        } catch(Exception e) {
            throw e;
        }
        long bootUid = 0;
        try {
            bootUid = dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
        } catch(Exception e) {
            throw e;
        }
        return bootUid;
    }

    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        DataItem bootDataItem = null;
        try {
            bootDataItem = dm.read(bootUid);
        } catch(Exception e) {
            throw e;
        }
        assert bootDataItem != null;
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.dm = dm;
        t.bootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }

    private long rootUid() {
        bootLock.lock();
        try {
            SubArray sa = bootDataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start+8));
        } finally {
            bootLock.unlock();
        }
    }

    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            long newRootUid = 0;
            try {
                newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);
            } catch(Exception e) {
                throw e;
            }
            bootDataItem.before();
            SubArray diRaw = bootDataItem.data();
            System.arraycopy(Parser.long2Byte(newRootUid), 0, diRaw.raw, diRaw.start, 8);
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
        } finally {
            bootLock.unlock();
        }
    }

    private long searchLeaf(long nodeUid, long key) throws Exception {
        Node node = null;
        try {
            node = Node.loadNode(this, nodeUid);
        } catch(Exception e) {
            throw e;
        }
        boolean isLeaf = node.isLeaf();
        node.release();

        if(isLeaf) {
            return nodeUid;
        } else {
            long next = 0;
            try {
                next = searchNext(nodeUid, key);
            } catch(Exception e) {
                throw e;
            }
            return searchLeaf(next, key);
        }
    }

    private long searchNext(long nodeUid, long key) throws Exception {
        while(true) {
            Node node = null;
            try {
                node = Node.loadNode(this, nodeUid);
            } catch(Exception e) {
                throw e;
            }
            SearchNextRes res = node.searchNext(key);
            node.release();
            if(res.uid != 0) return res.uid;
            nodeUid = res.siblingUid;
        }
    }

    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }

    private List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = rootUid();
        long leafUid = 0;
        try {
            leafUid = searchLeaf(rootUid, leftKey);
        } catch(Exception e) {
            throw e;
        }
        List<Long> uids = new ArrayList<>();
        while(true) {
            Node leaf = null;
            try {
                leaf = Node.loadNode(this, leafUid);
            } catch(Exception e) {
                throw e;
            }
            LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);
            if(res.siblingUid == 0) {
                break;
            } else {
                leafUid = res.siblingUid;
            }
        }
        return uids;
    }

    public void insert(long key, long uid) throws Exception {
        long rootUid = rootUid();
        InsertRes res = null;
        try {
            res = insert(rootUid, uid, key);
        } catch(Exception e) {
            throw e;
        }
        assert res != null;
        if(res.newNode != 0) {
            try {
                updateRootUid(rootUid, res.newNode, res.newKey);
            } catch(Exception e) {
                throw e;
            }
        }
    }

    class InsertRes {
        long newNode, newKey;
    }

    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        Node node = null;
        try {
            node = Node.loadNode(this, nodeUid);//加载根节点
        } catch(Exception e) {
            throw e;
        }
        boolean isLeaf = node.isLeaf();
        node.release();//释放根节点

        InsertRes res = null;
        if(isLeaf) {
            try {
                res = insertAndSplit(nodeUid, uid, key);
            } catch(Exception e) {
                throw e;
            }
            assert res != null;
        } else {
            long next = 0;
            try {
                next = searchNext(nodeUid, key);
            } catch(Exception e) {
                throw e;
            }

            InsertRes ir = null;
            try {
                ir = insert(next, uid, key);
            } catch(Exception e) {
                throw e;
            }
            if(ir.newNode != 0) {
                try {
                    res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
                } catch(Exception e) {
                    throw e;
                }
            } else {
                res = new InsertRes();
            }
            assert res != null;
        }
        return res;
    }

    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while(true) {
            Node node = null;
            try {
                node = Node.loadNode(this, nodeUid);
            } catch(Exception e) {
                throw e;
            }
            InsertAndSplitRes iasr = null;
            try {
                iasr = node.insertAndSplit(uid, key);
            } catch(Exception e) {
                throw e;
            }
            node.release();
            if(iasr.siblingUid != 0) {
                nodeUid = iasr.siblingUid;
            } else {
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res;
            }
        }
    }

    public void close() {
        bootDataItem.release();
    }
}
