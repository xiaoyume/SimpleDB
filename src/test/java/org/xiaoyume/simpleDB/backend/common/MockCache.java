package org.xiaoyume.simpleDB.backend.common;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/17 21:58
 */
public class MockCache extends AbstractCache<Long>{
    public MockCache(){
        super(50);
    }

    @Override
    protected Long getForCache(long key) throws Exception {
        return key;
    }

    @Override
    protected void releaseForCache(Long obj) {

    }
}
