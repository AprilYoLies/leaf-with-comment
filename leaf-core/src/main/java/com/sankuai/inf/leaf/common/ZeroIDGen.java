package com.sankuai.inf.leaf.common;

import com.sankuai.inf.leaf.IDGen;

/**
 * 默认的 id 生成器，它只会产生 0 值
 */
public class ZeroIDGen implements IDGen {
    @Override
    public Result get(String key) {
        return new Result(0, Status.SUCCESS);
    }

    @Override
    public boolean init() {
        return true;
    }
}
