package com.gfyulx.DI.flink.core.side.cache;

import com.gfyulx.DI.flink.core.side.SideTableInfo;

/**
 * @ClassName:  AbsSideCache
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/9 14:09
 *
 * @Copyright: 2018 gfyulx
 *
 */
public abstract class AbsSideCache {

    protected SideTableInfo sideTableInfo;

    public AbsSideCache(SideTableInfo sideTableInfo){
        this.sideTableInfo = sideTableInfo;
    }

    public abstract void initCache();

    public abstract CacheObj getFromCache(String key);

    public abstract void putCache(String key, CacheObj value);
}
