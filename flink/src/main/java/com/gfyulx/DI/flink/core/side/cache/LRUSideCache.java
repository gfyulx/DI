package com.gfyulx.DI.flink.core.side.cache;

import com.gfyulx.DI.flink.core.side.SideTableInfo;
import org.apache.flink.calcite.shaded.com.google.common.cache.Cache;
import org.apache.flink.calcite.shaded.com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

/**
 * @ClassName:  LRUSideCache
 * @Description: LRU缓存策略
 * @date:   2018/11/9 14:10
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class LRUSideCache extends AbsSideCache{

    protected transient Cache<String, CacheObj> cache;

    public LRUSideCache(SideTableInfo sideTableInfo) {
        super(sideTableInfo);
    }

    @Override
    public void initCache() {
        //当前只有LRU
        cache = CacheBuilder.newBuilder()
                .maximumSize(sideTableInfo.getCacheSize())
                .expireAfterWrite(sideTableInfo.getCacheTimeout(), TimeUnit.MILLISECONDS)
                .build();
    }

    @Override
    public CacheObj getFromCache(String key) {
        if(cache == null){
            return null;
        }

        return cache.getIfPresent(key);
    }

    @Override
    public void putCache(String key, CacheObj value) {
        if(cache == null){
            return;
        }

        cache.put(key, value);
    }
}
