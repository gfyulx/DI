package com.gfyulx.DI.flink.core.side;

import com.gfyulx.DI.flink.core.table.TableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.io.Serializable;

/**
 * @ClassName:  SideTableInfo
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/8 14:13
 *
 * @Copyright: 2018 gfyulx
 *
 */
public abstract class SideTableInfo extends TableInfo implements Serializable {

    public static final String TARGET_SUFFIX = "Side";

    public static final String CACHE_KEY = "cache";

    public static final String CACHE_SIZE_KEY = "cacheSize";

    public static final String CACHE_TTLMS_KEY = "cacheTTLMs";

    public static final String PARTITIONED_JOIN_KEY = "partitionedJoin";

    private String cacheType = "none";//None or LRU or ALL

    private int cacheSize = 10000;

    private long cacheTimeout = 60 * 1000;//

    private boolean partitionedJoin = false;

    public RowTypeInfo getRowTypeInfo(){
        Class[] fieldClass = getFieldClasses();
        TypeInformation<?>[] types = new TypeInformation[fieldClass.length];
        String[] fieldNames = getFields();
        for(int i=0; i<fieldClass.length; i++){
            types[i] = TypeInformation.of(fieldClass[i]);
        }

        return new RowTypeInfo(types, fieldNames);
    }

    public String getCacheType() {
        return cacheType;
    }

    public void setCacheType(String cacheType) {
        this.cacheType = cacheType;
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }

    public long getCacheTimeout() {
        return cacheTimeout;
    }

    public void setCacheTimeout(long cacheTimeout) {
        this.cacheTimeout = cacheTimeout;
    }

    public boolean isPartitionedJoin() {
        return partitionedJoin;
    }

    public void setPartitionedJoin(boolean partitionedJoin) {
        this.partitionedJoin = partitionedJoin;
    }
}
