package com.gfyulx.DI.flink.core.table;

import com.gfyulx.DI.flink.core.enums.ECacheType;
import com.gfyulx.DI.flink.core.side.SideTableInfo;
import com.gfyulx.DI.flink.core.util.MathUtil;

import java.util.Map;

/**
 * @ClassName:  AbsSideTableParser
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/9 11:17
 *
 * @Copyright: 2018 gfyulx
 *
 */

public abstract class AbsSideTableParser extends AbsTableParser {

    //Analytical create table attributes ==> Get information cache
    protected void parseCacheProp(SideTableInfo sideTableInfo, Map<String, Object> props){
        if(props.containsKey(SideTableInfo.CACHE_KEY.toLowerCase())){
            String cacheType = MathUtil.getString(props.get(SideTableInfo.CACHE_KEY.toLowerCase()));
            if(cacheType == null){
                return;
            }

            if(!ECacheType.isValid(cacheType)){
                throw new RuntimeException("can't not support cache type :" + cacheType);
            }

            sideTableInfo.setCacheType(cacheType);
            if(props.containsKey(SideTableInfo.CACHE_SIZE_KEY.toLowerCase())){
                Integer cacheSize = MathUtil.getIntegerVal(props.get(SideTableInfo.CACHE_SIZE_KEY.toLowerCase()));
                if(cacheSize < 0){
                    throw new RuntimeException("cache size need > 0.");
                }
                sideTableInfo.setCacheSize(cacheSize);
            }

            if(props.containsKey(SideTableInfo.CACHE_TTLMS_KEY.toLowerCase())){
                Long cacheTTLMS = MathUtil.getLongVal(props.get(SideTableInfo.CACHE_TTLMS_KEY.toLowerCase()));
                if(cacheTTLMS < 1000){
                    throw new RuntimeException("cache time out need > 1000 ms.");
                }
                sideTableInfo.setCacheTimeout(cacheTTLMS);
            }

            if(props.containsKey(SideTableInfo.PARTITIONED_JOIN_KEY.toLowerCase())){
                Boolean partitionedJoinKey = MathUtil.getBoolean(props.get(SideTableInfo.PARTITIONED_JOIN_KEY.toLowerCase()));
                if(partitionedJoinKey){
                    sideTableInfo.setPartitionedJoin(true);
                }
            }
        }
    }
}
