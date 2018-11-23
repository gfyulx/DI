
package com.gfyulx.DI.flink.core.side;

import com.gfyulx.DI.flink.core.enums.ECacheType;
import com.gfyulx.DI.flink.core.side.cache.AbsSideCache;
import com.gfyulx.DI.flink.core.side.cache.CacheObj;
import com.gfyulx.DI.flink.core.side.cache.LRUSideCache;
import org.apache.calcite.sql.JoinType;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;

import java.util.Collections;

/**
 * @ClassName:  AsyncReqRow
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/8 14:11
 *
 * @Copyright: 2018 gfyulx
 *
 */

public abstract class AsyncReqRow extends RichAsyncFunction<Row, Row> {

    private static final long serialVersionUID = 2098635244857937717L;

    protected SideInfo sideInfo;

    public AsyncReqRow(SideInfo sideInfo){
        this.sideInfo = sideInfo;
    }

    private void initCache(){
        SideTableInfo sideTableInfo = sideInfo.getSideTableInfo();
        if(sideTableInfo.getCacheType() == null || ECacheType.NONE.name().equalsIgnoreCase(sideTableInfo.getCacheType())){
            return;
        }

        AbsSideCache sideCache;
        if(ECacheType.LRU.name().equalsIgnoreCase(sideTableInfo.getCacheType())){
            sideCache = new LRUSideCache(sideTableInfo);
            sideInfo.setSideCache(sideCache);
        }else{
            throw new RuntimeException("not support side cache with type:" + sideTableInfo.getCacheType());
        }

        sideCache.initCache();
    }

    protected CacheObj getFromCache(String key){
        return sideInfo.getSideCache().getFromCache(key);
    }

    protected void putCache(String key, CacheObj value){
        sideInfo.getSideCache().putCache(key, value);
    }

    protected boolean openCache(){
        return sideInfo.getSideCache() != null;
    }


    protected abstract Row fillData(Row input, Object sideInput);

    protected void dealMissKey(Row input, ResultFuture<Row> resultFuture){
        if(sideInfo.getJoinType() == JoinType.LEFT){
            //Reserved left table data
            Row row = fillData(input, null);
            resultFuture.complete(Collections.singleton(row));
        }else{
            resultFuture.complete(null);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        initCache();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
