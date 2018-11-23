
package com.gfyulx.DI.flink.core.side;

import com.gfyulx.DI.flink.core.threadFactory.DTThreadFactory;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName:  AllReqRow
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/8 14:11
 *
 * @Copyright: 2018 gfyulx
 *
 */
public abstract class AllReqRow extends RichFlatMapFunction<Row, Row> {

    protected SideInfo sideInfo;

    private ScheduledExecutorService es;

    public AllReqRow(SideInfo sideInfo){
        this.sideInfo = sideInfo;

    }

    protected abstract Row fillData(Row input, Object sideInput);

    protected abstract void initCache() throws SQLException;

    protected abstract void reloadCache();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        initCache();
        System.out.println("----- all cacheRef init end-----");

        //start reload cache thread
        SideTableInfo sideTableInfo = sideInfo.getSideTableInfo();
        es = Executors.newSingleThreadScheduledExecutor(new DTThreadFactory("cache-all-reload"));
        es.scheduleAtFixedRate(() -> reloadCache(), sideTableInfo.getCacheTimeout(), sideTableInfo.getCacheTimeout(), TimeUnit.MILLISECONDS);
    }

}
