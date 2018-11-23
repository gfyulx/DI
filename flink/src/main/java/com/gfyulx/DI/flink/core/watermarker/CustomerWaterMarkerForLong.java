package com.gfyulx.DI.flink.core.watermarker;

import com.gfyulx.DI.flink.core.util.MathUtil;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName:  CustomerWaterMarkerForLong
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/7 14:23
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class CustomerWaterMarkerForLong extends BoundedOutOfOrdernessTimestampExtractor<Row> {

    private static final Logger logger = LoggerFactory.getLogger(CustomerWaterMarkerForLong.class);

    private static final long serialVersionUID = 1L;

    private int pos;

    private long lastTime = 0;

    public CustomerWaterMarkerForLong(Time maxOutOfOrderness, int pos) {
        super(maxOutOfOrderness);
        this.pos = pos;
    }

    @Override
    public long extractTimestamp(Row row) {

        try{
            Long eveTime = MathUtil.getLongVal(row.getField(pos));
            lastTime = eveTime;
            return eveTime;
        }catch (Exception e){
            logger.error("", e);
        }

        return lastTime;
    }
}
