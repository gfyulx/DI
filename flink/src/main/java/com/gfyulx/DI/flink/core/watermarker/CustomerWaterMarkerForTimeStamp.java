package com.gfyulx.DI.flink.core.watermarker;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

/**
 * @ClassName:  CustomerWaterMarkerForTimeStamp
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/7 14:23
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class CustomerWaterMarkerForTimeStamp extends BoundedOutOfOrdernessTimestampExtractor<Row> {

    private static final Logger logger = LoggerFactory.getLogger(CustomerWaterMarkerForTimeStamp.class);

    private static final long serialVersionUID = 1L;

    private int pos;

    private long lastTime = 0;

    public CustomerWaterMarkerForTimeStamp(Time maxOutOfOrderness, int pos) {
        super(maxOutOfOrderness);
        this.pos = pos;
    }

    @Override
    public long extractTimestamp(Row row) {
        try {
            Timestamp time = (Timestamp) row.getField(pos);
            lastTime = time.getTime();
            return time.getTime();
        } catch (RuntimeException e) {
            logger.error("", e);
        }
        return lastTime;
    }
}
