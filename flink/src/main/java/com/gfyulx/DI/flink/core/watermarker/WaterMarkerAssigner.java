package com.gfyulx.DI.flink.core.watermarker;

import com.gfyulx.DI.flink.core.table.SourceTableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

/**
 * @ClassName:  WaterMarkerAssigner
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/8 14:23
 *
 * @Copyright: 2018 gfyulx
 *
 */

public class WaterMarkerAssigner {

    public boolean checkNeedAssignWaterMarker(SourceTableInfo tableInfo){
        if(Strings.isNullOrEmpty(tableInfo.getEventTimeField())){
            return false;
        }

        return true;
    }

    public DataStream assignWaterMarker(DataStream<Row> dataStream, RowTypeInfo typeInfo, String eventTimeFieldName, int maxOutOfOrderness){

        String[] fieldNames = typeInfo.getFieldNames();
        TypeInformation<?>[] fieldTypes = typeInfo.getFieldTypes();

        if(Strings.isNullOrEmpty(eventTimeFieldName)){
            return dataStream;
        }

        int pos = -1;
        for(int i=0; i<fieldNames.length; i++){
            if(eventTimeFieldName.equals(fieldNames[i])){
                pos = i;
            }
        }

        Preconditions.checkState(pos != -1, "can not find specified eventTime field:" +
                eventTimeFieldName + " in defined fields.");

        TypeInformation fieldType = fieldTypes[pos];

        BoundedOutOfOrdernessTimestampExtractor waterMarker = null;
        if(fieldType.getTypeClass().getTypeName().equalsIgnoreCase("java.sql.Timestamp")){
            waterMarker = new CustomerWaterMarkerForTimeStamp(Time.milliseconds(maxOutOfOrderness), pos);
        }else if(fieldType.getTypeClass().getTypeName().equalsIgnoreCase("java.lang.Long")){
            waterMarker = new CustomerWaterMarkerForLong(Time.milliseconds(maxOutOfOrderness), pos);
        }else{
            throw new IllegalArgumentException("not support type of " + fieldType + ", current only support(timestamp, long).");
        }

        return dataStream.assignTimestampsAndWatermarks(waterMarker);
    }
}
