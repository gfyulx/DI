package com.gfyulx.DI.flink.core.source;

import com.gfyulx.DI.flink.core.table.SourceTableInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
/**
 * @ClassName:  IStreamSourceGener
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/6 9:35
 *
 * @Copyright: 2018 gfyulx
 *
 */
public interface IStreamSourceGener<T> {

    /**
     * @param sourceTableInfo
     * @param env
     * @param tableEnv
     * @return
     */
    T genStreamSource(SourceTableInfo sourceTableInfo, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv);

}
