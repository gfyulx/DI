package com.gfyulx.DI.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @ClassName: FlinkApplication
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date: 2018/9/20 10:16
 * @Copyright: 2018 gfyulx
 */
public class FlinkApplication {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        dataStream.print();

        env.execute("Window WordCount");
        StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> input = env1.fromElements(1, 2, 3);

        // collector type must be declared
        input.flatMap((Integer number, Collector<String> out) -> {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < number; i++) {
                builder.append("a");
                out.collect(builder.toString());
            }
        }).returns(Types.STRING).print();
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
