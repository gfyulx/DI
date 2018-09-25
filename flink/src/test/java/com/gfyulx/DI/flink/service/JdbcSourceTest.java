package com.gfyulx.DI.flink.service;

import com.gfyulx.DI.flink.configuration.JdbcConfiguration;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;

import static org.junit.Assert.*;


public class JdbcSourceTest implements Serializable {
    @Test
    public void TestJdbcSourceTest() throws Exception {
        JdbcConfiguration conf=new JdbcConfiguration();

        conf.setDriver("com.mysql.jdbc.Driver");
        conf.setUrl("jdbc:mysql://192.168.6.188:3306/Test");
        conf.setUsername("streamline");
        conf.setPassword("streamline");
        conf.setSql("select * from record");

        TestStruct testStruct=new TestStruct();
        //1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.从自定义source中读取数据
        DataStream<TestStruct> students=env.addSource(new JdbcSource<TestStruct>(conf,testStruct)).returns(TestStruct.class);
        //3.显示结果
        //students.print();
        /**
        WindowedStream<String, Tuple, TimeWindow> window = students.map(new MapFunction<TestStruct,String>() {
            @Override
            public String map(TestStruct sData) throws Exception {
                String sps = sData.team;
                return sps;
            }
        }).keyBy(0).timeWindow(Time.seconds(5));

        SingleOutputStreamOperator<String> apply = window.apply(new WindowFunction<String, String, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple tuple, TimeWindow window, Iterable<String> input, Collector<String> out) throws Exception {
                input.forEach(x -> {
                    System.out.println("apply function -> " + x);
                    out.collect(x);
                });
            }
        });

        apply.print();
*/
       // students.print();
        //自定义的source不能直接使用.print()方法，改为下面的方式输出。
        //PrintSinkFunction<TestStruct> printFunction = new PrintSinkFunction();
        //students.addSink(printFunction).name("Print to Std. Out");
        DataStream<String> newDS=students.map(new MapFunction<TestStruct, String>() {
            @Override
            public String map(TestStruct testStruct) throws Exception {
                return testStruct.team;
            }
        });
        System.out.println("==========");
        newDS.print();
        //4.触发流执行
        env.execute();

    }

}


