package com.gfyulx.DI.flink.service;

import com.gfyulx.DI.flink.configuration.JdbcConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.io.Serializable;

public class JdbcSinkTest implements Serializable {

    public static void main(String[] args) throws Exception {
        JdbcConfiguration conf=new JdbcConfiguration();

        conf.setDriver("com.mysql.jdbc.Driver");
        conf.setUrl("jdbc:mysql://192.168.6.188:3306/Test");
        conf.setUsername("streamline");
        conf.setPassword("streamline");
        conf.setSql("insert into  record(rid,team,season,victory,negative,ranking) values(?,?,?,?,?,?)");
        //1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.准备数据
        DataStream<TestStruct> tData = env.fromElements(
                new TestStruct(101, "zhangsan", "beijing biejing",2008,15,20),
                new TestStruct(102, "lisi", "tainjing tianjin",2018,20,15)
        );
        //3.将数据写入到自定义的sink中（这里是mysql）
        tData.addSink(new JdbcSink<TestStruct>(conf,new TestStruct()));
        //4.触发流执行
        env.execute();
    }

}