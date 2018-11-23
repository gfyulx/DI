package com.gfyulx.DI.hadoop.service;

import kafka.producer.KeyedMessage;
import kafka.javaapi.producer.Producer;

import kafka.producer.ProducerConfig;

import org.junit.Test;
import org.junit.runner.RunWith;
import com.gfyulx.DI.hadoop.service.util.KafkaProducerBase;
import java.util.ArrayList;
import java.util.Properties;

import static org.junit.Assert.*;

import org.junit.runner.RunWith;

import static org.junit.Assert.*;


public class KafkaProducerBaseTest {
    @Test
    public void KafkaProducerBaseTest() throws Exception {
        Properties config = new Properties();
        config.setProperty("test", "1");
        System.out.println(config.getProperty("test"));
        config.setProperty("test", "2");
        System.out.println(config.getProperty("test"));


        KafkaProducerBase kafkaProducer=new KafkaProducerBase<String,String>();
        kafkaProducer.setProducerConfig();
        //kafkaProducer.setProducerConfig("topic=test");
        //kafkaProducer.setProducerConfig("metadata.broker.list=192.168.6.188:9092");
        //kafkaProducer.setProducerConfig("serializer.class=kafka.serializer.StringEncoder");
        kafkaProducer.connect();
        kafkaProducer.send("1","Tom");
        kafkaProducer.send("2","Tonny");
        kafkaProducer.close();

        /*
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "192.168.6.188:2181");//声明zk
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("metadata.broker.list", "192.168.6.188:9092");// 声明kafka broker
        Producer<Integer, String> producer=new Producer<Integer, String>(new ProducerConfig(properties));
        producer.send(new KeyedMessage<Integer, String>("test", "message: " + 0));
        */
    }

}
