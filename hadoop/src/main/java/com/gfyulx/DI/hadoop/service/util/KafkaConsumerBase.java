package com.gfyulx.DI.hadoop.service.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * @ClassName: KafkaConsumerBase
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date: 2018/9/3 13:41
 * @Copyright: 2018 gfyulx
 */
public class KafkaConsumerBase {
    private String topic;
    Properties config;

    public void consumer() {
        ConsumerConnector consumer = createConsumer();
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1); // 1表示consumer thread线程数量 ,线程大于1时，kafKaStream返回的是List
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);// 获取每次接收到的这个数据,大于1时返回List
        //List<KafkaStream<byte[], byte[]>> streaList = messageStreams.get(topic);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
        while (iterator.hasNext()) {
            String message = new String(iterator.next().message());
            //Do something
            System.out.println("接收到: " + message);
        }
    }

    private ConsumerConnector createConsumer() {
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(config));
    }

    public void setConsumerConfig() throws Exception {
        /**
         * 属性参数,设置
         * 方式1：config.setProperty("producer.type", "sync"); 可以手动设置生产者参数
         * 方式2：直接加载kafka集群中的producer.properties
         * 在producer.properties中指定broker List
         * 		metadata.broker.list=shb01:9092,129.168.79.139:9092
         * 	必须包含:
         * 	zookeeper.connect
         * 	group.id
         * 	topic
         * 	         */
        Properties conf = new Properties();
        conf.load(KafkaProducerBase.class.getClassLoader().getResourceAsStream("consumer.properties"));
        this.config = conf;
    }

}
