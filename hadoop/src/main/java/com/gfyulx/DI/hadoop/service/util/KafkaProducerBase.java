package com.gfyulx.DI.hadoop.service.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


/**
 * @ClassName: KafkaProducerBase
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date: 2018/8/30 13:56
 * @Copyright: 2018 gfyulx
 */

/**useage
    kafkaProducer=new KafkaProducer<Interger,String>
    kafkaProducer.setProducerConfig() or kafkaProducer.setProducerConfig(xxx=xxx)
    kafkaProducer.connect()
    kafkaProducer.send(xxxxxx)
    kafkaProducer.close()
 */
public class KafkaProducerBase<K,V>  extends KafkaBase{
    Properties config = null;
    private String topic = null;
    private List<String> brokerList = new ArrayList<>();
    private String acks = "all";   //0--不确认， 1--server服务器保存后即确认 -1（all):所有副本都接收后确认
    private Long batchSize = (long) 10240;
    private Long bufferMemory = (long) 1048576;
    private String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
    private String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";
    private Integer retires=0;
    //linger.ms  缓冲区刷新间隔
    //serializer.class   解码类：kafka.serializer.StringEncoder 支持String;kafka.serializer.StringEncoder支持Bytes[]
    Producer <K,V> producer;

    public void setProducerConfig() throws Exception {
        /**
         * 属性参数,设置
         * 方式1：config.setProperty("producer.type", "sync"); 可以手动设置生产者参数
         * 方式2：直接加载kafka集群中的producer.properties
         * 在producer.properties中指定broker List
         * 		metadata.broker.list=shb01:9092,129.168.79.139:9092
         */
        Properties conf = new Properties();
        conf.load(KafkaProducerBase.class.getClassLoader().getResourceAsStream("producer.properties"));
        this.config = conf;
    }

    /*设置producer的连接值，需要在创建produce对象前设置*/
    public void setProducerConfig(String keyValue) throws Exception{
        if (config==null){
            System.out.println("load configure producer.properties");
            this.setProducerConfig();
        }
        if (keyValue.contains("=")) {
            String[] splitStr = keyValue.split("=");
            if (splitStr.length == 2) {
                config.setProperty(splitStr[0], splitStr[1]);
            }
        }
    }

    public Producer<K, V>connect() throws Exception {
        ProducerConfig pConfig = new ProducerConfig(config);
        Producer<K,V> produce = new Producer<K,V>(pConfig);
        this.producer=produce;
        return produce;
    }

    public int send(K key,V msg) throws Exception {
        //Producer produce=connect();
        List<KeyedMessage<K, V>> messageList = new ArrayList<KeyedMessage<K, V>>();
        messageList.add(new KeyedMessage<K,V>(config.getProperty("topic"),key,msg));
        this.producer.send(messageList);
        return 0;
    }
    //关闭
    public void close(){
        producer.close();
    }
}
