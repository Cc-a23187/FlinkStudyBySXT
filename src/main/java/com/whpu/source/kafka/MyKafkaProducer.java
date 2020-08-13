package com.whpu.source.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * @author cc
 * @create 2020-07-29-11:51
 */
public class MyKafkaProducer {
    public static void main(String[] args) throws InterruptedException {
        //1、创建kafka生产者配置信息
        Properties props = new Properties();

        //2、指定连接的kafka集群
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092,slave1:9092,slave2:9092");

        //3、ACK相应级别
        props.put(ProducerConfig.ACKS_CONFIG,"-1");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("group.id", "flink-test2");
        props.setProperty("auto.offset.reset","latest");
        //9、创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        while (true){
            //10、发送数据
            producer.send(new ProducerRecord<String, String>("first","myKey--"+ new Random().nextInt(10),"myValue--"+ new Random().nextInt(20)));
            //.get调用同步加载方法，实际是使用多线程中的future方法，使用sender线程的时候，将main阻塞
            Thread.sleep(1000);

        }

    }
}
