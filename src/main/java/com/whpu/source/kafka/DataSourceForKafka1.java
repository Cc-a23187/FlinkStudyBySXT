package com.whpu.source.kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author cc
 * @create 2020-07-28-15:05
 * 读取字符串形式的 kafka
 */
public class DataSourceForKafka1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //修改并行度 , 开启一个线程
        env.setParallelism(1);
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
        props.setProperty("bootstrap.servers", "master:9092");
        props.setProperty("zookeeper.connect", "master:2181");
        props.setProperty("group.id", "flink-test");
        //指定数据源
        DataStream<String> stream = env.
                addSource(new FlinkKafkaConsumer010<String>("first",new SimpleStringSchema(),props));
        //数据处理
        stream.map(new MapFunction<String, String>() {
                       @Override
                       public String map(String input) throws Exception {
                           return input+"---first";
                       }
                   }).print();
        env.execute();
    }

}
