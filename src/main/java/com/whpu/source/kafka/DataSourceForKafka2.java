package com.whpu.source.kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.*;

/**
 * @author cc
 * @create 2020-07-28-15:05
 */
public class DataSourceForKafka2 {
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
        props.setProperty("group.id", "flink-test2");
        props.setProperty("auto.offset.reset","earliest");

        //指定数据源
        DataStreamSource<String> stream = env.
                addSource(new FlinkKafkaConsumer010<String>("first",
                        new KafkaDeserializationSchema<String>() {
                    //流是否结束
                    @Override
                    public boolean isEndOfStream(String stringStringMap) {
                        return false;
                    }
                    //反序列化
                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                        String key = "null";
                        String value = "null";
                        if(consumerRecord.key()!=null){
                            key = new String(consumerRecord.key());
                        }
                        if(consumerRecord.value()!=null){
                            value = new String(consumerRecord.value());
                        }
                        return key+":"+value;

                    }
                    //设置返回类型为二元组
                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(new TypeHint<String>() {});
                    }
                }, props).setStartFromEarliest());
        //数据处理
        stream.map(new MapFunction<String, String>() {
            @Override
            public String map(String input) throws Exception {
                return input+"---first";
            }
        }).print();
        stream.print();
        env.execute();
    }
}
