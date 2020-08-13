package com.whpu.source.kafka;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author cc
 * @create 2020-07-28-15:05
 * 使用Tuple2<String, String>数据类型消费kafka数据
 */
public class DataSourceForKafka4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //修改并行度 , 开启一个线程
        env.setParallelism(1);
        //1、创建kafka生产者配置信息
        Properties props = new Properties();

        //2、指定连接的kafka集群
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave1:9092,slave2:9092");

        //3、ACK相应级别
        props.put(ProducerConfig.ACKS_CONFIG, "-1");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("group.id", "flink-test3");
        props.setProperty("auto.offset.reset", "earliest");

        //指定数据源
        DataStreamSource<Tuple2<String, String>> stream = env.
                addSource(new FlinkKafkaConsumer010<Tuple2<String, String>>("first",
                        new KafkaDeserializationSchema<Tuple2<String, String>>() {
                            @Override
                            public boolean isEndOfStream(Tuple2<String, String> stringStringMap) {
                                return false;
                            }

                            @Override
                            public Tuple2<String, String> deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                                String key = "null";
                                String value = "null";

                                if (consumerRecord.key() != null) {
                                    key = new String(consumerRecord.key());
                                }
                                if (consumerRecord.value() != null) {
                                    value = new String(consumerRecord.value());
                                }
                                Tuple2<String, String> data = new Tuple2<>(key,value);
                                return data;

                            }

                            @Override
                            public TypeInformation<Tuple2<String, String>> getProducedType() {
                                return TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
                                });
                            }
                        }, props).setStartFromEarliest());
        //数据处理
        stream.print("dataStream").setParallelism(1);
        env.execute();
    }
}
