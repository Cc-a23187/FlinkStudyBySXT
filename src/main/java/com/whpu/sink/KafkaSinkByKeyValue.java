package com.whpu.sink;

import com.whpu.WordCountPi;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.*;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;


/**
 * @author cc
 * @create 2020-07-31-16:47
 * kafka做sink的第二种（k、v）
 *
 */
public class KafkaSinkByKeyValue {
    public static void main(String[] args) throws Exception {
//初始化Flink的Streaming（流计算）上下文执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取数据
        DataStream<String> dataStream = env.socketTextStream("master", 8888);
        //计算
        DataStream<Tuple2<String, Integer>> words = dataStream
                .flatMap(new WordCountPi.LineSplitter())
                .keyBy(0)
                .sum(1);

        //1、创建kafka生产者配置信息
        Properties props = new Properties();

        //2、指定连接的kafka集群
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave1:9092,slave2:9092");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
//        props.setProperty("group.id", "flink-kafka-sink");
//        props.setProperty("auto.offset.reset", "earliest");

//        KafkaSerializationSchema<Tuple2<String, Integer>> kafkaKV = new KafkaSerializationSchema<Tuple2<String, Integer>>() {
//
//            @Override
//            public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Integer> stringIntegerTuple2, @Nullable Long aLong) {
//                ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>("kafkaKV",
//                        stringIntegerTuple2.f0.getBytes(), (stringIntegerTuple2.f1 + " ").getBytes());
//                return producerRecord;
//            }
//
//        };

        words.addSink(new FlinkKafkaProducer011<Tuple2<String, Integer>>("kafkaKV", new KeyedSerializationSchema<Tuple2<String, Integer>>() {
            @Override
            public byte[] serializeKey(Tuple2<String, Integer> stringIntegerTuple2) {
                return stringIntegerTuple2.f0.getBytes();
            }

            @Override
            public byte[] serializeValue(Tuple2<String, Integer> stringIntegerTuple2) {
                return stringIntegerTuple2.f1.toString().getBytes();
            }

            @Override
            public String getTargetTopic(Tuple2<String, Integer> stringIntegerTuple2) {
                return "kafkaKV";
            }
        }, props));
        env.execute();
    }
}
