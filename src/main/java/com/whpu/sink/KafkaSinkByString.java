package com.whpu.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;

/**
 * @author cc
 * @create 2020-07-31-15:44
 * kafka做为sink的第一种情况(string)
 * 把netcat作为数据源，将输入的数据以大写的形式写入kafka
 */
public class KafkaSinkByString {
    public static void main(String[] args) throws Exception {
        //初始化Flink的Streaming（流计算）上下文执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取数据
        DataStreamSource<String> dataStream = env.socketTextStream("master", 8888);
        //计算
        DataStream<String> words = dataStream
                .flatMap(new FlatMapFunction<String, String>() {
                             @Override
                             public void flatMap(String value, Collector<String> out) throws Exception {
//                                 out.collect(value);
                                 out.collect(value.toUpperCase());
                             }
                         });

        words.addSink(new FlinkKafkaProducer010<String>("master:9092,slave1:9092,slave2:9092","sinkString",new SimpleStringSchema()));

        env.execute("KafkaSinkString");
    }
}
