package com.whpu.transformation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author cc
 * @create 2020-08-05-15:27
 * 通过使用Connect连接算子，将两个不同数据类型的流合并在一起，形成一个格式为ConnectedStream的数据集
 */
public class TestConnect {
    public static void main(String[] args) throws Exception {
        //初始化Flink的Streaming（流计算）上下文执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取数据
        DataStream stream1 = env.fromElements((1),(2),(3));
        DataStream stream2 = env.fromElements(4,2);
        DataStream stream3 = env.fromElements("a","b","c");
        DataStream stream4 = env.fromElements(7,2);
        DataStream<Tuple2<String,Integer>> stream5 = env.fromElements(new Tuple2<>("a",1),new Tuple2<>("b",2));
        //transformation
        ConnectedStreams<Integer, String> result = stream1.connect(stream3);
        result.map(new CoMapFunction<Integer, String, Object>() {

                       @Override
                       public Object map1(Integer integer) throws Exception {
                           return integer;
                       }

                       @Override
                       public Object map2(String s) throws Exception {
                           return s;
                       }
                   }

        ).print();
        stream5.print();
        env.execute();
    }
}
