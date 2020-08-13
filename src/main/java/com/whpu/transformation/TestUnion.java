package com.whpu.transformation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author cc
 * @create 2020-08-05-14:30
 * 需求：使用union算子实现多条相同类型流的合并；了解其基本原理
 * **算子的类型和格式要一样，如果不同可以使用connect算子
 */
public class TestUnion {
    public static void main(String[] args) throws Exception {
        //初始化Flink的Streaming（流计算）上下文执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取数据
        DataStream stream1 = env.fromElements((1),(2),(3));
        DataStream stream2 = env.fromElements(5,2);
        DataStream stream3 = env.fromElements(6,2);
        DataStream stream4 = env.fromElements(7,2);
        DataStream<Tuple2<String,Integer>> stream5 = env.fromElements(new Tuple2<>("a",1),new Tuple2<>("b",2));
        //transformation
        DataStream result = stream1.union(stream2,stream3,stream4);
        result.print();
        stream5.print();
        env.execute();

    }
}
