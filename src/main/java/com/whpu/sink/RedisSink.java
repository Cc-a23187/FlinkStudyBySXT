package com.whpu.sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author cc
 * @create 2020-07-30-16:51
 */
public class RedisSink {
    public static void main(String[] args) {
        //初始化Flink的Streaming（流计算）上下文执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取数据
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 8888);
        //转换计算
//        dataStream.flatMap((s, collector) -> ());
    }
}
