package com.whpu.source.myself;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author cc
 * @create 2020-07-28-15:05
 * 自定义数据源 形式； 需求： 每隔两秒生成十条数据
 */
public class DataSourceForMyself {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //修改并行度 , 开启一个线程
        env.setParallelism(1);
        //指定数据源
        DataStream<StationLog> stream = env.addSource(new MyConsumerDataSource());
        //数据处理
        stream.print();
        env.execute();
    }

}
