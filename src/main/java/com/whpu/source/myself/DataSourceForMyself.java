package com.whpu.source.myself;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

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
        //每2秒钟处理一次数据
         /*windowAll方法Global Window：如果是 Non-Keyed 类型，则调用 WindowsAll()方法，
         所有的数据都会在窗口算子中由到一个 Task 中计算，并得到全局统计结果。*/
        stream.timeWindowAll(Time.seconds(2));
        //数据处理
        stream.print();
        env.execute();
    }

}
