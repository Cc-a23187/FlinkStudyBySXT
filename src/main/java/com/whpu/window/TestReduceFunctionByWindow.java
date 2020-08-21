package com.whpu.window;

import akka.stream.impl.fusing.Log;
import com.whpu.source.myself.MyConsumerDataSource;
import com.whpu.source.myself.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import static sun.misc.Version.print;

/**
 * @author cc
 * @create 2020-08-14-11:23
 * 需求：每隔五秒，统计每个基站日志的数量
 */
public class TestReduceFunctionByWindow {
    public static void main(String[] args) throws Exception {
        //初始化Flink的Streaming（流计算）上下文执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*设置使用EventTime作为Flink的时间处理标准，不指定默认是ProcessTime*/
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        //读取数据
//        DataStream<StationLog> stream = env.addSource(new MyConsumerDataSource());
        //        env.getClass().getResource("/station.log").getPath()
        DataStream<StationLog> dataStream = env.socketTextStream("192.168.242.150",8888)
                .map(new MapFunction<String, StationLog>() {
                    @Override
                    public StationLog map(String s) throws Exception {
                        String[] arr =s.split(",");
                        StationLog stationLog = new StationLog(arr[0].trim(),arr[1].trim(),arr[2].trim(),arr[3].trim(),Long.valueOf(arr[4].trim()),Long.valueOf(arr[5].trim()));
                        stationLog.setSid(arr[0].trim());
                        return stationLog;
                    }
                });//stationLog -> Tuple2.of(stationLog, stationLog.sid
//        dataStream.print("first");
        SingleOutputStreamOperator<Tuple2<String, Integer>> streamSink = dataStream.map(new MapFunction<StationLog, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(StationLog stationLog) throws Exception {
                return Tuple2.of(stationLog.sid,1);
            }
        });
//        streamSink.print("sec");
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = streamSink.keyBy(1)
                .timeWindow(Time.seconds(5))
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                        System.out.println("执行reduce操作：" + t1 + "," + t2);
                        return Tuple2.of(t1.f0, t1.f1 + t2.f1);
                    }
                });
        reduce.print("last");
        env.execute();

    }
}
