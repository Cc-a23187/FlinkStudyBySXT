package com.whpu.window;

import com.whpu.source.myself.MyConsumerDataSource;
import com.whpu.source.myself.StationLog;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author cc
 * @create 2020-08-17-17:00
 * @deprecated 每隔3秒计算最近5秒内，每个基站的日志数量  滑动窗口
 */
public class TestAggregatFunctionByWindow {
    public static void main(String[] args) throws Exception {
        //初始化Flink的Streaming（流计算）上下文执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*设置使用EventTime作为Flink的时间处理标准，不指定默认是ProcessTime*/
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //读取数据
        DataStream<StationLog> dataStream = env.addSource(new MyConsumerDataSource());
        //        env.getClass().getResource("/station.log").getPath()
        /*DataStream<StationLog> dataStream = env.socketTextStream("192.168.242.150",8888)
                .map(new MapFunction<String, StationLog>() {
                    @Override
                    public StationLog map(String s) throws Exception {
                        String[] arr =s.split(",");
                        StationLog stationLog = new StationLog(arr[0].trim(),arr[1].trim(),arr[2].trim(),arr[3].trim(),Long.valueOf(arr[4].trim()),Long.valueOf(arr[5].trim()));
                        stationLog.setSid(arr[0].trim());
                        return stationLog;
                    }
                });//stationLog -> Tuple2.of(stationLog, stationLog.sid*/
        DataStreamSink<Tuple2<String, Long>> streamSink = dataStream.map(new MapFunction<StationLog, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(StationLog stationLog) throws Exception {
                return Tuple2.of(stationLog.sid,1);
            }
        }).keyBy(1
        ).window(SlidingProcessingTimeWindows.of(Time.seconds(5),Time.seconds(3))
                //和 .timeWindow(Time.seconds(5),Time.seconds(3))相同
        ).aggregate(new AggregateFunction<Tuple2<String, Integer>, Tuple2<String,Long>, Tuple2<String,Long>>() {
                        @Override
                        public Tuple2<String, Long> createAccumulator() {
                            return Tuple2.of("",0l);
                        }

                        @Override
                        public Tuple2<String, Long> add(Tuple2<String, Integer> in, Tuple2<String, Long> acc) {
                            return Tuple2.of(in.f0,in.f1+acc.f1);
                        }

                        @Override
                        public Tuple2<String, Long> getResult(Tuple2<String, Long> acc) {
                            return acc;
                        }

                        @Override
                        public Tuple2<String, Long> merge(Tuple2<String, Long> acc, Tuple2<String, Long> acc1) {
                            return Tuple2.of(acc.f0,acc1.f1+acc.f1);
                        }
                    }

        ).print();
        env.execute();

    }
}
