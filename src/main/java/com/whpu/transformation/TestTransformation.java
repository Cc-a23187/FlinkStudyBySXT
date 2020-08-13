package com.whpu.transformation;

import com.whpu.source.myself.MyConsumerDataSource;
import com.whpu.source.myself.StationLog;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author cc
 * @create 2020-08-04-15:51
 * 需求：从自定义的数据源，读取基站通话日志；
 *  统计 通话成功的每一个基站，通话成功的总时长是多少秒；
 */
public class TestTransformation {
    public static void main(String[] args) throws Exception {
        //初始化Flink的Streaming（流计算）上下文执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取数据
        DataStream<StationLog> stream = env.addSource(new MyConsumerDataSource());
        //transformation


        SingleOutputStreamOperator text = stream
                .filter(new FilterFunction<StationLog>(){
                    @Override
                    public boolean filter(StationLog stationLog) throws Exception {
                        return stationLog.callType.equals("success");
                    }
                })
                .map(new MapFunction<StationLog, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String,Long> map(StationLog stationLog) throws Exception {
                        Tuple2<String,Long> mapOut = new Tuple2<>();
                        mapOut.f0=stationLog.getSid();
                        mapOut.f1=stationLog.getDuration();
                        return mapOut;
                    }
                })
                .keyBy(0)
//                .keyBy(new KeySelector<StationLog, Object>() {
//                    @Override
//                    public Object getKey(StationLog stationLog) throws Exception {
//
//                        return stationLog.sid,stationLog.duration;
//                    }
//                })
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String,Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2) throws Exception {
                        Tuple2<String,Long> tuple2 = new Tuple2<>();
                        Tuple2<String,Long> tuple3 = new Tuple2<>();
                        tuple3.setFields("xx",1L);
                        long duration =t1.f1+t2.f1;
                        tuple2.f0=t1.f0;
                        tuple2.f1=duration;
                        return tuple2;
                    }
                });

        text.print();
        env.execute();
    }
}
