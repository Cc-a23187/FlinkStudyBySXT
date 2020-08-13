package com.whpu.transformation;


import com.whpu.source.myself.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author cc
 * @create 2020-08-06-11:17
 * process api 开发者能控制的 最底层 api
 * 需求：监控所有被叫手机号码，如果这个手机号码，在五秒内，所有呼叫他的日志都是失败的，则发出警告信息；
 */
public class TestProcessFunction {
    public static void main(String[] args) throws Exception {
        //初始化Flink的Streaming（流计算）上下文执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*设置使用EventTime作为Flink的时间处理标准，不指定默认是ProcessTime*/
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(1);
        //读取数据
        DataStream<String> dataStream = env.socketTextStream("192.168.242.150",8888);
/*                .map(new MapFunction<String, StationLog>() {
                    @Override
                    public StationLog map(String s) throws Exception {
                        return null;
                    }
                })
                .keyBy(new KeySelector<StationLog, String>() {
                    @Override
                    public String getKey(StationLog stationLog) throws Exception {
                        return stationLog.getCallIn();
                    }
        }).process(new MonitorCallFail());

        dataStream.print();*/

        DataStream<StationLog> map = dataStream.map(new MapFunction<String, StationLog>() {
                    @Override
                    public StationLog map(String s) throws Exception {
                        String[] arr =s.split(",");
                        StationLog stationLog = new StationLog(arr[0].trim(),arr[1].trim(),arr[2].trim(),arr[3].trim(),Long.valueOf(arr[4].trim()),Long.valueOf(arr[5].trim()));
                        stationLog.setSid(arr[0].trim());
                        return stationLog;
                    }
                });
//        map.print("map");
        KeyedStream<StationLog, String> keyBy = map.keyBy(new KeySelector<StationLog, String>() {
            @Override
            public String getKey(StationLog stationLog) throws Exception {
                return stationLog.getCallIn();
            }
        });
//        keyBy.print("keyBy");
        SingleOutputStreamOperator<String> process = keyBy.process(new MonitorCallFail());

        process.print("process");
        env.execute();
    }
}
