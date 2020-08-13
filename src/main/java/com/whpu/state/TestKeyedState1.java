package com.whpu.state;

import com.whpu.source.myself.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author cc
 * @create 2020-08-10-14:32
 * 需求：计算每个手机的呼叫间隔时间，单位是毫秒ms。
 */
public class TestKeyedState1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取数据
        String filepath = "H:\\work\\亚信\\Flink\\FlinkForJavaTest2\\src\\main\\resources\\station.log";
        DataStream<StationLog> dataStream = env.readTextFile(filepath)
                .map(new MapFunction<String, StationLog>() {
                    @Override
                    public StationLog map(String s) throws Exception {
                        String[] arr = s.split(",");
                        StationLog stationLog = new StationLog(arr[0].trim(), arr[1].trim(), arr[2].trim(), arr[3].trim(), Long.valueOf(arr[4].trim()), Long.valueOf(arr[5].trim()));
                        stationLog.setSid(arr[0].trim());
                        return stationLog;
                    }
                });
        dataStream.keyBy(new KeySelector<StationLog, String>() {  //按照呼叫的手机号分组
            @Override
            public String getKey(StationLog stationLog) throws Exception {
                return stationLog.callOut;
            }
        }).flatMap(new RichFlatMapFunction<StationLog, Tuple2<Long, Long>>() {
            //定义一个保存前一条呼叫的数据的状态对象
            ValueState<StationLog> preData;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ValueStateDescriptor<StationLog> stateDescriptor = new ValueStateDescriptor<StationLog>("pre", StationLog.class);
                preData = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void flatMap(StationLog in, Collector<Tuple2<Long, Long>> collector) throws Exception {
                StationLog pre = preData.value();
                if (pre == null) { //如果状态中没有，则存入
                    preData.update(in);
                } else { //如果状态中有值则计算时间间隔
                    Long interval = in.callTime - pre.callTime;
                    Tuple2<Long, Long> out = new Tuple2<Long, Long>();
                    out.setFields(in.callTime, interval);
                    collector.collect(out);
                }
            }

        })
        .print();
        env.execute();
    }

}
