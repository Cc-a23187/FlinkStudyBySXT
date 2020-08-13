package com.whpu.state;

import com.whpu.source.myself.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author cc
 * @create 2020-08-10-14:32
 * 需求：计算每个手机的呼叫间隔时间，单位是毫秒ms。
 */
public class TestKeyedState2 {
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
        })//在scala中，可以使用mapwithstate方法去处理；
        .print();
        env.execute();
    }

}
