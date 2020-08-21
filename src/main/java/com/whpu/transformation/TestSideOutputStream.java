package com.whpu.transformation;

import com.whpu.source.myself.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author cc
 * @create 2020-08-06-17:05
 */
public class TestSideOutputStream {

    public static void main(String[] args) throws Exception {
        CreateSideOutputStream createSideOutputStream = new CreateSideOutputStream();
        //初始化Flink的Streaming（流计算）上下文执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取数据
        String filepath= "H:\\work\\亚信\\Flink\\FlinkForJavaTest2\\src\\main\\resources\\station.log";
        DataStream<StationLog> dataStream = env.readTextFile(filepath)
                .map(new MapFunction<String, StationLog>() {
                    @Override
                    public StationLog map(String s) throws Exception {
                        String[] arr =s.split(",");
                        StationLog stationLog = new StationLog(arr[0].trim(),arr[1].trim(),arr[2].trim(),arr[3].trim(),Long.valueOf(arr[4].trim()),Long.valueOf(arr[5].trim()));
                        stationLog.setSid(arr[0].trim());
                        return stationLog;
                    }
                });
        //侧流计算
        SingleOutputStreamOperator<StationLog> process = dataStream.process(createSideOutputStream);

        process.print("主流");

        DataStream<StationLog> sideOutput = process.getSideOutput(createSideOutputStream.notSuccessTag);

        sideOutput.print("侧流");

        env.execute();

    }

}
