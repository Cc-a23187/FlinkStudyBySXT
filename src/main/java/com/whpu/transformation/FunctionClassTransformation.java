package com.whpu.transformation;

import com.whpu.source.myself.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author cc
 * @create 2020-08-05-17:31
 * 需求：按照指定的时间格式输出每个通话的拨号时间  和 结束时间。(通话成功的)
 * 数据源来自本地文件
 *
 * 富函数 比 函数多了生命周期的管理，也能够获取上下文环境；
 */
public class FunctionClassTransformation {
    public static void main(String[] args) throws Exception {
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

        SplitStream<StationLog> splitStream = dataStream.split(stationLog -> {
            ArrayList<String> strings = new ArrayList<>();
            if (stationLog.getCallType().equals("success")) {
                strings.add("Success");
            } else {
                strings.add("No Success");
            }
            return strings;
        });

        DataStream<StationLog> stream = splitStream.select("Success");
        SingleOutputStreamOperator<String> map = stream.map(new MyMapFunction());
        //计算通话成功的起始和结束时间
        map.print("成功");
        env.execute();

    }
}


