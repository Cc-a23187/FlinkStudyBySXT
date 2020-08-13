package com.whpu.transformation;

import com.whpu.source.myself.MyConsumerDataSource;
import com.whpu.source.myself.StationLog;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author cc
 * @create 2020-08-05-16:11
 * 需求：从随机自定义的数据源中读取基站通话日志，把通话成功的和通话失败的分离出来
 */
public class TestSplitAndSelect {
    public static void main(String[] args) throws Exception {
        //初始化Flink的Streaming（流计算）上下文执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取数据
        DataStream<StationLog> stream = env.addSource(new MyConsumerDataSource());
        //切割
        SplitStream<StationLog> splitStream=stream.split(
                stationLog -> {
                    ArrayList<String> strings = new ArrayList<>();

                    if (stationLog.getCallType().equals("success")) {
                        strings.add("Success");
                    } else {
                        strings.add("No Success");
                    }
                    return strings;
                }
        );
        DataStream<StationLog> stream1 = splitStream.select("Success");
        DataStream<StationLog> stream2 = splitStream.select("No Success");
        stream1.print("成功");
        stream2.print("失败");
        env.execute();
    }
}
