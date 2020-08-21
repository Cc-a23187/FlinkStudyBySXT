package com.whpu.time;

import com.whpu.source.myself.MyConsumerDataSource;
import com.whpu.source.myself.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import static sun.misc.Version.print;

/**
 * @author cc
 * @create 2020-08-19-15:31
 * @deprecated  每隔5秒统计一下最近10秒内，每个基站中通话时间最长的一次通话发生的时间还有，
 * (有序时间)    主叫号码，被叫号码，通话时长，并且还得告诉我们当前发生的时间范围（10秒）
 */
public class MaxLongCallTime {

    public static void main(String[] args) throws Exception {
        //env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //①设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //②获取数据源
//        DataStream<StationLog> dataStream = env.addSource(new MyConsumerDataSource())
        SingleOutputStreamOperator<StationLog> dataStream = env.socketTextStream("192.168.242.151",8888)
            .map(new MapFunction<String, StationLog>() {
                    @Override
                public StationLog map(String s) throws Exception {
                    String[] arr =s.split(",");
                    return new StationLog(arr[0].trim(),arr[1].trim(),arr[2].trim(),arr[3].trim(),Long.valueOf(arr[4].trim()),Long.valueOf(arr[5].trim()));
                }
            })
        //③引入waterMark（数据是有序的）
            .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<StationLog>() {
                Long currentMaxTimestamp = 0L;
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

                @Nullable
                @Override
                public Watermark getCurrentWatermark() {
                    return new Watermark(currentMaxTimestamp);
                }

                @Override
                public long extractTimestamp(StationLog stationLog, long l) {
                    long timestamp = stationLog.getCallTime();
                    currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                    System.out.println("key:"+stationLog.getSid()+",eventtime:["+stationLog.getCallTime()+"|"+sdf.format(stationLog.getCallTime())+"],currentMaxTimestamp:["+currentMaxTimestamp+"|"+
                            sdf.format(currentMaxTimestamp)+"],watermark:["+getCurrentWatermark().getTimestamp()+"|"+sdf.format(getCurrentWatermark().getTimestamp())+"]");
                return timestamp;
                }
            });//参数中指定Eventtime具体的值是什么
        //④分组——开窗
        SingleOutputStreamOperator<String> data = dataStream
                .filter(stationLog -> ((stationLog.getCallType().equals("success"))))
                .keyBy("sid")
                .timeWindow(Time.seconds(10), Time.seconds(5))
                .reduce(new MyReduceFunction(), new ReturnMaxTimeWindowFunction());

        data.print();

        env.execute();




    }
}
