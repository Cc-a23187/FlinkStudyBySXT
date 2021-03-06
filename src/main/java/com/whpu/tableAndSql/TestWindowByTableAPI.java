package com.whpu.tableAndSql;

import com.whpu.source.myself.StationLog;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author cc
 * @create 2020-08-25-9:41
 * @description  基于flink sql的滚动窗口操作
 * 每隔5s统计，每个基站的通话数量，假设数据是乱序。最多延迟3s
 */
public class TestWindowByTableAPI {
    public static void main(String[] args) throws Exception {
        //初始化上下文
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, fsSettings);
        //使用eventTime作为时间语义
        streamEnv.setParallelism(1);
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //获取数据源
        DataStream<StationLog> fileSource = /*streamEnv.addSource(new MyConsumerDataSource());*/
                streamEnv.socketTextStream("master", 8888)
                        .map(line -> {
                            String[] split = line.trim().split(",");
                            return new StationLog(split[0], split[1], split[2], split[3], Long.valueOf(split[4]), Long.valueOf(split[5]));
                        })
                        //引入waterMark让水位线 根据 EventTime 延迟触发 3s
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<StationLog>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(StationLog stationLog) {
                        return stationLog.getCallTime();
                    }
                });
        //从DataStream中创建动态的table，并且可以指定eventTime字段  rowTime
        Table table = tableEnv.fromDataStream(fileSource, "sid,callOut,callIn,callType,callTime.rowTime,duration");
        //每10秒统计每个基站通话成功总时长  使用sql统计
        Table result = table.filter("callType='success'")
                .window(Tumble.over("5.second").on("callTime").as("window"))
                //先按照窗口分组，再按照sid分组
                .groupBy("window,sid")
                .select("sid , window.start,window.end,sid.count,duration.sum");
        /*//滑动窗口写法
        table.window(Slide.over("10.second").every("5.second").on("callTime").as("window"));*/
        tableEnv.toRetractStream(result, Row.class)
                .filter(t -> t.f0)
                .print();

        tableEnv.execute("sql");



    }
}
