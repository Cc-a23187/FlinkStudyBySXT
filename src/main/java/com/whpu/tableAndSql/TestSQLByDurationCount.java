package com.whpu.tableAndSql;

import com.whpu.source.myself.StationLog;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

/**
 * @author cc
 * @create 2020-08-25-11:11
 * 统计每个基站，通话成功的通话的最大时长
 */
public class TestSQLByDurationCount {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, fsSettings);

        /*//获取数据源
        CsvTableSource fileSource = new CsvTableSource(TestSQLByDurationCount.class.getResource("/station.log").getPath(),
                new String[]{"sid", "callOut", "callIn", "callType", "callTime", "duration"},
                new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.LONG, Types.LONG});
        //使用纯粹的sql
        //注册为一张表 该方法没有返回值
        tableEnv.registerTableSource("t_station_log" , fileSource);
        //执行sql
        Table t_station_log = tableEnv.sqlQuery("select sid,sum(duration) as d_c"+
                " from t_station_log where callType = 'success' group by sid");*/

        //使用table api和sql混用
        DataStream<StationLog> stream = streamEnv.readTextFile(TestSQLByDurationCount.class.getResource("/station.log").getPath())
                .map(line -> {
                    String[] split = line.trim().split(",");
                    return new StationLog(split[0], split[1], split[2], split[3], Long.valueOf(split[4]), Long.valueOf(split[5]));
                });
        //以对象的形式获取一张表
        Table table = tableEnv.fromDataStream(stream);

        Table t_station_log = tableEnv.sqlQuery("select sid,sum(duration) as d_c from " + table + " where callType = 'success' group by sid");

        //打印表
        tableEnv.toRetractStream(t_station_log, Row.class)
                .filter(T -> T.f0)
                .print();

        tableEnv.execute("sql");
    }
}
