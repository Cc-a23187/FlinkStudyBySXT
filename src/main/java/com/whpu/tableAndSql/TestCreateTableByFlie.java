package com.whpu.tableAndSql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;

/**
 * @author cc
 * @create 2020-08-21-15:12
 * @Description   读取csv table信息         通过文件创建表，主要用于批计算所以在最后没有
 * 注意：本案例的最后面不要 streamEnv.execute()，否则报错。因为没有其他流计算逻辑
 */
public class TestCreateTableByFlie {
    public static void main(String[] args) {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, fsSettings);

        //获取数据源
        CsvTableSource fileSource = new CsvTableSource(TestCreateTableByFlie.class.getResource("/station.log").getPath(),
                new String[]{"sid", "callOut", "callIn", "callType", "callTime", "duration"},
                new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.LONG, Types.LONG});
        //注册为一张表 该方法没有返回值
        tableEnv.registerTableSource("t_station_log" , fileSource);
        //扫描表结构
        Table t_station_log = tableEnv.scan("t_station_log");
        //打印表结构
        t_station_log.printSchema();

    }
}
