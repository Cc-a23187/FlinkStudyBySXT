package com.whpu.tableAndSql;

import com.whpu.source.myself.StationLog;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author cc
 * @create 2020-08-21-15:12
 * @Description   读取csv table信息         通过文件创建表，主要用于批计算所以在最后没有
 * 注意：本案例的最后面不要 streamEnv.execute()，否则报错。因为没有其他流计算逻辑
 */
public class TestCreateTableByDataStream {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, fsSettings);

        //获取数据源
        DataStream<StationLog> fileSource = /*streamEnv.addSource(new MyConsumerDataSource());*/
                streamEnv.socketTextStream("master", 8888)
                .map(line -> {
                    String[] split = line.trim().split(",");
                    return new StationLog(split[0], split[1], split[2], split[3], Long.valueOf(split[4]), Long.valueOf(split[5]));
                });
        //注册为表

/*        //filter过滤
        Table table =  tableEnv.fromDataStream( fileSource)
                .filter("callType = 'success'");

        DataStream<Row> dataStream = tableEnv.toAppendStream(table, Row.class);
        dataStream.print();*/

/*        //打印查询  原生sql查询
        tableEnv.registerDataStream("t2",fileSource);
        tableEnv.sqlQuery("select * from t2").printSchema();*/

        //分组聚合
        Table result = tableEnv.fromDataStream( fileSource)
                .groupBy("sid").select("sid, sid.count as number");

        DataStream<Tuple2<Boolean, Row>> dataStream2 = tableEnv.toRetractStream(result, Row.class);
//                .filter(tuple -> tuple.f0); //返回为ture
        dataStream2.print();
        streamEnv.execute();

    }
}
