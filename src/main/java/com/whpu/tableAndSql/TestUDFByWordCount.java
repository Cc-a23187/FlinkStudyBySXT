package com.whpu.tableAndSql;

import com.whpu.source.myself.StationLog;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Arrays;

/**
 * @author cc
 * @create 2020-08-24-17:05
 */
public class TestUDFByWordCount  {
    public static void main(String[] args) throws Exception {
        //初始化环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings setting = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, setting);

        //获取数据源
        DataStream<String> fileSource = streamEnv.socketTextStream("master", 8888);

        //转换为Table  并将字段名设置为line
        Table table = tableEnv.fromDataStream(fileSource, "line");

        //注册一个function
        tableEnv.registerFunction("split" , new MyFlatMapFunction());
        Table result = table.flatMap("split(line)").as("word , word_c")
                .groupBy("word")
                .select("word , word_c.sum as cnt");

        tableEnv.toRetractStream(result , Row.class).filter(t -> t.f0).print();

        streamEnv.execute("WordCountByUDF");

    }
}
