package com.whpu.source;

import com.whpu.WordCountPi;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author cc
 * @create 2020-07-27-14:32
 * 基于文件DataSource
 */
public class DataSourceForDoc {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //修改并行度,开启一个线程
        env.setParallelism(1);
        //从文件中读取数据  hdfs://192.168.242.150:8020/myText.txt
        // 从hdfs读取文件，需要写上hdfs的nameService（高可用集群），或者namenode ip及端口号
        DataStream<String> dataStream = env.readTextFile("hdfs://192.168.242.151:8020/myText.txt");
        //基于DataStream按照空格分词，然后按照word做key做group by
//        dataStream.print("dataStream").setParallelism(1);
        DataStream<Tuple2<String, Integer>> counts = dataStream
                .flatMap(new WordCountPi.LineSplitter())
                .keyBy(0)
                .sum(1);
        counts.print();
        env.execute("Data Source For Doc");

    }
}

