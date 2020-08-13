package com.whpu.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author cc
 * @create 2020-07-28-14:17
 * 基于集合的DataSource
 */
public class DataSourceForCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //修改并行度 , 开启一个线程
        env.setParallelism(1);
        ArrayList<Integer> data = new ArrayList<>();
        data.add(10);
        data.add(15);
        data.add(20);
        //基于DataStream按照空格分词，然后按照word做key做group by
        DataStreamSource<Integer> collectionData= env.fromCollection(data);
        collectionData.print("dataStream").setParallelism(1);
        env.execute("Data Source For Collection");

    }
}
