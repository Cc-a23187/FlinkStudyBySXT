package com.whpu;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author cc
 * @create 2020-07-06-9:59
 */
public class WordCountPi {

    public static void main(String[] args) throws Exception {
        //创建一个批处理运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //从文件中读取数据
        DataSet<String> dataset = env.fromElements("I have a good friend. She is a very nice person. The first time I saw her at school, she smiled at me and I like her a lot. When we do homework in a team, we become friends. As we share the same interest, we are close. Now we often hang out for fun and we cherish our friendship so much.");

        //基于dataset按照空格分词，然后按照word做key做group by
        DataSet<Tuple2<String, Integer>> counts = dataset
                .flatMap(new LineSplitter())
                .groupBy(0)
                .sum(1);
        System.out.println("输出如下：");
        counts.print();
    }

    public static  class LineSplitter implements FlatMapFunction<String, Tuple2<String,Integer>> {

        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: line.split(" ")){
                out.collect(new Tuple2<>(word,1));
            }
        }
    }
}
