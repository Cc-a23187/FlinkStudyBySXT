package com.whpu.state;

import com.whpu.WordCountPi;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author cc
 * @create 2020-08-11-16:02
 * 需求：CheckPoint案例实现，使用hdfs做状态后端，取消job后再次恢复job测试；
 */
public class TestCheckPointByHDFS {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //——————————开启一个checkpoint并且设置一些参数——————————
        //每隔5s开启一个checkpoint
        env.enableCheckpointing(5000);
        //使用FsStateBackend 存放检查点数据
        env.setStateBackend(new FsStateBackend("hdfs://slave1:8020/checkpoint/cp2"));
        //——————————————————————官网拿来的参数———————getCheckpointConfig———————————————
        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // make sure 500 ms of progress happen between checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // enable externalized checkpoints which are retained after job cancellation
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //连接socket获取输入的数据
        DataStreamSource<String> text = env.socketTextStream("192.168.242.150", 8888, "\n");

        //计算数据
        DataStream<Tuple2<String, Integer>> counts = text
                .flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        for (String word: s.split(" ")){
                            collector.collect(new Tuple2<>(word,1));
                        }
                    }
                })
                .keyBy(0)
                .sum(1);
        counts.print();
        env.execute();

    }
}
