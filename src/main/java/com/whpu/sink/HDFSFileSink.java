package com.whpu.sink;

import com.whpu.source.myself.MyConsumerDataSource;
import com.whpu.source.myself.StationLog;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

/**
 * @author cc
 * @create 2020-07-30-11:39
 * 功能： 使用自定义source 作为数据源， 把基站日志数据写入到hdfs 并且每间隔两秒钟生成一个文件；
 */
public class HDFSFileSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStream<StationLog> dataStream = env.addSource(new MyConsumerDataSource());
        //创建一个文件滚动规则
        DefaultRollingPolicy<StationLog, String> rolling = DefaultRollingPolicy.create()
                .withInactivityInterval(2000)//设置不活跃时间
                .withMaxPartSize(2000) //每隔两秒生成一个文件
                .build(); //生效 创建
        //定义hdfs 的sink
        StreamingFileSink<StationLog> fileSink = StreamingFileSink.forRowFormat(
                new Path("hdfs://192.168.242.151:8020/temp/flink/mySink001"),
                new SimpleStringEncoder<StationLog>("UTF-8"))
                .withBucketCheckInterval(1000)//检查分桶间隔时间
                .withRollingPolicy(rolling)
                .build();

        dataStream.addSink(fileSink);
        dataStream.print();
        env.execute();




    }


}
