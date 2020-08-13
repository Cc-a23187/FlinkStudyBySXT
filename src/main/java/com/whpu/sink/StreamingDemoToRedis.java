package com.whpu.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
/**
 * @author cc
 * @create 2020-07-30-17:07
 */
public class StreamingDemoToRedis {
    public static void main(String[] args)  throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.socketTextStream(
                "master", 9000, "\n");

        //input: l_words word , 其中 l_words 代表 list 类型
        //对数据进行组装,把string转化为tuple2<String,String>
        DataStream<Tuple2<String, String>> l_wordsData = text.map(
                new MapFunction<String, Tuple2<String, String>>() {

                    @Override
                    public Tuple2<String, String> map(String value) throws Exception {
                        return new Tuple2<>("l_words", value);
                    }
                });

        //创建redis的配置
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost").setPort(6379).build();

        //创建redissink
        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(
                conf, new MyRedisMapper());

        l_wordsData.addSink(redisSink);
        env.execute("StreamingDemoToRedis");

    }

    public static class MyRedisMapper implements RedisMapper<Tuple2<String, String>> {
        //表示从接收的数据中获取需要操作的redis key
        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }
        //表示从接收的数据中获取需要操作的redis value
        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }
    }
}

