package com.whpu.cep;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.xml.crypto.Data;
import java.util.*;

/**
 * @author cc
 * @create 2020-08-26-16:48
 * 检测在规定时间内，登录超过三次的用户，则是恶意登录，找出恶意登录的用户；
 */

public class TestCEPByLogin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        //从collection中获取数据
        DataStream<EventLog> stream = streamEnv.fromCollection(Arrays.asList(
                new EventLog(1L, "张三", "fail", 1574840003L),
                new EventLog(1L, "张三", "fail", 1574840004L),
                new EventLog(1L, "张三", "fail", 1574840005L),
                new EventLog(2L, "李四", "fail", 1574840006L),
                new EventLog(2L, "李四", "sucess", 1574840007L),
                new EventLog(1L, "张三", "fail", 1574840008L)
        ))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<EventLog>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(EventLog eventLog) {
                        return eventLog.getEventTime() * 1000;
                    }
                });

        stream.print("input");


        //1、定义模式（pattern） begin为模式的开始
            //1、定义一个fail方法，检测失败
        IterativeCondition<EventLog> fail = new IterativeCondition<EventLog>() {
            @Override
            public boolean filter(EventLog eventLog, Context<EventLog> context) throws Exception {
                return eventLog.getEventType().equals("fail");
            }
        };

        Pattern<EventLog, EventLog> pattern = Pattern.<EventLog>begin("start").where(fail)
                .next("fail2").where(fail)
                .next("fail3").where(fail)
                .within(Time.seconds(10));


        //2、cep做模式检测
        PatternStream<EventLog> patternStream = CEP.pattern(stream.keyBy(EventLog::getId), pattern);

        //3、选择结果，输出alert
        DataStream<String> select = patternStream.select(new PatternSelectFunction<EventLog, String>() {
            @Override
            public String select(Map<String, List<EventLog>> map) throws Exception {

                Iterator<String> keyIterator = map.keySet().iterator();
                EventLog eventLogs1 = (EventLog) map.get(keyIterator.next()).iterator().next();
                EventLog eventLogs2 = (EventLog) map.get(keyIterator.next()).iterator().next();
                EventLog eventLogs3 = (EventLog) map.get(keyIterator.next()).iterator().next();
                return "id:" + eventLogs1.getId() + " 用户名:" + eventLogs1.getUserName() +
                        "  失败告警 登录的时间:" + eventLogs1.getEventTime()
                        + ":" + eventLogs2.getEventTime() + ":" + eventLogs3.getEventTime();
            }

        });
        select.print("main");

        streamEnv.execute();
    }
}
