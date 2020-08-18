package com.whpu.window;

import com.whpu.source.myself.MyConsumerDataSource;
import com.whpu.source.myself.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @author cc
 * @create 2020-08-18-10:05
 */
public class TestProcessWindowFunction {
    public static void main(String[] args) throws Exception {
        //初始化Flink的Streaming（流计算）上下文执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*设置使用EventTime作为Flink的时间处理标准，不指定默认是ProcessTime*/
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        //读取数据
        DataStream<StationLog> dataStream = env.addSource(new MyConsumerDataSource());
        //        env.getClass().getResource("/station.log").getPath()
//        DataStream<StationLog> dataStream = env.socketTextStream("192.168.242.150",8888)
//                .map(new MapFunction<String, StationLog>() {
//                    @Override
//                    public StationLog map(String s) throws Exception {
//                        String[] arr =s.split(",");
//                        StationLog stationLog = new StationLog(arr[0].trim(),arr[1].trim(),arr[2].trim(),arr[3].trim(),Long.valueOf(arr[4].trim()),Long.valueOf(arr[5].trim()));
//                        stationLog.setSid(arr[0].trim());
//                        return stationLog;
//                    }
//                });//stationLog -> Tuple2.of(stationLog, stationLog.sid
//        dataStream.print("first");
        SingleOutputStreamOperator<Tuple2<String, Integer>> streamSink = dataStream.map(new MapFunction<StationLog, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(StationLog stationLog) throws Exception {
                return Tuple2.of(stationLog.sid,1);
            }
        });
        streamSink.print("sec");
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = streamSink.keyBy(0)
                .timeWindow(Time.seconds(5))
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        System.out.println("-----------");
                        Map<String, Integer> counts = new HashMap<>();
                        Integer count = 0;
                        String id = null;
                        for(Tuple2<String, Integer> ele: iterable){
                            count++;

                            id = ele.f0;
                        }
                        collector.collect(Tuple2.of(id,count));
                    }
                });
        reduce.print("last");
        env.execute();

    }
}

/**

package com.whpu.window;

        import com.whpu.source.myself.MyConsumerDataSource;
        import com.whpu.source.myself.StationLog;
        import org.apache.flink.api.common.functions.MapFunction;
        import org.apache.flink.api.common.functions.ReduceFunction;
        import org.apache.flink.api.java.tuple.Tuple;
        import org.apache.flink.api.java.tuple.Tuple2;
        import org.apache.flink.streaming.api.datastream.DataStream;
        import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
        import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
        import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
        import org.apache.flink.streaming.api.windowing.time.Time;
        import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
        import org.apache.flink.util.Collector;

        import java.util.HashMap;
        import java.util.Map;

*/
/**
 * @author cc
 * @create 2020-08-18-10:05
 *//*

public class TestProcessWindowFunction {
    public static void main(String[] args) throws Exception {
        //初始化Flink的Streaming（流计算）上下文执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        */
/*设置使用EventTime作为Flink的时间处理标准，不指定默认是ProcessTime*//*

//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        //读取数据
        DataStream<StationLog> dataStream = env.addSource(new MyConsumerDataSource());
        //        env.getClass().getResource("/station.log").getPath()
//        DataStream<StationLog> dataStream = env.socketTextStream("192.168.242.150",8888)
//                .map(new MapFunction<String, StationLog>() {
//                    @Override
//                    public StationLog map(String s) throws Exception {
//                        String[] arr =s.split(",");
//                        StationLog stationLog = new StationLog(arr[0].trim(),arr[1].trim(),arr[2].trim(),arr[3].trim(),Long.valueOf(arr[4].trim()),Long.valueOf(arr[5].trim()));
//                        stationLog.setSid(arr[0].trim());
//                        return stationLog;
//                    }
//                });//stationLog -> Tuple2.of(stationLog, stationLog.sid
//        dataStream.print("first");
        SingleOutputStreamOperator<Tuple2<String, Integer>> streamSink = dataStream.map(new MapFunction<StationLog, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(StationLog stationLog) throws Exception {
                return Tuple2.of(stationLog.sid,1);
            }
        });
        streamSink.print("sec");
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = streamSink.keyBy(0)
                .timeWindow(Time.seconds(5))
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        System.out.println("-----------");
                        Map<String, Integer> counts = new HashMap<>();
                        Integer count = 0;
                        String id = null;
                        for(Tuple2<String, Integer> ele: iterable){
                            count++;
                            if (counts.get(ele.f0)!=null){
                                counts.put(ele.f0,counts.get(ele.f0)+ele.f1);
                            }
                            else counts.put(ele.f0,ele.f1);
                            id = ele.f0;
                            count = counts.get(ele.f0);
                        }
                        collector.collect(Tuple2.of(id,count));
                    }
                });
        reduce.print("last");
        env.execute();

    }
}
*/
