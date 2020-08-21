package com.whpu.time;

import com.whpu.source.myself.StationLog;
import com.whpu.transformation.CreateSideOutputStream;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author cc
 * @create 2020-08-20-17:19
 */
public class LatenessDataOnWindow {

    public static void main(String[] args) throws Exception {
        //创建侧流
        CreateSideOutputStream createSideOutputStream = new CreateSideOutputStream();
        //env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //①设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //周期 引入waterMark的设置,默认就是100L
        env.getConfig().setAutoWatermarkInterval(100L);
        //②获取数据源
//        DataStream<StationLog> dataStream = env.addSource(new MyConsumerDataSource())
        SingleOutputStreamOperator<StationLog> dataStream = env.socketTextStream("192.168.242.150",8888)
                .map(new MapFunction<String, StationLog>() {
                    @Override
                    public StationLog map(String s) throws Exception {
                        String[] arr =s.split(",");
                        return new StationLog(arr[0].trim(),arr[1].trim(),arr[2].trim(),arr[3].trim(),Long.valueOf(arr[4].trim()),Long.valueOf(arr[5].trim()));
                    }
                })
                //③引入waterMark
                //第一种 直接采用flink提供的 AssignerWithPeriodicWatermarks 接口的实现类
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<StationLog>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(StationLog stationLog) {
                        return stationLog.callTime;
                    }
                });

        //定义一个侧输出流的标签
//        OutputTag<StationLog> outputTags = new OutputTag<StationLog>("late");
        OutputTag<StationLog> outputTags = new OutputTag<StationLog>("late" , TypeInformation.of(StationLog.class));
        //④分组——开窗
        SingleOutputStreamOperator<String> data =dataStream
                .filter(stationLog -> ((stationLog.getCallType().equals("success"))))
                .keyBy("sid")
                .timeWindow(Time.seconds(10), Time.seconds(5))
                //设置迟到的数据超出了2秒的情况下，怎么办。交给AllowedLateness处理
                //也分两种情况，第一种：允许数据迟到5秒（迟到2-5秒），再次延迟触发窗口函数。
                //      触发的条是：Watermark < end-of-window + allowedlateness
                //第二种：迟到的数据在5秒以上，输出到则流中
                .allowedLateness(Time.seconds(5)) //运行数据迟到5秒，还可以触发窗口
                .sideOutputLateData(outputTags)

                .aggregate(new AggregateFunction<StationLog, Tuple2<String,Long>, Tuple2<String,Long> >() {
                    @Override
                    public Tuple2<String, Long> createAccumulator() {
                        return Tuple2.of("",0L);
                    }

                    @Override
                    public Tuple2<String, Long> add(StationLog stationLog, Tuple2<String, Long> tuple2) {
                        return Tuple2.of(tuple2.f0,tuple2.f1+1);
                    }

                    @Override
                    public Tuple2<String, Long> getResult(Tuple2<String, Long> tuple2) {
                        return tuple2;
                    }

                    @Override
                    public Tuple2<String, Long> merge(Tuple2<String, Long> acc1, Tuple2<String, Long> acc2) {
                        return Tuple2.of(acc1.f0,acc1.f1+acc2.f1);
                    }
                }, new  WindowFunction<Tuple2<String,Long>, String, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                        Tuple2<String,Long> value = input.iterator().next();
                        StringBuffer sb = new StringBuffer();

                        sb.append("窗口的范围:").append(window.getStart()).append("---").append(window.getEnd());
                        sb.append("\n");
                        sb.append("当前的基站ID是:").append(tuple)
                            .append("， 呼叫的数量是:").append(value.f1);
                        out.collect(sb.toString());
                    }

                });


        DataStream<StationLog> sideOutput = data.getSideOutput(outputTags);

        sideOutput.print("侧流");

        data.print("主流");

        env.execute();
    }
}

