package com.whpu.time;

import com.whpu.source.myself.StationLog;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @author cc
 * @create 2020-08-19-17:44
 */
public class ReturnMaxTimeWindowFunction implements WindowFunction<StationLog, String, Tuple, TimeWindow> {


    @Override
    public void apply(Tuple tuple, TimeWindow window, Iterable<StationLog> input, Collector<String> out) throws Exception {
        StationLog value = input.iterator().next();
        StringBuffer sb = new StringBuffer();

        sb.append("窗口范围是:").append(window.getStart()).append("----").append(window.getEnd());
        sb.append("\n");
        sb.append("呼叫时间：").append(value.callTime)
                .append("主叫号码：").append(value.callOut)
                .append("被叫号码：").append(value.callIn)
                .append("通话时长：").append(value.duration);


    }
}
