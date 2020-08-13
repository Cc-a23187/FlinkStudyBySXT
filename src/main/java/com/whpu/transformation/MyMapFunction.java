package com.whpu.transformation;

import com.whpu.source.myself.StationLog;
import org.apache.flink.api.common.functions.MapFunction;

import java.text.SimpleDateFormat;

/**
 * @author cc
 * @create 2020-08-06-9:57
 */
public class MyMapFunction implements MapFunction<StationLog, String>    {
    //定义时间格式
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    @Override
    public String map(StationLog stationLog) throws Exception {
        String startTime = format.format(stationLog.getCallTime());
        String endTime = format.format(stationLog.getCallTime()+stationLog.duration*1000);
        return "主叫号码："+stationLog.getCallOut()+", 被叫号码："+stationLog.callIn+", 呼叫起始时间："+startTime+", 呼叫结束时间："+endTime;
    }
}
