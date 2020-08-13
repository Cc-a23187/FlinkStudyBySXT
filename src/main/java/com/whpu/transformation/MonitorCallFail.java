package com.whpu.transformation;

import com.whpu.source.myself.StationLog;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author cc
 * @create 2020-08-06-14:36
 * //自定义一个底层process方法
 */
public class MonitorCallFail extends KeyedProcessFunction<String, StationLog,String>{

    //使用一个状态对象去保存时间   transient
      ValueState<Long> timeState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        timeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time", Long.class));
    }

    @Override
    public void processElement(StationLog stationLog, Context context, Collector<String> collector) throws Exception {
        Long time = timeState.value();

//        System.out.println(stationLog.toString());
//        System.out.println(time+"  "+timeState.value());
        if (time==null &&stationLog.getCallType().equals("fail")) //第一次发现呼叫失败
        //获取当前系统时间，并注册计时器
        {

            long nowTime = context.timerService().currentProcessingTime();
            long onTime = nowTime+5*1000L;
            context.timerService().registerProcessingTimeTimer(onTime);
            timeState.update(onTime);
            System.out.println(nowTime+"  "+onTime);
        }
        if ( time!=null &&!stationLog.getCallType().equals("fail")){
            context.timerService().deleteProcessingTimeTimer(time);
            timeState.clear();
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        String warnStr = "触发时间："+timestamp+", 手机号"+ctx.getCurrentKey();
        out.collect(warnStr);
        timeState.clear();
    }
}