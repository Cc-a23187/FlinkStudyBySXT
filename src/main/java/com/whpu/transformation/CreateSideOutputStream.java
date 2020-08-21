package com.whpu.transformation;

import com.whpu.source.myself.StationLog;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class CreateSideOutputStream extends ProcessFunction<StationLog,StationLog> {
    OutputTag<StationLog> notSuccessTag = new OutputTag<StationLog>("not_success"){};
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<StationLog> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
    }

    @Override
    public void processElement(StationLog stationLog, Context context, Collector<StationLog> collector) throws Exception {
        if(stationLog.callType.equals("success")){
            collector.collect(stationLog);//主流对外输出
        }else {
            context.output(notSuccessTag,stationLog);//输出侧流
        }
    }

}
