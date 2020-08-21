package com.whpu.time;

import com.whpu.source.myself.StationLog;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author cc
 * @create 2020-08-19-17:44
 */
public class MyReduceFunction implements ReduceFunction<StationLog> {
    @Override
    public StationLog reduce(StationLog v1, StationLog v2) throws Exception {
        if(v1.duration>v2.duration) return v1;
        else return v2;
    }
}
