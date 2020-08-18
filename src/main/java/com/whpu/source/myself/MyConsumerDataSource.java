package com.whpu.source.myself;

/**
 * @author cc
 * @create 2020-07-29-16:23
 * @deprecated 自定义的Source,需求：每隔两秒钟，生成10条随机基站通话日志数据
 *
 * 主要的方法，启动一个source，并且从source中返回数据
 * 如果run方法停止，则数据流终止
 */
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class MyConsumerDataSource implements SourceFunction<StationLog> {
    private boolean flag = true;
    private String[] types = new String[]{"fail","busy","barring","success"};

    @Override
    public void run(SourceContext<StationLog> sourceContext) throws Exception {
        Random random = new Random();
//        StationLog stationLog = null;
        while (flag){
            for (int i=0 ;i<=5 ;i++) {
                String callOut = String.format("1860000%04d", random.nextInt(10000));
                String callIn = String.format("1890000%04d", random.nextInt(10000));
                StationLog  stationLog  = new StationLog("station_" + random.nextInt(10)  , callOut, callIn,
                        types[random.nextInt(4)], System.currentTimeMillis(), (long) random.nextInt(20));
                sourceContext.collect(stationLog);//发数据
            }

            Thread.sleep(1000);//每发送一次数据休眠2秒
        }

    }
    //终止数据流
    @Override
    public void cancel() {
        flag = false;
    }
}
