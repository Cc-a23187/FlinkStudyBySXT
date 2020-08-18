package com.whpu

/**
  * @author cc
  * @create 2020-08-18-14:37
  */
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import scala.util.Random

/**
  * 自定义的Source,需求：每隔两秒钟，生成10条随机基站通话日志数据
  */
class MyCustomerSource extends SourceFunction[StationLog]{
  //是否终止数据流的标记
  var flag =true;

  /**
    * 主要的方法，启动一个Source，并且从Source中返回数据
    * 如果run方法停止，则数据流终止
    * @param ctx
    */
  override def run(ctx: SourceFunction.SourceContext[StationLog]): Unit = {
    val r = new Random()
    var types=Array("fail","basy","barring","success")
    while(flag){
      1.to(10).map(i=>{
        var callOut ="1860000%04d".format(r.nextInt(10000)) //主叫号码
        var callIn ="1890000%04d".format(r.nextInt(10000)) //被叫号码
        //生成一条数据
        new StationLog("station_"+r.nextInt(10),callOut,callIn,types(r.nextInt(4)),System.currentTimeMillis(),r.nextInt(20))
      }).foreach(ctx.collect(_)) //发送数据到流
      Thread.sleep(2000) //每隔2秒发送一次数据
    }
  }
  //终止数据流
  override def cancel(): Unit = {
    flag =false;
  }
}
object CustomerSource {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    val stream: DataStream[StationLog] = streamEnv.addSource(new MyCustomerSource)

    stream.print()

    streamEnv.execute("自定义Source")
  }
}
