package com.whpu.transformation

/**
  * @author cc
  * @create 2020-08-10-15:51
  */

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import com.whpu.source.myself.StationLog
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.streaming.api.scala._

object TestRichMapFunction {

//  def main(args: Array[String]): Unit = {
//    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
//    import org.apache.flink.streaming.api.scala._
//    var filePath = getClass.getResource("/station.log").getPath
//    val data: DataStream[String] = streamEnv.readTextFile(filePath)
//    data.map(line=>{
//      val arr = line.split(",")
//      new StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
//    }).filter(_.callType.equals("success"))
//      .map(new MyRichMapFunction).print()
//    streamEnv.execute()
//
//  }
//
//  class MyRichMapFunction extends RichMapFunction[StationLog, StationLog]{
//    var conn: Connection = _
//    var pstm: PreparedStatement = _
//
//    override def map(in: StationLog): StationLog = {
//      pstm.setString(1, in.callOut)
//      var result: ResultSet = pstm.executeQuery()
//      if(result.next()){
//        in.callOut = result.getString(1)
//      }
//      pstm.setString(1, in.callIn)
//      result = pstm.executeQuery()
//      if(result.next()){
//        in.callIn = result.getString(1)
//      }
//      in
//    }
//
//    override def open(parameters: Configuration): Unit = {
//      conn = DriverManager.getConnection("jdbc:mysql://node11/test","root","123")
//      pstm = conn.prepareStatement("select name from t_phone where phone_number=?")
//    }
//
//    override def close(): Unit = {
//      pstm.close()
//      conn.close()
//    }
//  }

}