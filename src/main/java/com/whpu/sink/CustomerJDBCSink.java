package com.whpu.sink;

import com.whpu.source.myself.MyConsumerDataSource;
import com.whpu.source.myself.StationLog;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;

/**
 * @author cc
 * @create 2020-08-04-11:21
 */
public class CustomerJDBCSink {
    public static void main(String[] args) throws Exception {
        //初始化Flink的Streaming（流计算）上下文执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取数据
        DataStreamSource<StationLog> streamSource = env.addSource(new MyConsumerDataSource());
        //数据写入mysql
        streamSource.addSink(new MyCustomerJDBCSink());

        env.execute();

    }
}
//自定义一个Sink写入Mysql
class MyCustomerJDBCSink extends RichSinkFunction<StationLog>{
    Connection conn = null;
    PreparedStatement pst = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        try{
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/customerJdbcSink?useSSL=false", "root", "root");
            pst = conn.prepareStatement("insert into t_station_log(sid,call_out,call_in,call_type,call_time,duration)values (?,?,?,?,?,?);");
        }catch(SQLException | ClassNotFoundException se){
            System.out.println("数据库连接失败 | 没驱动");
            se.printStackTrace();
        }

    }

    @Override
    public void close() throws Exception {
        super.close();
        pst.close();
        conn.close();
    }

    @Override
    public void invoke(StationLog value, Context context) throws Exception {
        pst.setString(1,value.sid);
        pst.setString(2,value.callOut);
        pst.setString(3,value.callIn);
        pst.setString(4,value.callType);
        pst.setLong(5,value.callTime);
        pst.setLong(6,value.duration);
        System.out.println(pst+" 被执行");
        pst.executeUpdate();//执行

    }
}
