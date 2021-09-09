package com.atguigu.day03.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @ClassName Flink04_Sink_Custom
 * @Description //TODO 自定义数sink
 * @Author Amor_leonard
 * @Date 2021/9/9 10:05
 * @Version 1.0
 **/
public class Flink04_Sink_Custom {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {

                String[] splits = value.split(" ");

                WaterSensor ws = new WaterSensor(splits[0], Long.parseLong(splits[1]), Integer.parseInt(splits[2]));

                return ws;
            }
        });

        map.addSink(new mysql());
        env.execute();
    }

    //由于在sinkfunction中，连接和关闭资源的动作每条数据都需要执行一次，所以可以使用富函数替换，将创建连接与关闭连接写到open和close方法中。
    public static class mysql extends RichSinkFunction<WaterSensor> {
        //提升全局变量快捷键 ctrl+alt+f
        private static Connection connection;
        private PreparedStatement ps;

        @Override
        public void open(Configuration parameters) throws Exception {
            //创建mysql连接
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456");
            //语句预执行者
            ps = connection.prepareStatement("insert into sensor values(?,?,?)");
        }

        @Override
        public void close() throws Exception {
            //关闭资源
            ps.close();
            connection.close();
        }

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {
            //给占位符赋值
            ps.setString(1, value.getId());
            ps.setLong(2, value.getTs());
            ps.setInt(3, value.getVc());

            //执行语句预执行者
            ps.execute();
        }
    }
}
