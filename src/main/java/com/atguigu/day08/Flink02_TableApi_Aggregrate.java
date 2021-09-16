package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * DataStreamAPI与表混合使用
 */
public class Flink02_TableApi_Aggregrate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(
                        new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60)
                );

        //创建表的执行环境
        StreamTableEnvironment tableenv = StreamTableEnvironment.create(env);

        //将流转换为动态表
        Table table = tableenv.fromDataStream(waterSensorStream);

        //查询动态表
        Table selection = table
                .groupBy($("id"))
                .aggregate($("vc").sum().as("vc_sum"))
                .select($("id"), $("vc_sum"));

        //把动态表转化为流
        DataStream<Tuple2<Boolean, Row>> result = tableenv.toRetractStream(selection, Row.class);

        result.print();

        env.execute();
    }
}