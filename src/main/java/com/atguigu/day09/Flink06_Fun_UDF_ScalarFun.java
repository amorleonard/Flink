package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink06_Fun_UDF_ScalarFun {
    public static void main(String[] args) {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读取文件得到DataStream
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        //3.将流转换为动态表
        Table table = tableEnv.fromDataStream(waterSensorDataStreamSource);

        //不注册直接使用自定义函数
//        table
//                .select(call(MyUDF.class, $("id")),$("id"))
//                .execute()
//                .print();

        //注册一个自定义函数
        tableEnv.createTemporarySystemFunction("MyUdf", MyUDF.class);

        //TableAPI
//        table
//                .select(call("MyUdf", $("id")),$("id"))
//                .execute()
//                .print();

        //SQL
        tableEnv.executeSql("select MyUdf(id) from "+table).print();
    }

    //自定义一个UDF函数，用来求某个字段的长度
    public static class MyUDF extends ScalarFunction {

        public int eval(String value) {
            return value.length();
        }
    }

//    public static class MyMap implements MapFunction<String,Integer>{
//
//        @Override
//        public Integer map(String value) throws Exception {
//            return value.length();
//        }
//    }
}
