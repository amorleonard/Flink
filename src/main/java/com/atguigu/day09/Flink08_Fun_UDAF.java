package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink08_Fun_UDAF {
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
//                .groupBy($("id"))
//                .select(call(MyUDAF.class, $("vc")),$("id"))
//                .execute()
//                .print();

        //注册一个自定义函数
        tableEnv.createTemporarySystemFunction("MyUdaf", MyUDAF.class);

        //TableAPI
//                table
//                .groupBy($("id"))
//                .select(call("MyUdaf", $("vc")),$("id"))
//                .execute()
//                .print();
        //SQL
        tableEnv.executeSql("select id,MyUdaf(vc) from "+table+" group by id").print();
    }

    //自定义一个UDAF函数，用来求vc的平均值
  public static class MyUDAF extends AggregateFunction<Double, Tuple2<Integer,Integer>>{

        /**
         * 初始化累加器
         * @return
         */
        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return Tuple2.of(0,0);
        }

        /**
         * 累加操作
         * @param acc
         * @param value
         */
        public void accumulate(Tuple2<Integer, Integer> acc, Integer value) {
            acc.f0 = acc.f0 + value;
            acc.f1 = acc.f1 + 1;
        }

        /**
         * 获取结果
         * @param accumulator
         * @return
         */
        @Override
        public Double getValue(Tuple2<Integer, Integer> accumulator) {
            return accumulator.f0*1D/accumulator.f1;
        }
    }

}
