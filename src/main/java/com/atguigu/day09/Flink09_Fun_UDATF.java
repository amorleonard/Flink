package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink09_Fun_UDATF {
    public static void main(String[] args) {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读取文件得到DataStream
//        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
//                new WaterSensor("sensor_1", 2000L, 20),
//                new WaterSensor("sensor_2", 3000L, 30),
//                new WaterSensor("sensor_1", 4000L, 40),
//                new WaterSensor("sensor_1", 5000L, 50),
//                new WaterSensor("sensor_2", 6000L, 60));
        SingleOutputStreamOperator<WaterSensor> waterSensorDataStreamSource = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });


        //3.将流转换为动态表
        Table table = tableEnv.fromDataStream(waterSensorDataStreamSource);

        //不注册直接使用自定义函数
//        table
//                .groupBy($("id"))
//                .flatAggregate(call(MyUDATF.class, $("vc")).as("value", "top"))
//                .select($("id"),$("value"),$("top"))
//                .execute()
//                .print();

        //注册一个自定义函数
        tableEnv.createTemporarySystemFunction("MyUdatf", MyUDATF.class);

        //TableAPI
        table
                .groupBy($("id"))
                .flatAggregate(call("MyUdatf", $("vc")).as("value", "top"))
                .select($("id"),$("value"),$("top"))
                .execute()
                .print();

    }

    //自定义一个UDATF函数，用来求vc的Top2
    public static class top2Vc{
        public Integer first;
        public Integer second;
    }

    //同过自定义UDTF函数来实现Top2的需求
    public static class MyUDATF extends TableAggregateFunction<Tuple2<Integer,Integer>,top2Vc>{

        @Override
        public top2Vc createAccumulator() {
            top2Vc top2Vc = new top2Vc();
            top2Vc.first = Integer.MIN_VALUE;
            top2Vc.second = Integer.MIN_VALUE;
            return top2Vc;
        }

        //累加操作
        public void accumulate(top2Vc acc,Integer value){
            //先比较当前数据是否大于第一
            if (value>acc.first){
                //先将之前的第一名置为第二名
                acc.second = acc.first;
                //再将当前数据置为第一名
                acc.first = value;
            }else if (value>acc.second){
                //不大于第一但是大于第二
                acc.second = value;
            }
        }

        //将结果发送出去
        public void emitValue(top2Vc acc, Collector<Tuple2<Integer,Integer>> out){
            if (acc.first!=Integer.MIN_VALUE){
                out.collect(Tuple2.of(acc.first,1));
            }

            if (acc.second!=Integer.MIN_VALUE){
                out.collect(Tuple2.of(acc.second,2));
            }
        }
    }


}
