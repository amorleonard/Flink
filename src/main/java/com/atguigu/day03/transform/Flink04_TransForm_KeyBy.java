package com.atguigu.day03.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName Flink04_TransForm_KeyBy
 * @Description //TODO
 * @Author Amor_leonard
 * @Date 2021/9/8 11:20
 * @Version 1.0
 **/
public class Flink04_TransForm_KeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env.setParallelism(4);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> flatMap = socketTextStream.flatMap(new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String value, Collector<WaterSensor> out) throws Exception {
                String[] splits = value.split(" ");
                out.collect(new WaterSensor(splits[0], Long.parseLong(splits[1]), Integer.parseInt(splits[2])));
            }
        }).setParallelism(4);

        //方式一：通过new KeySelector实现keyby
//        KeyedStream<WaterSensor, String> keyby = flatMap.keyBy(new KeySelector<WaterSensor, String>() {
//            @Override
//            public String getKey(WaterSensor value) throws Exception {
//                return value.getId();
//            }
//        });

        //方式二：通过属性名来实现keyby
        KeyedStream<WaterSensor, Tuple> keyBy = flatMap.keyBy("id");

        flatMap.print("原始数据：").setParallelism(2);
        keyBy.print("经过keyby之后：").setParallelism(2);

        //方式三：通过下标实现keyby,只适用于tuple元组

        env.execute();
    }
}
