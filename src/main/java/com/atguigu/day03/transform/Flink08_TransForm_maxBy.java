package com.atguigu.day03.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName Rolling_aggregation
 * @Description //TODO
 * @Author Amor_leonard
 * @Date 2021/9/8 8:28
 * @Version 1.0
 **/
public class Flink08_TransForm_maxBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> flatMap = socketTextStream.flatMap(new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String value, Collector<WaterSensor> out) throws Exception {
                String[] splits = value.split(" ");
                out.collect(new WaterSensor(splits[0], Long.parseLong(splits[1]), Integer.parseInt(splits[2])));
            }
        });

        KeyedStream<WaterSensor, Tuple> keyby = flatMap.keyBy("id");

        //当设置为true时，会优先选择最早的最大值
//        SingleOutputStreamOperator<WaterSensor> vc = keyby.maxBy("vc", true);
        //当设置为true时，会优先选择最新的最大值
        SingleOutputStreamOperator<WaterSensor> vc = keyby.maxBy("vc", false);
        vc.print();
        env.execute();

    }
}
