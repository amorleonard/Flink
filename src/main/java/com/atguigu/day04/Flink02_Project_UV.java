package com.atguigu.day04;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * @ClassName Flink02_Project_UV
 * @Description //TODO 网站独立访客数（UV）的统计
 * @Author Amor_leonard
 * @Date 2021/9/9 14:38
 * @Version 1.0
 **/
public class Flink02_Project_UV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("E:\\BigData0426\\Flink\\input\\UserBehavior.csv");

        SingleOutputStreamOperator<Tuple2<String, Long>> uv = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] splits = value.split(",");
                UserBehavior ub = new UserBehavior(Long.parseLong(splits[0]), Long.parseLong(splits[1]), Integer.parseInt(splits[2]), splits[3], Long.parseLong(splits[4]));

                if ("pv".equals(ub.getBehavior())) {
                    out.collect(Tuple2.of("uv", ub.getUserId()));
                }
            }
        });


        KeyedStream<Tuple2<String, Long>, Tuple> keyBy = uv.keyBy(0);

        //去重并求和
        keyBy.process(new KeyedProcessFunction<Tuple, Tuple2<String, Long>, Tuple2<String, Long>>() {
            HashSet<Long> hashSet = new HashSet<>();

            @Override
            public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                //按照ub.getUserId()去重
                hashSet.add(value.f1);
                out.collect(Tuple2.of("uv", (long) hashSet.size()));
            }
        }).print();

        env.execute();
    }
}
