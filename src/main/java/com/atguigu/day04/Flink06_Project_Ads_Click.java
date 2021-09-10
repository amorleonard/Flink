package com.atguigu.day04;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

//各省份页面广告点击量实时统计
public class Flink06_Project_Ads_Click {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("E:\\BigData0426\\Flink\\input\\AdClickLog.csv");

       //方式一：将province与adid进行字符串拼接
//        SingleOutputStreamOperator<Tuple2<String, Long>> protoad = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
//            @Override
//            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
//                String[] split = value.split(",");
//                out.collect(Tuple2.of(split[2] + "-" + split[1], 1l));
//            }
//        });

        //方式二：将province与adid组成tuple元组
        SingleOutputStreamOperator<Tuple2<Tuple2<String, Long>, Long>> protoad = streamSource.flatMap(new FlatMapFunction<String, Tuple2<Tuple2<String, Long>, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<Tuple2<String, Long>, Long>> out) throws Exception {
                String[] split = value.split(",");
                out.collect(Tuple2.of(Tuple2.of(split[2], Long.parseLong(split[1])), 1L));
            }
        });

        protoad.keyBy(0).sum(1).print();

        env.execute();
    }
}
