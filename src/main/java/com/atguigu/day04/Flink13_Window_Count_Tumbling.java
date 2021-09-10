package com.atguigu.day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;


public class Flink13_Window_Count_Tumbling {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.将数据组成Tuple元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneStream = streamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });

        //4.将相同单词的数据聚和到一块
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneStream.keyBy(0);

        //TODO 5.开启一个基于元素个数的滚动窗口
        WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> window = keyedStream.countWindow(5);


        window.sum(1).print();

        env.execute();

    }
}