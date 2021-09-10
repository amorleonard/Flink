package com.atguigu.day04;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class Flink16_Window_Time_Tumbling_AggFun {
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

        //5.开启一个基于时间的滚动窗口
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        //TODO 6.使用增量聚合函数，AggFun
        window.aggregate(new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {
            /**
             * 初始化累加器
             *
             * @return
             */
            @Override
            public Integer createAccumulator() {
                System.out.println("初始化累加器");
                return 0;
            }

            /**
             * 累加器的累加操作
             *
             * @param value
             * @param accumulator
             * @return
             */
            @Override
            public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
                System.out.println("累加操作");
                return accumulator + value.f1;
            }

            /**
             * 获取累加结果
             *
             * @param accumulator
             * @return
             */
            @Override
            public Integer getResult(Integer accumulator) {
                System.out.println("获取结果");
                return accumulator;
            }

            /**
             * 合并累加器 （此方法只在会话窗口中使用）
             *
             * @param a
             * @param b
             * @return
             */
            @Override
            public Integer merge(Integer a, Integer b) {
                System.out.println("Merge...");
                return a + b;
            }
        }).print();

        env.execute();

    }
}