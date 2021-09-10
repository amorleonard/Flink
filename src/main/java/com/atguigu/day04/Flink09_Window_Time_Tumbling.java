package com.atguigu.day04;

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

/**
 * 窗口源码解析：
 * 1.为什么窗口是左闭右开
 * public long maxTimestamp() {
 * 		return end - 1;
 *        }
 * 2.窗口开始时间怎么计算
 * public static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
 * 		return timestamp - (timestamp - offset + windowSize) % windowSize;
 *    }
 * 3.滑动窗口为什么一条数据属于多个窗口
 * 开启一个基于时间的滑动窗口，窗口大小为5，滑动步长为2
 * 第一步：计算出laststart = timestamp - (timestamp - offset + slide) % slide;
 * 第二步：根据循环推断出来这条数据属于哪几个窗口
 * for (long start = lastStart;start > timestamp - size;start -= slide) {
 *
 * 		windows.add(new TimeWindow(start, start + size));
 * }
 * 第一秒钟来的数据会开启如下几个窗口
 * start =0  -2 -4
 * 0>-4  -2 >-4  -4>-4
 * new TimeWindow(0, 5) new TimeWindow(-2, 3)
 * 第四秒钟来的数据会开启下面几个窗口
 * start = 4 2 0
 * 4 > -1 2>-1 0>-1
 * new TimeWindow(4, 9) new TimeWindow(2, 7) new TimeWindow(0, 5)
 */
public class Flink09_Window_Time_Tumbling {
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

        //将相同key的数据聚合到一起
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneStream.keyBy(0);

        //基于时间的滚动窗口
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        window.sum(1).print();

        env.execute();
    }
}
