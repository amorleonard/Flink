package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName Flink03_Stream_UnBounded_WordCount
 * @Description //TODO无界流数据
 * @Author Amor_leonard
 * @Date 2021/9/4 21:57
 * @Version 1.0
 **/
public class Flink03_Stream_UnBounded_WordCount {
    public static void main(String[] args) throws Exception {
        //1、获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //并行度设置为1
        env.setParallelism(1);

        //2、获取无界数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3、flatmap转换，将数据按照空格切开，并组成tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Long>> wordtooneDstream = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] splits = value.split(" ");

                for (String words : splits) {
                    out.collect(Tuple2.of(words, 1l));
                }
            }
        });

        //4、将相同key的数据聚合到一起
        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordtooneDstream.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        });

        //5、聚合计算
        SingleOutputStreamOperator<Tuple2<String, Long>> result = keyedStream.sum(1);

        result.print();

        env.execute();

    }
}
