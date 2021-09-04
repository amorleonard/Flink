package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName Flink02_Stream_Bounded_WordCount
 * @Description //TODO 有界流
 * @Author Amor_leonard
 * @Date 2021/9/4 20:57
 * @Version 1.0
 **/
public class Flink02_Stream_Bounded_WordCount {
    public static void main(String[] args) throws Exception {
        //1、创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //并行度设置为1
        env.setParallelism(1);

        //2、读取文件
        DataStreamSource<String> streamSource = env.readTextFile("E:\\BigData0426\\Flink\\input\\word.txt");

        //3、转换数据格式
        SingleOutputStreamOperator<String> wordDstream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });

        //4、将单词组成tuple元组
        SingleOutputStreamOperator<Tuple2<String, Long>> wordtooneDstream = wordDstream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                return Tuple2.of(value, 1l);
            }
        });

        //5、将相同的key聚合到一起
        //自定义key选择器来决定走哪一个key
        KeyedStream<Tuple2<String, Long>, String> keyDstream = wordtooneDstream.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        });

        //6、聚合计算
        SingleOutputStreamOperator<Tuple2<String, Long>> result = keyDstream.sum(1);

        result.print();

        env.execute();

    }
}
