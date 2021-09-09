package com.atguigu.day04;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName Flink02_Project_pv_PROCESS
 * @Description //TODO
 * @Author Amor_leonard
 * @Date 2021/9/9 14:49
 * @Version 1.0
 **/
public class Flink01_Project_PV_PROCESS {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("E:\\BigData0426\\Flink\\input\\UserBehavior.csv");

        streamSource.process(new ProcessFunction<String, Tuple2<String, Long>>() {
            private Long count = 0l;
            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] split = value.split(",");
                if ("pv".equals(split[3])) {
                    count++;
                    out.collect(Tuple2.of("pv",count));
                }
            }
        }).print();

        env.execute();
    }
}
