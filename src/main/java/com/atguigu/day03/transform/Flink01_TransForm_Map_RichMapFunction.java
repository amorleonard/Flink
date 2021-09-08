package com.atguigu.day03.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Flink01_TransForm_Map_RichMapFunction
 * @Description //TODO
 * @Author Amor_leonard
 * @Date 2021/9/7 18:19
 * @Version 1.0
 **/
public class Flink01_TransForm_Map_RichMapFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> streamSource = env.readTextFile("E:\\BigData0426\\Flink\\input\\word.txt");

        SingleOutputStreamOperator<String> result = streamSource.map(new myMap());

        result.print();
        env.execute();
    }

    //
    public static class myMap extends RichMapFunction<String, String> {
        //默认生命周期方法，最先被调用，每个并行实例执行一次,如建立连接...
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open...");
        }


        @Override
        public String map(String value) throws Exception {
            System.out.println(getRuntimeContext().getTaskNameWithSubtasks());
            return value + 1;
        }

        //默认生命周期方法，最后被调用，一般情况下，每个并行实例执行一次，但是在读取文件时，每个并行实例会执行两次
        @Override
        public void close() throws Exception {
            System.out.println("close...");
        }

    }
}


