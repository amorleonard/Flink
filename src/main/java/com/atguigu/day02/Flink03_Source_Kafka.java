package com.atguigu.day02;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @ClassName Flink03_Source_Kafka
 * @Description //TODO 从kafka中读取数据
 * @Author Amor_leonard
 * @Date 2021/9/6 15:23
 * @Version 1.0
 **/
public class Flink03_Source_Kafka {
    public static void main(String[] args) throws Exception {
        //1、创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度为1
        env.setParallelism(1);

        Properties kafkaproperties = new Properties();
        kafkaproperties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        kafkaproperties.setProperty("group.id", "test");
        kafkaproperties.setProperty("auto.offset.reset", "latest");

        //从kafka获取数据
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), kafkaproperties));

        source.print();

        env.execute();

    }
}
