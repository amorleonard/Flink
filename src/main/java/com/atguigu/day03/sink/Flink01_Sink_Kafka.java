package com.atguigu.day03.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @ClassName Flink01_Sink_Kafka
 * @Description //TODO 相当于自定义kafka的生产者，从9999端口接收数据发送到kafka中。
 * @Author Amor_leonard
 * @Date 2021/9/8 18:29
 * @Version 1.0
 **/
public class Flink01_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<String> map = streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                String[] splits = value.split(" ");
                WaterSensor waterSensor = new WaterSensor(splits[0], Long.parseLong(splits[1]), Integer.parseInt(splits[2]));

                //将waterSensor对象转为json字符串，发送到kafka
                String ws = JSON.toJSONString(waterSensor);
                return ws;
            }
        });

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        FlinkKafkaProducer<String> myproducer = new FlinkKafkaProducer<String>("topic_sensor", new SimpleStringSchema(), properties);

        map.addSink(myproducer);

        env.execute();

    }
}
