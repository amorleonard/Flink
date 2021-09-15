package com.atguigu.day07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 总结：
 * 模式序列：每个复杂的模式序列包含多个简单的模式(简单模式通过严格连续、松散连续、非确定的松散连续进行连接就形成了模式模式序列)
 * 单个模式可以是单例模式或者循环模式.
 * 单例模式：只接受一个事件，也就相当于接收一条数据
 * 循环模式：循环模式可以接收多个事件，单例模式+量词
 * 连续性可以用在单例模式上也可以用在单个循环模式上
 * 单例模式连接其他模式时使用next("严格连续")、followBy("松散连续")、followedByAny("非确定的松散连续")
 * 循环模式连接其他模式时使用consecutive("严格连续")、默认是松散连续、allowCombinations("非确定的松散连续")
 */
public class Flink04_CEP_Mode {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("E:\\BigData0426\\Flink\\input\\Sensor.txt");

        SingleOutputStreamOperator<WaterSensor> mapStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = mapStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs() * 1000;
                            }
                        })
        );

        //定义模式
        Pattern<WaterSensor, WaterSensor> pattern = Pattern.<WaterSensor>begin("start")
                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value, Context<WaterSensor> ctx) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                })
                //严格连续(一对一)
                .next("end")
                //不想后面直接连着一个特定事件
                //.notNext("end")
                //松散连续(一对多)
                //.followedBy("followedby")
                //非确定的松散连续(多对多)
                //.followedByAny("followedany")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_2".equals(value.getId());
                    }
                });

        //将模式作用于流上
        PatternStream<WaterSensor> patternStream = CEP.pattern(waterSensorSingleOutputStreamOperator, pattern);

        //获取符合要求的数据
        patternStream.select(new PatternSelectFunction<WaterSensor, String>() {
            @Override
            public String select(Map<String, List<WaterSensor>> pattern) throws Exception {
                return pattern.toString();
            }
        }).print();

        env.execute();
    }
}
