package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink09_SideOutput_Exec {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.将端口读过来数据转为WaterSensor
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //4.将相同时间的水位传感器的数据聚和到一块
        KeyedStream<WaterSensor, Long> keyedStream = waterSensorStream.keyBy(WaterSensor::getTs);

        //5.采集监控传感器水位值，将水位值高于5cm的值输出到side output
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.process(new KeyedProcessFunction<Long, WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                //首先将所有的数据输出到主流
                out.collect(value);
                //判断如果传感器发送过来的水位高于5cm则往侧输出输出一份
                if (value.getVc() > 5) {
                    ctx.output(new OutputTag<WaterSensor>("output") {
                    }, value);
                }
            }
        });

        result.print("主流");

        //获取侧输出流的数据并打印
        result.getSideOutput(new OutputTag<WaterSensor>("output") {
        }).print("水位高于5cm");

        env.execute();
    }
}
