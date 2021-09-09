package com.atguigu.day02.source;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;


/**
 * @ClassName Flink04_Source_Custom
 * @Description //TODO 自定义source,自定义数据源
 * @Author Amor_leonard
 * @Date 2021/9/6 17:56
 * @Version 1.0
 **/
public class Flink04_Source_Custom {
    public static void main(String[] args) throws Exception {
        //1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.addSource(new SourceFunction<WaterSensor>() {
            private boolean isRuning = true;
            private volatile Random random = new Random();

            @Override
            public void run(SourceContext<WaterSensor> ctx) throws Exception {
                while (isRuning) {
                    ctx.collect(new WaterSensor("sensor_" + this.random.nextInt(10), System.currentTimeMillis(), this.random.nextInt(10)));

                    Thread.sleep(20);
                }
            }

            @Override
            public void cancel() {
                isRuning = false;
            }

        });

        waterSensorDataStreamSource.print();

        env.execute();
    }
}
