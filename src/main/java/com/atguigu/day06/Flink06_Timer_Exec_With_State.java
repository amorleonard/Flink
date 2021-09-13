package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink06_Timer_Exec_With_State {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<WaterSensor> mapstream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(" ");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        KeyedStream<WaterSensor, Tuple> keyedStream = mapstream.keyBy("id");


        //5.监控水位传感器的水位值，如果水位值在五秒钟之内连续上升，则报警，并将报警信息输出到侧输出流。
        SingleOutputStreamOperator<String> result = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            //定义一个变量，用来保存上一次的水位值
            private ValueState<Integer> lastVc;

            //定义一个变量，用来存放定时器时间
            private ValueState<Long> timer;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastVc = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVc", Integer.class, Integer.MIN_VALUE));
                timer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class, Long.MIN_VALUE));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //1.拿当前值判断是否大于上一次水位值
                if (value.getVc() > lastVc.value()) {
                    //注册定时器
                   // if (timer.value() == Long.MIN_VALUE) {//证明没有注册过定时器
                        System.out.println("注册定时器。。。" + ctx.getCurrentKey());
                        timer.update(ctx.timerService().currentProcessingTime() + 5000);
                        ctx.timerService().registerProcessingTimeTimer(timer.value());
                    //}
                } else {
                    //当前水位值没有大于上次水位值
                    System.out.println("删除定时器" + ctx.getCurrentKey());
                    ctx.timerService().deleteProcessingTimeTimer(timer.value());

                    //重置定时器时间
                    timer.clear();
                }

                //将当前水位保存到lastVc中
                lastVc.update(value.getVc());
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                //定时器触发
                //重置定时器时间
                timer.clear();
                ctx.output(new OutputTag<String>("output"){}, "连续5s水位上升，报警！！！！！");
            }
        });

        result.getSideOutput(new OutputTag<String>("output") {}).print();

        env.execute();
    }
}
