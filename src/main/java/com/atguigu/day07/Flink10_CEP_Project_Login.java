package com.atguigu.day07;

import com.atguigu.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

//用户2秒内连续两次及以上登录失败则判定为恶意登录。
public class Flink10_CEP_Project_Login {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("E:\\BigData0426\\Flink\\input\\LoginLog.csv");

        SingleOutputStreamOperator<LoginEvent> mapStream = streamSource.map(new MapFunction<String, LoginEvent>() {
            @Override
            public LoginEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new LoginEvent(
                        Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3])
                );
            }
        });

        //分配watermark
        SingleOutputStreamOperator<LoginEvent> loginEventSingleOutputStreamOperator = mapStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                            @Override
                            public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                return element.getEventTime() * 1000;
                            }
                        })
        );

        //按照userid进行分组
        KeyedStream<LoginEvent, Long> keyedStream = loginEventSingleOutputStreamOperator.keyBy(LoginEvent::getUserId);

        //定义模式
        Pattern<LoginEvent, LoginEvent> loginEventPattern = Pattern.<LoginEvent>begin("begin")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                        return "fail".equals(value.getEventType());
                    }
                })
                .timesOrMore(2)
                //严格连续
                .consecutive()
                //within()表示超时数据
                .within(Time.seconds(2));

        //将模式作用于流上
        PatternStream<LoginEvent> pattern = CEP.pattern(keyedStream, loginEventPattern);

        //筛选需要的数据
        pattern.select(new PatternSelectFunction<LoginEvent,String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                return pattern.toString();
            }
        }).print();

        env.execute();

    }
}
