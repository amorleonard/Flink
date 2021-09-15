package com.atguigu.day07;

import com.atguigu.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

//统计创建订单到下单中间超过15分钟的超时数据以及正常的数据。
public class Flink11_CEP_Project_OrderWatch {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("input/OrderLog.csv");

        SingleOutputStreamOperator<OrderEvent> orderEventStream = streamSource.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new OrderEvent(
                        Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3])
                );
            }
        });

        //分配watermark
        SingleOutputStreamOperator<OrderEvent> ordereventtime = orderEventStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                            @Override
                            public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                                return element.getEventTime()*1000;
                            }
                        })
        );

        //按照orderid进行分区
        KeyedStream<OrderEvent, Long> orderEventLongKeyedStream = ordereventtime.keyBy(OrderEvent::getOrderId);

        //定义模式
        Pattern<OrderEvent, OrderEvent> within = Pattern.<OrderEvent>begin("create")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                .next("pay")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.minutes(15));

        //将模式作用于流上
        PatternStream<OrderEvent> pattern = CEP.pattern(orderEventLongKeyedStream, within);

        //筛选想要的数据,将不匹配的数据输出到侧输出流
        SingleOutputStreamOperator<String> selection = pattern.select(
                new OutputTag<String>("output") {
                },
                new PatternTimeoutFunction<OrderEvent, String>() {
                    @Override
                    public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
                        return pattern.toString();
                    }
                },
                new PatternSelectFunction<OrderEvent, String>() {
                    @Override
                    public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
                        return pattern.toString();
                    }
                }
        );

        //将正常的数据输出到主流
        selection.print();

        //将超市数据输出到侧输出流
        selection.getSideOutput(new OutputTag<String>("output"){}).print("超时未支付");

        env.execute();
    }
}
