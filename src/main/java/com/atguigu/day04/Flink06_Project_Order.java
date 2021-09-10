package com.atguigu.day04;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class Flink06_Project_Order {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //订单事件流
        DataStreamSource<String> orderevent = env.readTextFile("E:\\BigData0426\\Flink\\input\\OrderLog.csv");

        //交易事件流
        DataStreamSource<String> receiptlog = env.readTextFile("E:\\BigData0426\\Flink\\input\\ReceiptLog.csv");

        //将两条流都转为pojo
        SingleOutputStreamOperator<OrderEvent> ordereventpojo = orderevent.flatMap(new FlatMapFunction<String, OrderEvent>() {
            @Override
            public void flatMap(String value, Collector<OrderEvent> out) throws Exception {
                String[] split = value.split(",");

                OrderEvent orderEvent = new OrderEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));

                out.collect(orderEvent);
            }
        });


        SingleOutputStreamOperator<TxEvent> txeventpojo = receiptlog.flatMap(new FlatMapFunction<String, TxEvent>() {
            @Override
            public void flatMap(String value, Collector<TxEvent> out) throws Exception {
                String[] split = value.split(",");

                TxEvent txEvent = new TxEvent(split[0], split[1], Long.parseLong(split[2]));

                out.collect(txEvent);
            }
        });

        //连接两条流
        ConnectedStreams<OrderEvent, TxEvent> connectedStreams = ordereventpojo.connect(txeventpojo);

        //聚合相同交易码的数据
        ConnectedStreams<OrderEvent, TxEvent> keyBy = connectedStreams.keyBy("txId", "txId");

        keyBy.process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>() {
            //ordermap缓存order数据
            HashMap<String, OrderEvent> ordermap = new HashMap<>();

            //txmap缓存tx数据
            HashMap<String, TxEvent> txmap = new HashMap<>();

            @Override
            public void processElement1(OrderEvent value, KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>.Context ctx, Collector<String> out) throws Exception {
                //如果有缓存数据
                if(txmap.containsKey(value.getTxId())){
                    out.collect("订单:" + value.getOrderId() + "对账成功");
                    txmap.remove(value.getTxId());
                }else {
                    //没有缓存数据
                    ordermap.put(value.getTxId(), value);
                }
            }

            @Override
            public void processElement2(TxEvent value, KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>.Context ctx, Collector<String> out) throws Exception {

                //如果有缓存数据
                if(txmap.containsKey(value.getTxId())){
                    out.collect("订单:" + ordermap.get(value.getTxId()).getOrderId() + "对账成功");
                    txmap.remove(value.getTxId());
                }else {
                    //没有缓存数据
                    txmap.put(value.getTxId(), value);
                }
            }
        }).print();

        env.execute();
    }
}

