package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class Flink02_KeyState_ListState {
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


        //5.针对每个传感器输出最高的3个水位值
        keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            //TODO 1.定义状态
            private ListState<Integer> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //TODO 2.初始化状态
                listState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("list-State", Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //TODO 3.使用状态
                listState.add(value.getVc());

                //创建list集合用来存放状态中的数据
                ArrayList<Integer> list = new ArrayList<>();
                //获取状态中的数据
                for (Integer lastVc : listState.get()) {
                    list.add(lastVc);
                }

                //对list集合中的数据排序
                list.sort((o1, o2) -> o2-o1);

                //如果list中的数据大于三条则把第四天也就是最小的删除掉
                if (list.size()>3){
                    list.remove(3);
                }

                //将最大的三条数据存放到状态中
                listState.update(list);
                out.collect(list.toString());
            }
        }).print();

        env.execute();
    }
}
