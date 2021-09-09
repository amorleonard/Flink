package com.atguigu.day04;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @ClassName Flink01_Project_PV
 * @Description //TODO 网站总浏览量（PV）的统计
 * @Author Amor_leonard
 * @Date 2021/9/9 14:04
 * @Version 1.0
 **/
public class Flink01_Project_PV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("E:\\BigData0426\\Flink\\input\\UserBehavior.csv");

        //将数据转换为JavaBean
        SingleOutputStreamOperator<UserBehavior> map = streamSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] splits = value.split(",");
                UserBehavior ub = new UserBehavior(Long.parseLong(splits[0]), Long.parseLong(splits[1]), Integer.parseInt(splits[2]), splits[3], Long.parseLong(splits[4]));
                return ub;
            }
        });

        //过滤用户行为是pv的用户
        SingleOutputStreamOperator<UserBehavior> filter = map.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                if ("pv".equals(value.getBehavior())) {
                    return true;
                }
                return false;
            }
        });

        //将数据转化为tuple类型
        SingleOutputStreamOperator<Tuple2<String, Long>> toone = filter.map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                return Tuple2.of(value.getBehavior(), 1l);
            }
        });

        //按照key聚合
        KeyedStream<Tuple2<String, Long>, Tuple> keyBy = toone.keyBy(0);

        //sum求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyBy.sum(1);

        //打印结果
        sum.print();

        env.execute();

    }
}
