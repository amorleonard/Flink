package com.atguigu.day04;

import com.atguigu.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

//APP市场推广统计 - 分渠道
public class Flink04_Project_AppAnalysis_By_Chanel {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //自定义source
        DataStreamSource<MarketingUserBehavior> addSource = env.addSource(new AppMarketingDataSource());


        SingleOutputStreamOperator<Tuple2<String, Integer>> channeltobehavior = addSource.flatMap(new FlatMapFunction<MarketingUserBehavior, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(MarketingUserBehavior value, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(Tuple2.of(value.getChannel() + "-" + value.getBehavior(), 1));
            }
        });

        //按照key分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyBy = channeltobehavior.keyBy(0);

        keyBy.sum(1).print();

        env.execute();
    }

    public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior> {
        boolean canRun = true;
        Random random = new Random();
        List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (canRun) {
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                        (long) random.nextInt(1000000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis());
                ctx.collect(marketingUserBehavior);
                Thread.sleep(200);
            }
        }

        @Override
        public void cancel() {
            canRun = false;
        }
    }
}
