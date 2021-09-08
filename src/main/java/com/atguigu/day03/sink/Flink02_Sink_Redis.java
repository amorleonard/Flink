package com.atguigu.day03.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @ClassName Flink02_Sink_Redis
 * @Description //TODO 发送数据到redis中
 * @Author Amor_leonard
 * @Date 2021/9/8 19:22
 * @Version 1.0
 **/
public class Flink02_Sink_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] splits = value.split(" ");
                WaterSensor waterSensor = new WaterSensor(splits[0], Long.parseLong(splits[1]), Integer.parseInt(splits[2]));
                return waterSensor;
            }
        });

        FlinkJedisPoolConfig flinkconf = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").build();
        map.addSink(new RedisSink<>(flinkconf, new myRedis()));
        env.execute();
    }


    public static class myRedis implements RedisMapper<WaterSensor>{
        /**
         * @author Amor_leonard
         * @Description //TODO 指定用什么类型存储，第二个参数是redis的大key
         * @date 2021/9/8 19:45
         * @Param []
         * @return org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription
         */
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"0426");
        }

        /**
         * @author Amor_leonard
         * @Description //TODO 指定hash中的小key
         * @date 2021/9/8 19:46
         * @Param [waterSensor]
         * @return java.lang.String
         */
        @Override
        public String getKeyFromData(WaterSensor waterSensor) {
            return waterSensor.getId();
        }

        /**
         * @author Amor_leonard
         * @Description //TODO 获取所要存储的value
         * @date 2021/9/8 19:47
         * @Param [waterSensor]
         * @return java.lang.String
         */
        @Override
        public String getValueFromData(WaterSensor waterSensor) {
            String ws = JSON.toJSONString(waterSensor);
            return ws;
        }
    }
}
