package com.atguigu.day03.sink;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Driver;

/**
 * @ClassName Flink05_Sink_JDBC
 * @Description //TODO
 * @Author Amor_leonard
 * @Date 2021/9/9 10:55
 * @Version 1.0
 **/
public class Flink05_Sink_JDBC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {

                String[] splits = value.split(" ");

                WaterSensor ws = new WaterSensor(splits[0], Long.parseLong(splits[1]), Integer.parseInt(splits[2]));

                return ws;
            }
        });

        map.addSink(JdbcSink.sink(
                "insert into sensor values(?,?,?)",
                (ps,ws)->{
                    ps.setString(1, ws.getId());
                    ps.setLong(2, ws.getTs());
                    ps.setInt(3, ws.getVc());
                },
                //与ES写入数据时一样，通过阈值控制什么时候写入数据，以下设置为来一条写一条
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop102:3306/test")
                        .withDriverName(Driver.class.getName())
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
                ));

        env.execute();
    }
}
