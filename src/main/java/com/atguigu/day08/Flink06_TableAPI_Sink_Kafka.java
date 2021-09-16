package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

public class Flink06_TableAPI_Sink_Kafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));

        //获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //创建动态表
        Table table = tableEnv.fromDataStream(waterSensorStream);

        //从动态表中查询数据
        Table selection = table.where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));

        //连接外部系统
        Schema schema = new Schema().field("id", "STRING")
                .field("ts", "BIGINT")
                .field("vc", "INT");

        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("sensor")
                .sinkPartitionerRoundRobin()
                .property("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092")
        ).withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        selection.executeInsert("sensor");

    }
}
