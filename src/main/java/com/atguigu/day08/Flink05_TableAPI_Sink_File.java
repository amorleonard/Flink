package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

public class Flink05_TableAPI_Sink_File {
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

        //创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //转化为动态表
        Table table = tableEnv.fromDataStream(waterSensorStream);

        //查询动态表数据，转化为结果表
        Table resultTable = table.where($("id").isEqual("sensor_1")).select($("id"), $("ts"), $("vc"));

        //连接外部系统
        Schema schema = new Schema().field("id", "String")
                .field("ts", "BIGINT")
                .field("vc", "Integer");

        tableEnv.connect(new FileSystem().path("output/sensor-sql.txt"))
                .withFormat(new Csv().fieldDelimiter('|'))
                .withSchema(schema)
                .createTemporaryTable("sensor");

        //将结果表数据插到临时表中
        resultTable.executeInsert("sensor");

    }
}
