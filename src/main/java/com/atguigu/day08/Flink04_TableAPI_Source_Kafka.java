package com.atguigu.day08;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Flink04_TableAPI_Source_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //通过Connector声明读入数据
        Schema schema = new Schema()
                .field("id", "STRING")
                .field("ts", "BIGINT")
                .field("vc", "INT");

        tableEnv.connect(
                        new Kafka()
                                .version("universal")
                                .topic("sensor")
                                .startFromLatest()
                                .property("group.id", "bigdata")
                                .property("bootstrap.servers", "hadoop102:9092,hadoop102:9092,hadoop102:9092")
                )
                //.withFormat(new Json())
                .withFormat(new Csv().lineDelimiter("\n"))
                .withSchema(schema)
                .createTemporaryTable("sensor");

        //获取创建的临时表
        Table table = tableEnv.from("sensor");

        //动态查询数据
        Table selecttable = table
                .groupBy($("id"))
                .select($("id"), $("vc").sum());

        //将结果动态表转化为流
        DataStream<Tuple2<Boolean, Row>> result = tableEnv.toRetractStream(selecttable, Row.class);
        //打印结果
        result.print();

        env.execute();


    }
}
