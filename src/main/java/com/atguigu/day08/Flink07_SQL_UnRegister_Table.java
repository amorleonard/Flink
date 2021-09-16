package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;

import lombok.SneakyThrows;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Flink07_SQL_UnRegister_Table {

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //将流转换为动态表(未注册)
        Table oragintable = tableEnv.fromDataStream(waterSensorStream);

        //sql查询得到结果表
        //Table resulttable = tableEnv.sqlQuery("select * from " + oragintable + " where id='sensor_1'");
        //方式二：直接将结果表进行execute,并打印
        //resulttable.execute().print();

        //方式三，直接对表的执行环境调用executerSql()方法，返回的是一个TableResult对象，可以直接打印
        tableEnv.executeSql("select * from " + oragintable + " where id='sensor_1'").print();


/*      打印结果方式一：将动态表转化为流，打印流
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(resulttable, Row.class);

        tuple2DataStream.print();

        env.execute();*/
    }
}
