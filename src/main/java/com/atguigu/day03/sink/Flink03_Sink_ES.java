package com.atguigu.day03.sink;


import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;


public class Flink03_Sink_ES {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));

                return waterSensor;
            }
        });

        //TODO  将数据发送至ES
        ArrayList<HttpHost> httphosts = new ArrayList<>();
        HttpHost httpHost = new HttpHost("hadoop102", 9200);
        HttpHost httpHost2 = new HttpHost("hadoop103", 9200);
        HttpHost httpHost3 = new HttpHost("hadoop104", 9200);
        httphosts.add(httpHost);
        httphosts.add(httpHost2);
        httphosts.add(httpHost3);

        ElasticsearchSink.Builder<WaterSensor> sensorBuilder = new ElasticsearchSink.Builder<>(httphosts, new ElasticsearchSinkFunction<WaterSensor>() {


            @Override
            public void process(WaterSensor element, RuntimeContext ctx, RequestIndexer indexer) {
                //指定索引名，类型，docId
                IndexRequest indexRequest = new IndexRequest("flink-0426", "_doc", "1001");
                String jsonString = JSON.toJSONString(element);
                //指定要写入的数据
                IndexRequest request = indexRequest.source(jsonString, XContentType.JSON);

                indexer.add(request);
            }
        });

        sensorBuilder.setBulkFlushMaxActions(1);
        map.addSink(sensorBuilder.build());


        env.execute();
    }
}