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
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.HashMap;


public class Flink03_Sink_ES {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(" ");
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

        ElasticsearchSink.Builder<WaterSensor> sensorBuilder = new ElasticsearchSink.Builder<>(
                httphosts,
                new ElasticsearchSinkFunction<WaterSensor>() {
                    public IndexRequest createIndexRequest(WaterSensor ws) {
                        /**
                         * @Description //TODO 集合是key:value数据类型，可以代表json结构
                         */
//                        HashMap<String, Object> json = new HashMap<>();
//                        json.put("id",ws.getId());
//                        json.put("ts", ws.getTs());
//                        json.put("vc", ws.getVc());
//                        return Requests.indexRequest()
//                                .index("flink-0426")
//                                .type("_doc")
//                                .source(json);

                        String wsjson = JSON.toJSONString(ws);
                        return Requests.indexRequest()
                                .index("flink-0426")
                                .type("_doc")
                                .source(wsjson,XContentType.JSON);
                    }

                    @Override
                    public void process(WaterSensor element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                });

        //如果是无界流, 需要配置bulk的缓存
        sensorBuilder.setBulkFlushMaxActions(1);
        map.addSink(sensorBuilder.build());


        env.execute();
    }
}