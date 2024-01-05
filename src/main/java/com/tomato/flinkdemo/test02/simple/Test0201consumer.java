package com.tomato.flinkdemo.test02.simple;

import com.alibaba.fastjson.JSONObject;
import com.tomato.flinkdemo.test02.util.ConnectMySqlSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author w-tomato
 * @description 读取kafka数据 写入mysql
 * @date 2024/1/6
 */
public class Test0201consumer {

    public static void main(String[] args) {
        try {
            final Logger LOG = LoggerFactory.getLogger(Test0201consumer.class);

            Properties properties = new Properties();
            properties.put("group.id", "flink-kafka-connector");
            properties.put("bootstrap.servers", "127.0.0.1:9092");
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStreamSource<String> message = env.addSource(new FlinkKafkaConsumer011<>("db_kafka_topic", new SimpleStringSchema(), properties));
            message.map(new MapFunction<String, JSONObject>() {
                            @Override
                            public JSONObject map(String s) throws Exception {
                                System.out.println("已收到 并入库：" + s);
                                LOG.info("已收到 并入库：" + s);
                                JSONObject jsonObject = JSONObject.parseObject(s);
                                String name = jsonObject.getString("name");
                                String count = jsonObject.getString("count");
                                String category = jsonObject.getString("category");
                                JSONObject root=new JSONObject();
                                root.put("name", name);
                                root.put("category_count", category + "_" + count);

                                return root;
                            }
                        }

            ).addSink(new ConnectMySqlSink());
            env.execute("Test02consumer kafka to db");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
