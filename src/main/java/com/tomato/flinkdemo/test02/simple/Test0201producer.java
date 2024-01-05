package com.tomato.flinkdemo.test02.simple;

import com.alibaba.fastjson.JSON;
import com.tomato.flinkdemo.test02.util.ConnectMySqlSource;
import com.tomato.flinkdemo.test02.util.SourceVO;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author w-tomato
 * @description 从mysql读取数据发送到kafka
 * @date 2024/1/6
 */
public class Test0201producer {

    public static void main(String[] args) {
        try {
            final Logger LOG = LoggerFactory.getLogger(Test0201producer.class);

            Properties properties = new Properties();
            properties.put("group.id", "flink-kafka-connector");
            properties.put("bootstrap.servers", "127.0.0.1:9092");
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStreamSource<SourceVO> source =  env.addSource(new ConnectMySqlSource());
            source.map(new MapFunction<SourceVO, String>() {
                @Override
                public String map(SourceVO sourceVO) throws Exception {
                    String sourceVoStr= JSON.toJSON(sourceVO).toString();
                    System.out.println("已读取 并发送到db_kafka_topic：" + sourceVoStr);
                    LOG.info("已读取 并发送到db_kafka_topic：" + sourceVoStr);
                    return sourceVoStr;
                }
            }).addSink(new FlinkKafkaProducer011<>("db_kafka_topic", new SimpleStringSchema(), properties));
            env.execute("Test02consumer db to kafka");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
