package com.tomato.flinkdemo.test02.simple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author w-tomato
 * @description 用flink从消息队列读取数据统计个数
 *
 * 消费的时候不知道为什么拿不到消息，在终端用命令行消费一次之后就可以了
 * @date 2024/1/5
 */
public class Test02consumer {

    public static void main(String[] args) {
        try {
            Properties properties = new Properties();
            properties.put("group.id", "flink-kafka-connector");
            properties.put("bootstrap.servers", "127.0.0.1:9092");
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStreamSource<String> message = env.addSource(new FlinkKafkaConsumer011<String>("test1", new SimpleStringSchema(), properties));
            message.map(new MapFunction<String, Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> map(String s) {
                                Tuple2 tuple2 = new Tuple2<>(s, 1);
                                System.out.println("已接收：" + s);
                                return tuple2;
                            }
                        }

                    )
                    .keyBy(0)
                    .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                                @Override
                                public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t0, Tuple2<String, Integer> t1) throws Exception {
                                    System.out.println("已处理：" + t0.f0 + "---" + t0.f1 + t1.f1);
                                    return new Tuple2<>(t0.f0, t0.f1 + t1.f1);
                                }
                            }
                    )
                    .print();
            env.execute("Test02consumer cd");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
