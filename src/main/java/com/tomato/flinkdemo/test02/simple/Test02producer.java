package com.tomato.flinkdemo.test02.simple;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * @author w-tomato
 * @description 生产1000个1-10之间的随机数发送到kafka
 * @date 2024/1/5
 */
public class Test02producer {

    public static void main(String[] args) {
        for(int i = 0; i <= 1000; i++){
            Random random = new Random();
            int intItem = random.nextInt(1000);
            String numStr = String.valueOf(intItem % 10);
            Test02producer test02producer = new Test02producer();
            test02producer.send(numStr);
            System.out.println("已发送数据:"+numStr);
        }
    }

    /**发送方法
     * 发送消息配置
     * @param value
     */
    public void send(String value){
        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");//xxx服务器ip
        props.put("acks", "all");//所有follower都响应了才认为消息提交成功，即"committed"
        props.put("retries", 0);//retries = MAX 无限重试，直到你意识到出现了问题:)
        //props.put("batch.size", 16384);//producer将试图批处理消息记录，以减少请求次数.默认的批量处理消息字节数
        //batch.size当批量的数据大小达到设定值后，就会立即发送，不顾下面的linger.ms
        props.put("linger.ms", 1);//延迟1ms发送，这项设置将通过增加小的延迟来完成--即，不是立即发送一条记录，producer将会等待给定的延迟时间以允许其他消息记录发送，这些消息记录可以批量处理
        // props.put("buffer.memory", 33554432);//producer可以用来缓存数据的内存大小。
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        final  String TOPIC = "test1";
        // 用try-with-resources的方式当执行完代码块会自动关闭producer
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(TOPIC,value));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
