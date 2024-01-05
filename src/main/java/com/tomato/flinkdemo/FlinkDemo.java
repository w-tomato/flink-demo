package com.tomato.flinkdemo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author w-tomato
 * @description
 * @date 2024/1/4
 */
public class FlinkDemo {
    public static void main(String[] args) throws Exception {
        // 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置运行模式 现在默认为自动模式 这个方法已经被废弃
//        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 2.加载数据源
        DataStreamSource<String> elementsSource = env.fromElements("java,scala,php,c++",
                "java,scala,php", "java,scala", "java");
        // 3.数据转换
        DataStream<String> flatMap = elementsSource.flatMap((FlatMapFunction<String, String>) (element, out) -> {
            String[] wordArr = element.split(",");
            for (String word : wordArr) {
                out.collect(word);
            }
        });
        //DataStream 下边为DataStream子类
        SingleOutputStreamOperator<String> source = flatMap.map((MapFunction<String, String>) String::toUpperCase);
        // 4.数据输出
        source.print();
        // 5.执行程序
        env.execute("flink-hello-world");
    }
}
