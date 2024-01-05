package com.tomato.flinkdemo.test01;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Set;

/**
 * @author w-tomato
 * @description 读取文件做品类个数统计
 * @date 2024/1/4
 */
public class Test0101 {
    public static void main(String[] args) {
        try {
            // 1.准备环境
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            // 2.加载数据源
            DataSource<String> ds = env.readTextFile("data/商品-分类.txt");
            // 3.数据转换
            ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                           public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                               String[] ObjectStrArr = s.split(";");
                               for (String objectStr : ObjectStrArr) {
                                   JSONObject jsonObject = JSON.parseObject(objectStr);
                                   Set<String> keys = jsonObject.keySet();
                                   Iterator<String> iterator = keys.iterator();
                                   while (iterator.hasNext()) {
                                       String next = iterator.next();
                                       String category = jsonObject.getString(next);
                                       collector.collect(new Tuple2<>(category, 1));
                                   }

                               }
                           }
                       }
            ).groupBy(0).reduce((ReduceFunction<Tuple2<String, Integer>>) (stringIntegerTuple2, t1) -> new Tuple2<>(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + t1.f1)).print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
