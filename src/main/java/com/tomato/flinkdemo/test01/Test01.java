package com.tomato.flinkdemo.test01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * @author w-tomato
 * @description 读取文件统计词语个数
 * @date 2024/1/4
 */
public class Test01 {
    public static void main(String[] args) {
        try{
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            DataSource<String> ds = env.readTextFile("data/词.txt");
            // collector是一个收集器，用于收集数据，由flink自动创建
            // 这块虽然IDE标灰色提示可以用lambda表达式，但改成lambda会报错
            // 这里打断点看了一下ds里是一行一行的数据
            ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                    String[] strArr = s.split(",");
                    for (String item : strArr) {
                        // collector.collect()方法用于收集数据，这里收集的是一个Tuple2类型的数据，第一个是单词，第二个是1
                        // 1的意思是，这个单词出现了一次，这里的1会在下面的reduce中进行累加
                        // Tuple2这个类型是flink自带的，Tuple是元组的意思，用于封装两个数据，同样的还有Tuple3、Tuple4等等
                        collector.collect(new Tuple2<>(item, 1));
                    }
                }
            })
                    // groupBy(0)的意思是按照Tuple2的第一个元素进行分组，也就是按照单词进行分组
                    .groupBy(0)
                    // reduce方法是对数据进行聚合，这里的聚合是对第二个元素进行累加
                    // reduce里的t0和t1是两个相邻的元素，而且他们的f0是相同的，因为上面的groupBy(0)已经按照f0分组了
                    // 就是说同组的每两个相邻的元素都会进入reduce方法，然后把他们的f1相加，然后再把f1相加的结果封装成一个新的Tuple2
                    .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t0, Tuple2<String, Integer> t1) throws Exception {
                            return new Tuple2<String, Integer>(t0.f0,t0.f1+ t1.f1) ;
                        }
                    }).print();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
