package com.tomato.flinkdemo.test01;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

/**
 * @author w-tomato
 * @description 数据迁移  从test01表到test02表
 * @date 2024/1/4
 */
public class Test0103 {

    public static void main(String[] args) {
        try{
            // 建表语句在最下边，字段没啥意义，单纯为了测试
            String sql_select="select id, name,`count`,category from db_test01";
            String sql_insert="insert into db_test02 (name, category_count) values (?, ?)";
            //创建数据源
            JDBCInputFormat jdbcInputFormat_Select = JDBCInputFormat.buildJDBCInputFormat()
                    .setDBUrl("jdbc:mysql://localhost:3306/flink_test")
                    .setDrivername("com.mysql.jdbc.Driver")
                    .setUsername("root")
                    .setPassword("whc123")
                    // 字段类型和数量要和sql_select查询出来的字段数量一致
                    .setRowTypeInfo(new RowTypeInfo(Types.INT, Types.STRING, Types.INT, Types.STRING))
                    .setQuery(sql_select)
                    .finish();
            JDBCOutputFormat jdbcOutputFormat_Insert = JDBCOutputFormat.buildJDBCOutputFormat()
                    .setDBUrl("jdbc:mysql://localhost:3306/flink_test")
                    .setDrivername("com.mysql.jdbc.Driver")
                    .setUsername("root")
                    .setPassword("whc123")
                    .setQuery(sql_insert)
                    .finish();
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            DataSet<Row> ds = env.createInput(jdbcInputFormat_Select);
            ds.map(new MapFunction<Row, Row>() {
                @Override
                public Row map(Row row) throws Exception {
                    // 从1开始，0是id，其实上边可以不用select id，懒得改了
                    String name = row.getField(1).toString();
                    String category = row.getField(3).toString();
                    String category_count = category + "_" + row.getField(2).toString();
                    Row row1 = new Row(2);
                    row1.setField(0, name);
                    row1.setField(1, category_count);
                    return row1;
                }
            }).returns(Types.ROW(Types.STRING, Types.STRING)).output(jdbcOutputFormat_Insert);
            env.setParallelism(1);
            env.execute();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

/*create table `db_test01` (
        `id` int NOT NULL AUTO_INCREMENT COMMENT '记录ID',
        `name` varchar(20) NOT NULL COMMENT '名称',
        `count` int NOT NULL  COMMENT '数量',
        `category` varchar(10) DEFAULT NULL COMMENT '分类',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

create table `db_test02` (
        `id` int NOT NULL AUTO_INCREMENT COMMENT '记录ID',
        `name` varchar(20) NOT NULL COMMENT '名称',
        `category_count` varchar(20) NOT NULL  COMMENT '分类_数量',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO `db_test01` (`name`, `count`, `category`) VALUES
('iPhone12', 5, '手机'),
('拍立得', 3, '相机'),
('Xbox手柄', 2, '游戏设备'),
('MacBook Pro', 8, '电脑'),
('AirPods Pro', 4, '耳机'),
('Kindle Paperwhite', 6, '电子书阅读器'),
('PlayStation 5', 1, '游戏设备'),
('Canon EOS R5', 9, '相机'),
('Nintendo Switch', 7, '游戏设备'),
('Sony WH-1000XM4', 3, '耳机'),
('iPad Air', 5, '平板电脑');
*/

}
