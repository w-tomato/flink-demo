package com.tomato.flinkdemo.test02.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author w-tomato
 * @description
 * @date 2024/1/6
 */
public class ConnectMySqlSource extends RichSourceFunction<SourceVO> {

    private static final Logger logger = LoggerFactory.getLogger(ConnectMySqlSource.class);
    private Connection connection = null;

    private PreparedStatement ps = null;
    @Override
    public void open(Configuration parameters) throws Exception {

        super.open(parameters);

        Class.forName("com.mysql.jdbc.Driver");//加载数据库驱动

        connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/flink_test", "root", "whc123");//获取连接

        ps = connection.prepareStatement("select name, `count`, category from db_test01 ");

    }
    @Override
    public void run(SourceContext<SourceVO> ctx) throws Exception {

        try {

            ResultSet resultSet = ps.executeQuery();


            while (resultSet.next()) {

                SourceVO vo = new SourceVO();

                vo.setId(resultSet.getString("id"));
                vo.setName(resultSet.getString("name"));
                vo.setCount(resultSet.getString("count"));
                vo.setCategory(resultSet.getString("category"));
                ctx.collect(vo);

            }

        } catch (Exception e) {

            logger.error("runException:{}", e);

        }
    }

    @Override
    public void cancel() {

        try {

            super.close();

            if (connection != null) {

                connection.close();

            }

            if (ps != null) {

                ps.close();

            }

        } catch (Exception e) {

            logger.error("runException:{}", e);

        }

    }

}
