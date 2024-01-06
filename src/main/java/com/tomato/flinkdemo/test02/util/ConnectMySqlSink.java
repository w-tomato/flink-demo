package com.tomato.flinkdemo.test02.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author w-tomato
 * @description
 * @date 2024/1/6
 */
public class ConnectMySqlSink extends RichSinkFunction<JSONObject> {

    private static final Logger logger = LoggerFactory.getLogger(ConnectMySqlSink.class);
    private Connection connection;

    private PreparedStatement preparedStatement;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        Class.forName("com.mysql.jdbc.Driver");

        // 获取数据库连接
        connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/flink_test", "root", "whc123");//获取连接

        preparedStatement = connection.prepareStatement("insert into db_test02 (name, category_count)values(?,?)");

        super.open(parameters);

    }

    @Override
    public void close()throws Exception {
        super.close();
        if(preparedStatement != null){

            preparedStatement.close();

        }

        if(connection != null){

            connection.close();

        }

        super.close();

    }
    @Override
    public void invoke(JSONObject s) throws Exception {

        try {
            preparedStatement.setString(1,s.getString("name"));
            preparedStatement.setString(2,s.getString("category_count"));
            preparedStatement.executeUpdate();

        }catch (Exception e){

            e.printStackTrace();

        }

    }

}
