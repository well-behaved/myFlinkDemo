package com.xue.flink.demo.sql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author: xuexiong@souche.com
 * @date: 2023/2/8 18:27
 * @description:
 */
public class MySqlDetailDemo {
    public static void main(String[] args) throws Exception {
        /*
        环境准备和数据源定义
         */
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /*
        表环境 创建
         */
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                //分为 old planner  和 BlinkPlanner 一般使用 BlinkPlanner
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        //表环境
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, environmentSettings);

        /*
        使用sql 连接到 mysql的某张表 并且注册到flink
         */
        String kafkaConnectSql = new StringBuilder()
                .append("create table my_table_flink (")
                .append("   `user` varchar(40) null")
                .append("   ,url varchar(40) null")
                .append("   ,`time_stamp` TIMESTAMP null")
                .append(")with(")
                .append("   'connector.type'='jdbc',")
                .append("   'connector.url'='jdbc:mysql://127.0.0.1:3306/my_database?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=UTC',")
                .append("   'connector.table'='my_table',")
                .append("   'connector.driver'='com.mysql.jdbc.Driver',")
                .append("   'connector.username'='root',")
                .append("   'connector.password'='123456'")
                .append(")")
                .toString();

        streamTableEnvironment.executeSql(kafkaConnectSql);

        Table table = streamTableEnvironment.sqlQuery("select user,time_stamp from my_table_flink");
        table.printSchema();

        //注册一个临时表
        streamTableEnvironment.createTemporaryView("only_user", table);
        Table onlyUser = streamTableEnvironment.sqlQuery("select * from only_user ");
        streamTableEnvironment.toChangelogStream(onlyUser).print("onlyUser 测试");


        DataStream<Row> rowDataStream = streamTableEnvironment.toChangelogStream(table);
        rowDataStream.print("rowDataStream 测试");
        env.execute();


    }
}
