package com.xue.flink.demo.sql;

import com.xue.flink.demo.mySource.ClickSource;
import com.xue.flink.demo.mySource.Event;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author: xuexiong@souche.com
 * @date: 2023/2/8 14:49
 * @description: sql入门例子
 */
public class MySqlDemo {
    public static void main(String[] args) throws Exception {
        /*
        环境准备和数据源定义
         */
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //数据源
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        stream.print();


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
        //数据源 注册 临时表
        streamTableEnvironment.createTemporaryView("user_click_table", stream);



        /*
        执行sql
         */
        //筛选 Bob 的点击数据
        String bobSql = "select * from user_click_table where user = 'Bob' ";
        Table userBobDataTable = streamTableEnvironment.sqlQuery(bobSql);

        //统计bob 的点击数量
        String bobCountSql = "select user, count(user) from user_click_table where user = 'Bob' group by user ";
        Table userBobCpuntDataTable = streamTableEnvironment.sqlQuery(bobCountSql);

        /*
        表转化为流
         */
        DataStream<Row> rowDataStream = streamTableEnvironment.toDataStream(userBobDataTable);
        //有变化的可以调用 toChangelogStream
        DataStream<Row> userBobCountDataStream = streamTableEnvironment.toChangelogStream(userBobCpuntDataTable);

        /*
        sink 输出
         */
        rowDataStream.print("bob点击的数据");
        userBobCountDataStream.print("bob点击的数量统计");


        env.execute();


    }
}
