package com.xue.flink.demo.tableApi;

import com.xue.flink.demo.mySource.ClickSource;
import com.xue.flink.demo.mySource.Event;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author: xuexiong@souche.com
 * @date: 2023/2/8 10:59
 * @description: table api小例子
 */
public class TableApiSimpleDemo {
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


        /*
        DataStream转换为 table
         */
        Table table = streamTableEnvironment.fromDataStream(stream);

        /*
        指定table api
        获取bob的数据
         */
        Table userBobDataTable = table.select($("user"),$("url")).where($("user").isEqual("Bob"));
        /*
        表转化为流
         */
        DataStream<Row> rowDataStream = streamTableEnvironment.toDataStream(userBobDataTable);

        /*
        sink 输出
         */
        rowDataStream.print("rowDataStream");


        env.execute();
    }
}
