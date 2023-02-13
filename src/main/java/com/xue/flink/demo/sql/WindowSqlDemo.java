package com.xue.flink.demo.sql;

import com.xue.flink.demo.mySource.ClickSource;
import com.xue.flink.demo.mySource.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.util.Optional;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author: xuexiong@souche.com
 * @date: 2023/2/10 14:16
 * @description: 窗口相关sql 测试
 */
public class WindowSqlDemo {
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
        水位线设置
         */
        SingleOutputStreamOperator<Event> singleOutputStreamOperator = stream.assignTimestampsAndWatermarks(
                //乱序 水位线生成
                WatermarkStrategy
                        // //指定乱序生成器 设置两秒延迟
                        .<Event>forBoundedOutOfOrderness(Duration.ofMillis(2000))
                        //设置获取时间戳逻辑
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> {
                                    //自定义取出 数据中的时间 这里取得是数据中的happenDate 字段
                                    return Optional.ofNullable(element).map(Event::getTimestamp).orElse(0L);
                                }
                        )
        );
        Table withWaterTimeTable = streamTableEnvironment.fromDataStream(singleOutputStreamOperator
                , $("user"), $("url"), $("timestamp"),
                //设置水位线 字段名称为water_time
                $("water_time").rowtime());
        streamTableEnvironment.createTemporaryView("user_data_table", withWaterTimeTable);
        /*
        DataStream转换为 table
         */


        /*
            Group Window Aggregation（1.13 之前只有此类方案，此方案在 1.13 及之后版本已经标记为废弃)

            group by 子句中，即 tumble(row_time, interval '1' minute)，第一个参数为事件时间的时间戳；第二个参数为滚动窗口大小。
         */
        String windowSql =
                "select `user`," +
                        //数量
                        "count(*)," +
                        //水位线结束时间 TUMBLE_END  CAST为flink1.3之前处理时区
                        "CAST(TUMBLE_END(water_time, INTERVAL '10' SECOND) as TIMESTAMP) as end_time" +
                        " from user_data_table " +
                        " group by `user`, tumble(water_time,interval '10' second)";

        streamTableEnvironment.toChangelogStream(streamTableEnvironment.sqlQuery(windowSql))
                .print("count not 1.3");

        /*
            flink13版本处理方式 新的 推荐使用 滚动窗口
        */
        String windowSql2 =
                "select `user`," +
                        //数量
                        "count(*)," +
                        //水位线结束时间 TUMBLE_END  CAST为flink1.3之前处理时区
                        "CAST( window_start as TIMESTAMP)," +
                        "CAST( window_end as TIMESTAMP)" +
                        " from TABLE( TUMBLE(TABLE user_data_table, DESCRIPTOR(water_time), INTERVAL '10' SECOND) ) " +
                        " group by `user`,window_start,window_end";

        streamTableEnvironment.toChangelogStream(streamTableEnvironment.sqlQuery(windowSql2))
                .print("count 1.3");


        env.execute();

    }
}
