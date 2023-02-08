package com.xue.flink.demo.sql;

import com.xue.flink.demo.mySource.ClickSource;
import com.xue.flink.demo.mySource.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: xuexiong@souche.com
 * @date: 2023/2/8 14:49
 * @description: sql入门例子
 */
public class MySqlDemo {
    public static void main(String[] args) {
        /*
        环境准备和数据源定义
         */
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //数据源
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        stream.print();


    }
}
