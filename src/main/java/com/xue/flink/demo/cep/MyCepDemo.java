package com.xue.flink.demo.cep;

import com.xue.flink.demo.ClickSource;
import com.xue.flink.demo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Optional;

/**
 * @author: xuexiong@souche.com
 * @date: 2023/2/13 16:27
 * @description: flink cep入门demo
 */
public class MyCepDemo {
    public static void main(String[] args) throws Exception {
        /*
        定义环境
         */
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /*
        添加数据源
         */
        DataStreamSource<Event> eventDataStreamSource = env.addSource(new ClickSource());
        eventDataStreamSource.print();
        /*
        水位线设置
         */
        SingleOutputStreamOperator<Event> singleOutputStreamOperator = eventDataStreamSource.assignTimestampsAndWatermarks(
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
        /*
        分组
         */
        KeyedStream<Event, String> eventStringKeyedStream = singleOutputStreamOperator.keyBy(date -> date.user);

        /*
        定义patter规则  这里定例子为 用户连续点击两次百度页面
         */
        Pattern<Event, Event> continueClick = Pattern.<Event>begin("continueClick")
                //连续访问./cart
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.url.equals("百度");
                    }
                })
                //两次
                .times(2)
                //严格紧邻
                .consecutive();;
        /*
        应用到流上
         */
        //应用
        PatternStream<Event> eventPatternStream = CEP.pattern(eventStringKeyedStream, continueClick);

        //输出
        eventPatternStream.select((PatternSelectFunction<Event, String>) pattern -> {
            if (pattern == null || pattern.isEmpty()) {
                return null;
            }
            Event continueClickEvent = pattern.get("continueClick").get(0);
            return "连续点击:" + continueClickEvent.toString();
        }).print("发现问题");


        env.execute();

    }
}
