package com.xue.flink.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author: xuexiong@souche.com
 * @date: 2023/1/5 16:52
 * @description:
 */
public class MyReduceDemo {
    public static void main(String[] args) throws Exception {
        // flink 根据上下文环境 决定环境是本地模拟 还是 在真实的 flink集群中
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(3);
        SourceFunction<String> mySourceFunction = new SourceFunction<String>() {
            private boolean isRunning = true;

            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                List<String> randomList = Arrays.asList("张三", "李四", "王五", "赵六");
                Random randomNumber = new Random();
                while (isRunning) {
                    String inputName = randomList.get(randomNumber.nextInt(30) % 4);
                    sourceContext.collect(inputName);
                    System.out.println("输入:" + inputName);
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        };


        SingleOutputStreamOperator<Tuple2<String, Long>> reduceDemo
                = executionEnvironment.addSource(mySourceFunction)
                //转换一下 方便后面统计
                .map((MapFunction<String, Tuple2<String, Long>>) value -> Tuple2.of(value, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                //按照姓 keyby 一下
                .keyBy((KeySelector<Tuple2<String, Long>, String>) value -> value.f0.substring(0, 1))
                //reduce  实现 sum
                .reduce(
                        new ReduceFunction<Tuple2<String, Long>>() {
                            @Override
                            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2)
                                    throws Exception {
                                return Tuple2.of(value1.f0.substring(0, 1), value1.f1 + value2.f1);
                            }
                        }
                );
        reduceDemo.print();
        executionEnvironment.execute();


    }
}
