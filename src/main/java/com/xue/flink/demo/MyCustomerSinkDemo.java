package com.xue.flink.demo;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author: xuexiong@souche.com
 * @date: 2023/1/10 14:28
 * @description: 自定义输出测试
 */
public class MyCustomerSinkDemo {
    public static void main(String[] args) throws Exception {
        // flink 根据上下文环境 决定环境是本地模拟 还是 在真实的 flink集群中
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(6);


        /*自定义的source (Custom-source)*/
        //SourceFunction 对所有StreamSource的顶层接口，直接继承该接口的Source无法将并行度设置大于1
        //RichSourceFunction  implements SourceFunction，同时 extends AbstractRichFunction。支撑Source 全生命周期控制，如 open()，close()。
        //ParallelSourceFunction：ParallelSourceFunction继承了SourceFunction接口，它并没有定义其他额外的方法，仅仅是用接口名来表达意图，即可以被并行执行的stream data source（继承该接口的实例能够将并行度设置大于1）。
        //RichParallelSourceFunction：实现了ParallelSourceFunction接口，同时继承了AbstractRichFunction，所以可以支撑自定义Open,Close 函数。
        SourceFunction<String> mySourceFunction = new SourceFunction<String>() {
            private boolean isRunning = true;

            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                List<String> randomList = Arrays.asList("张三", "李四", "王五", "赵六");
                while (isRunning) {
                    sourceContext.collect(randomList.get(new Random().nextInt(randomList.size()-1)));
                    Thread.sleep(2000);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        };
        //自定义输出
        executionEnvironment.addSource(mySourceFunction).addSink(new RichSinkFunction<String>() {
            /*
            建立连接
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("建立连接");
            }

            /*
            执行输出方法
             */
            @Override
            public void invoke(String value, Context context) {
                System.out.println("value:"+value+"---"+context.currentProcessingTime());

            }

            /*
            关闭连接
             */
            @Override
            public void close() throws Exception {
                super.close();
            }
        }).setParallelism(4);


        executionEnvironment.execute();
    }

}
