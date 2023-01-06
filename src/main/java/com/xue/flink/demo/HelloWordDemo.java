package com.xue.flink.demo;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author: xuexiong@souche.com
 * @date: 2022/12/21 16:51
 * @description: flink demo
 * 参考自官方文档:https://nightlies.apache.org/flink/flink-docs-release-1.8/tutorials/local_setup.html
 */
public class HelloWordDemo {
    public static void main(String[] args) throws Exception {
        // the port to connect to
        final int port;
        try {
//            final ParameterTool params = ParameterTool.fromArgs(args);
//            port = params.getInt("port");
            port = 9000;
        } catch (Exception e) {
            System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'");
            return;
        }

        // get the execution environment 准备环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //指定模型为自动 既支持流
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // get input data by connecting to the socket 准备输入源
        DataStream<String> text = env.socketTextStream("127.0.0.1", port, "\n");

        // parse the data, group it, window it, and aggregate the counts 处理数据
        DataStream<WordWithCount> windowCounts = text
                .flatMap(new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String value, Collector<WordWithCount> out) {
                        //value为每一行数据
                        for (String word : value.split("\\s")) {
                            //按照空格拆分 放入集合中
                            out.collect(new WordWithCount(word, 1L));
                        }
                    }
                })
                .keyBy("word")
                .timeWindow(Time.seconds(5), Time.seconds(1))
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) {
                        return new WordWithCount(a.word, a.count + b.count);
                    }
                });

        // print the results with a single thread, rather than in parallel
        windowCounts.print().setParallelism(1);
//        env.setRestartStrategy(RestartStrategies.noRestart());

        env.execute("Socket Window WordCount");

    }

    // Data type for words with count 单词统计 静态类
    public static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}
