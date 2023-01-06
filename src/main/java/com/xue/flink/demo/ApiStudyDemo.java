package com.xue.flink.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author: xuexiong@souche.com
 * @date: 2023/1/4 14:59
 * @description: 熟悉一下 api
 * <p>
 * <p>
 * Tuple1 Tuple2 Tuple3.....  类似复杂数据接口 1就是一个元素  2就是两个元素  3就是三个元素
 */
public class ApiStudyDemo {
    public static void main(String[] args) throws Exception {
        // flink 根据上下文环境 决定环境是本地模拟 还是 在真实的 flink集群中
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();


        /*
        source 数据输入
         */

        /*基于本地集合的source（Collection-based-source） 基本自己测试使用*/
        DataStreamSource<String> fromElementsSource
                = executionEnvironment.fromElements("张三,18,北京", "李四,23,天津");
        DataStreamSource<String> fromCollectionSource
                = executionEnvironment.fromCollection(Arrays.asList("张三撒", "李四", "张武爱仕达", "薛仁贵"));


        /*基于文件的source （File-based-source） 基本自己测试使用*/
//        DataStreamSource<String> readTextFileSource = executionEnvironment.readTextFile("文件地址");


        /*基于socket的source （Socket-based-source）*/
//        DataStreamSource<String> executionEnvironmentSource
//                = executionEnvironment.socketTextStream("主机名", 7777);


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
                Random randomNumber = new Random(randomList.size() - 1);
                while (isRunning) {
                    sourceContext.collect(randomList.get(randomNumber.nextInt()));
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        };


        DataStreamSource<String> customSource
                = executionEnvironment.addSource(mySourceFunction);


        /*常用内置source*/
        //kafka
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "kafka地址:端口");
//        properties.setProperty("group.id", "消费组ID");
//        executionEnvironment.addSource(new FlinkKafkaConsumer<>(
//                Arrays.asList("topic1", "topic2")
//                //序列化方法
//                , new SimpleStringSchema()
//                , properties
//        ));
        //mysql redis elasticsearch等略





        /*
        tracsformation 数据转换
         */
        /*map 将DataSet中的每一个元素转换为另一个元素*/
        SingleOutputStreamOperator<String> mapSingleOutputStreamOperator
                = fromElementsSource.map(item -> item + "-map")
                //泛型 flink无法保证识别出数据 所以需要执行数据类型
                .returns(Types.STRING);
        mapSingleOutputStreamOperator.print();


        /*filter*/
        SingleOutputStreamOperator<String> filterSingleOutputStreamOperator
                = fromElementsSource.filter(item -> !item.equals("张三")).returns(Types.STRING);
        filterSingleOutputStreamOperator.print();

        /*flatMap 将 DataSet 中的每一个元素转换为 0…n 个元素*/
        SingleOutputStreamOperator<String> flatMapSingleOutputStreamOperator =
                fromElementsSource.flatMap(
                        (FlatMapFunction<String, String>) (value, out) -> {
                            String[] fields = value.split(",");
                            for (String field : fields)
                                out.collect(field);
                        }
                ).returns(Types.STRING);
        flatMapSingleOutputStreamOperator.print();

        /*keyBy 绝大多数情况，我们要根据事件的某种属性或数据的某个字段进行分组，对一个分组内的数据进行处理*/
        KeyedStream<Tuple2<String, Long>, String> keyByKeyedStream
                = fromCollectionSource
                //转换一下 方便后面统计
                .map((MapFunction<String, Tuple2<String, Long>>) value -> Tuple2.of(value, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                //按照姓 keyby 一下
                .keyBy((KeySelector<Tuple2<String, Long>, String>) value -> value.f0.substring(0, 1));


        keyByKeyedStream.print();

//        /*sum min max 聚合算子可以在在keyedStream 流上进行滚动的聚合（即累计的操作），而且同一个 keyedStream 流上只能调用一次 聚合算子*/
//        SingleOutputStreamOperator<Tuple2<String, Long>> sum
//                = keyByKeyedStream.sum(1);
//        sum.print();


        /* reduce 实现复杂的聚合逻辑 */
        /*
        sink 数据输出
         */
        SingleOutputStreamOperator<Tuple2<String, Long>> reduceSingleOutputStreamOperator = keyByKeyedStream.reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                System.out.println("reduce"+value1.f0);
                System.out.println("reduce"+value2.f0);
                if (value2.f0.length() > 0) {
                    return Tuple2.of(value1.f0.substring(0,1), value1.f1 + value2.f1);
                }
                return null;
            }
        });
        reduceSingleOutputStreamOperator.print();
        reduceSingleOutputStreamOperator.print();


        executionEnvironment.execute();

    }

    /**
     * 自定义获取key
     * lamda表达式 因为java的泛型擦除机制会导致flink识别不出返回值的具体类型 无法使用改写法
     * (KeySelector<String, Tuple2<String, Integer>>) value -> new Tuple2<>(value.substring(0, 1), 1);
     *
     * @author xuexiong@souche.com
     * @return
     * @exception
     * @date 2023/1/5 11:11
     */
    private static class MyKeySelector implements KeySelector<String, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> getKey(String value) throws Exception {
            return new Tuple2<>(value.substring(0, 1), 1);
        }
    }
}
