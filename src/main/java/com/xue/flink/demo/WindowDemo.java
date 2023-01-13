package com.xue.flink.demo;

import com.xue.flink.demo.mySource.DateEventDTO;
import com.xue.flink.demo.mySource.DateUserCountDTO;
import com.xue.flink.demo.utils.DateUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;
import java.util.Optional;

/**
 * @author: xuexiong@souche.com
 * @date: 2023/1/13 10:31
 * @description: 窗口例子
 */
public class WindowDemo {
    public static void main(String[] args) throws Exception {
        //        数据源整备
        // flink 根据上下文环境 决定环境是本地模拟 还是 在真实的 flink集群中
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<DateEventDTO> dataStreamSource = executionEnvironment.fromElements(
                new DateEventDTO("薛雄", "百度", DateUtils.strToDate("2023-01-12 17:17:14", DateUtils.DATETIME_FORMAT))
                , new DateEventDTO("薛雄2", "天猫", DateUtils.strToDate("2023-01-12 17:17:24", DateUtils.DATETIME_FORMAT))
                , new DateEventDTO("薛雄3", "天猫", DateUtils.strToDate("2023-01-12 17:17:34", DateUtils.DATETIME_FORMAT))
                , new DateEventDTO("薛雄", "天猫", DateUtils.strToDate("2023-01-12 17:17:44", DateUtils.DATETIME_FORMAT))
                , new DateEventDTO("薛雄3", "天猫", DateUtils.strToDate("2023-01-12 17:17:54", DateUtils.DATETIME_FORMAT))
                , new DateEventDTO("薛雄", "天猫", DateUtils.strToDate("2023-01-12 17:17:54", DateUtils.DATETIME_FORMAT))
                , new DateEventDTO("薛雄2", "天猫", DateUtils.strToDate("2023-01-12 17:17:54", DateUtils.DATETIME_FORMAT))
                , new DateEventDTO("薛雄", "天猫", DateUtils.strToDate("2023-01-12 17:18:54", DateUtils.DATETIME_FORMAT))
                , new DateEventDTO("薛雄3", "天猫", DateUtils.strToDate("2023-01-12 17:19:54", DateUtils.DATETIME_FORMAT))
                , new DateEventDTO("薛雄3", "天猫", DateUtils.strToDate("2023-01-12 17:12:54", DateUtils.DATETIME_FORMAT))
                , new DateEventDTO("薛雄", "天猫", DateUtils.strToDate("2023-01-12 17:11:54", DateUtils.DATETIME_FORMAT))
                , new DateEventDTO("薛雄", "天猫", DateUtils.strToDate("2023-01-12 17:11:54", DateUtils.DATETIME_FORMAT))
                , new DateEventDTO("薛雄", "天猫", DateUtils.strToDate("2023-01-12 17:11:54", DateUtils.DATETIME_FORMAT))
                , new DateEventDTO("薛雄", "天猫", DateUtils.strToDate("2023-01-12 17:11:54", DateUtils.DATETIME_FORMAT))
        );
        /*
        时间窗口-滚动 事件时间 窗口设置
        处理时间类似略
         */
        dataStreamSource
                // 按照姓名 分区
                .keyBy(new KeySelector<DateEventDTO, String>() {
                    @Override
                    public String getKey(DateEventDTO value) throws Exception {
                        return Optional.ofNullable(value).map(DateEventDTO::getUser).orElse("无名氏");
                    }
                })
                //设置窗口 TumblingProcessingTimeWindows为滚动窗口  这里为设置一个60秒的滚动窗口
                //可以传入两个 Time 类型的参数：size 和 offset。 size为窗口时间 offset为偏移量一般为处理时区
                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
        ;
        /*
        时间窗口-滑动 事件时间 窗口设置
         处理时间类似略
         */
        dataStreamSource
                // 按照姓名 分区
                .keyBy(new KeySelector<DateEventDTO, String>() {
                    @Override
                    public String getKey(DateEventDTO value) throws Exception {
                        return Optional.ofNullable(value).map(DateEventDTO::getUser).orElse("无名氏");
                    }
                })
                //设置窗口 SlidingProcessingTimeWindows为滑动窗口
                //size 和 slide，前者表示滑动窗口的大小， 后者表示滑动窗口的滑动步长。
                .window(SlidingEventTimeWindows.of(Time.days(1), Time.hours(2)))
        ;
        /*
        时间窗口-会话窗口设置
         处理时间类似略
         */
        dataStreamSource
                // 按照姓名 分区
                .keyBy(new KeySelector<DateEventDTO, String>() {
                    @Override
                    public String getKey(DateEventDTO value) throws Exception {
                        return Optional.ofNullable(value).map(DateEventDTO::getUser).orElse("无名氏");
                    }
                })
                //设置窗口 ProcessingTimeSessionWindows为会话窗口
                //.withGap()方法需要传入一个 Time 类型的参数 size，表示会话的超时时间，也就是最小间隔 session gap。
                .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
        ;
        /*
        计数窗口-滚动计数
         */
        dataStreamSource
                // 按照姓名 分区
                .keyBy(new KeySelector<DateEventDTO, String>() {
                    @Override
                    public String getKey(DateEventDTO value) throws Exception {
                        return Optional.ofNullable(value).map(DateEventDTO::getUser).orElse("无名氏");
                    }
                })
                //设置数量
                .countWindow(10)
        ;
         /*
        计数窗口-滑动计数
         */
        dataStreamSource
                // 按照姓名 分区
                .keyBy(new KeySelector<DateEventDTO, String>() {
                    @Override
                    public String getKey(DateEventDTO value) throws Exception {
                        return Optional.ofNullable(value).map(DateEventDTO::getUser).orElse("无名氏");
                    }
                })
                //设置数量
                .countWindow(10, 2)
        ;
        /*
        全局窗口
         */
        dataStreamSource
                // 按照姓名 分区
                .keyBy(new KeySelector<DateEventDTO, String>() {
                    @Override
                    public String getKey(DateEventDTO value) throws Exception {
                        return Optional.ofNullable(value).map(DateEventDTO::getUser).orElse("无名氏");
                    }
                })
                //设置 全局窗口
                .window(GlobalWindows.create())
        ;
        /*+++++++++++++++++++++++++++分割线++++++++++++++++++++++++++++*/
        SingleOutputStreamOperator<DateEventDTO> singleOutputStreamOperatoBoundedOut = dataStreamSource.assignTimestampsAndWatermarks(
                //乱序 水位线生成
                WatermarkStrategy
                        // //指定乱序生成器 设置两秒延迟
                        .<DateEventDTO>forBoundedOutOfOrderness(Duration.ofMillis(2000))
                        //设置获取时间戳逻辑
                        .withTimestampAssigner((SerializableTimestampAssigner<DateEventDTO>) (element, recordTimestamp) -> {
                                    //自定义取出 数据中的时间 这里取得是数据中的happenDate 字段
                                    return Optional.ofNullable(element).map(DateEventDTO::getHappenDate).map(Date::getTime).orElse(0L);
                                }
                        )
        );
        /*
        时间窗口-滚动 事件时间 窗口设置
        处理时间类似略
         */
        SingleOutputStreamOperator<DateEventDTO> reduceSingleOutputStreamOperator = singleOutputStreamOperatoBoundedOut
                // 按照姓名 分区
                .keyBy(new KeySelector<DateEventDTO, String>() {
                    @Override
                    public String getKey(DateEventDTO value) throws Exception {
                        return Optional.ofNullable(value).map(DateEventDTO::getUser).orElse("无名氏");
                    }
                })
                //设置窗口 TumblingProcessingTimeWindows为滚动窗口  这里为设置一个60秒的滚动窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .reduce(new ReduceFunction<DateEventDTO>() {
                    @Override
                    public DateEventDTO reduce(DateEventDTO value1, DateEventDTO value2) throws Exception {
                        System.out.println("-----");
                        //获取用户最新访问的时间
                        return value1.getHappenDate().getTime() >= value2.getHappenDate().getTime() ? value1 : value2;
                    }
                });
//        reduceSingleOutputStreamOperator.print();



                /*
        聚合函数设置
         */
        SingleOutputStreamOperator<DateUserCountDTO> aggregateSingleOutputStreamOperator = singleOutputStreamOperatoBoundedOut
                // 按照姓名 分区
                .keyBy(new KeySelector<DateEventDTO, String>() {
                    @Override
                    public String getKey(DateEventDTO value) throws Exception {
                        return Optional.ofNullable(value).map(DateEventDTO::getUser).orElse("无名氏");
                    }
                })
                //设置窗口 TumblingProcessingTimeWindows为滚动窗口  这里为设置一个60秒的滚动窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .aggregate(new AggregateFunction<DateEventDTO, DateUserCountDTO, DateUserCountDTO>() {
                    /**
                     * 返回参数初始化
                     * 创建一个累加器，这就是为聚合创建了一个初始状态，每个聚合任务只会调用一次。
                     */
                    @Override
                    public DateUserCountDTO createAccumulator() {
                        return new DateUserCountDTO();
                    }

                    /**
                     *将输入的元素添加到累加器中。 规约方法
                     */
                    @Override
                    public DateUserCountDTO add(DateEventDTO value, DateUserCountDTO accumulator) {
                        accumulator.setUser(value.getUser());
                        accumulator.setCount(accumulator.getCount() + 1);
                        return accumulator;
                    }

                    /**
                     *最终结果转化处理 从累加器中提取聚合的输出结果。
                     */
                    @Override
                    public DateUserCountDTO getResult(DateUserCountDTO accumulator) {
                        return accumulator;
                    }

                    /**
                     *合并窗口时使用 合并两个累加器，并将合并后的状态作为一个累加器返回。这个方法只在需要合并窗口的场景下才会被调用；
                     * 最常见的合并窗口（Merging Window）的场景就是会话窗口（Session Windows）。
                     */
                    @Override
                    public DateUserCountDTO merge(DateUserCountDTO a, DateUserCountDTO b) {
                        DateUserCountDTO finalResult = new DateUserCountDTO();
                        finalResult.setUser(a.getUser());
                        finalResult.setCount(a.getCount() + b.getCount());
                        return finalResult;
                    }
                });

//        aggregateSingleOutputStreamOperator.print();

        /*
            全窗口函数例子 全窗口 也就是数据全部到达后才开始进行计算处理
         */
        SingleOutputStreamOperator<DateUserCountDTO> processSingleOutputStreamOperator = singleOutputStreamOperatoBoundedOut
                // 按照姓名 分区
                .keyBy(new KeySelector<DateEventDTO, String>() {
                    @Override
                    public String getKey(DateEventDTO value) throws Exception {
                        return Optional.ofNullable(value).map(DateEventDTO::getUser).orElse("无名氏");
                    }
                })
                //设置窗口 TumblingProcessingTimeWindows为滚动窗口  这里为设置一个60秒的滚动窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                //设置全窗口处理逻辑 统计用户单位时间的访问次数
                .process(new ProcessWindowFunction<DateEventDTO, DateUserCountDTO, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<DateEventDTO> elements
                            , Collector<DateUserCountDTO> out) throws Exception {
                        DateUserCountDTO dateUserCountDTO = new DateUserCountDTO();
                        dateUserCountDTO.setUser(key);
                        //获取总共的数量
                        int i = 0;
                        for (DateEventDTO ignored : elements) {
                            i++;
                        }
                        dateUserCountDTO.setCount(i);

                        out.collect(dateUserCountDTO);
                    }
                });



                /*
        聚合函数设置
         */
        SingleOutputStreamOperator<DateUserCountDTO> aggregateAndProcessSingleOutputStreamOperator = dataStreamSource
                //设置水位线
                .assignTimestampsAndWatermarks(
                        //乱序 水位线生成
                        WatermarkStrategy
                                // //指定乱序生成器 设置两秒延迟
                                .<DateEventDTO>forBoundedOutOfOrderness(Duration.ofMillis(2000))
                                //设置获取时间戳逻辑
                                .withTimestampAssigner((SerializableTimestampAssigner<DateEventDTO>) (element, recordTimestamp) -> {
                                            //自定义取出 数据中的时间 这里取得是数据中的happenDate 字段
                                            return Optional.ofNullable(element).map(DateEventDTO::getHappenDate).map(Date::getTime).orElse(0L);
                                        }
                                )
                )
                // 按照姓名 分区
                .keyBy(new KeySelector<DateEventDTO, String>() {
                    @Override
                    public String getKey(DateEventDTO value) throws Exception {
                        return Optional.ofNullable(value).map(DateEventDTO::getUser).orElse("无名氏");
                    }
                })
                //设置窗口 TumblingProcessingTimeWindows为滚动窗口  这里为设置一个60秒的滚动窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                .aggregate(
                        /*
                        增量组合函数 计算逻辑
                         */
                        new AggregateFunction<DateEventDTO, DateUserCountDTO, DateUserCountDTO>() {
                            /**
                             * 返回参数初始化
                             * 创建一个累加器，这就是为聚合创建了一个初始状态，每个聚合任务只会调用一次。
                             */
                            @Override
                            public DateUserCountDTO createAccumulator() {
                                return new DateUserCountDTO();
                            }

                            /**
                             * 将输入的元素添加到累加器中。 规约方法
                             */
                            @Override
                            public DateUserCountDTO add(DateEventDTO value, DateUserCountDTO accumulator) {
                                accumulator.setUser(value.getUser());
                                accumulator.setCount(accumulator.getCount() + 1);
                                return accumulator;
                            }

                            /**
                             * 最终结果转化处理 从累加器中提取聚合的输出结果。
                             */
                            @Override
                            public DateUserCountDTO getResult(DateUserCountDTO accumulator) {
                                return accumulator;
                            }

                            /**
                             * 合并窗口时使用 合并两个累加器，并将合并后的状态作为一个累加器返回。这个方法只在需要合并窗口的场景下才会被调用；
                             * 最常见的合并窗口（Merging Window）的场景就是会话窗口（Session Windows）。
                             */
                            @Override
                            public DateUserCountDTO merge(DateUserCountDTO a, DateUserCountDTO b) {
                                DateUserCountDTO finalResult = new DateUserCountDTO();
                                finalResult.setUser(a.getUser());
                                finalResult.setCount(a.getCount() + b.getCount());
                                return finalResult;
                            }
                        },
                        new ProcessWindowFunction<DateUserCountDTO, DateUserCountDTO, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context context,
                                                Iterable<DateUserCountDTO> elements, Collector<DateUserCountDTO> out) throws Exception {
                                while (elements.iterator().hasNext()) {
                                    DateUserCountDTO dateUserCount = elements.iterator().next();
                                    dateUserCount.setTimeStamp(new Date(context.currentWatermark()));
                                    out.collect(dateUserCount);
                                }
                            }
                        }

                );
        aggregateAndProcessSingleOutputStreamOperator.print();


        executionEnvironment.execute();


    }
}
