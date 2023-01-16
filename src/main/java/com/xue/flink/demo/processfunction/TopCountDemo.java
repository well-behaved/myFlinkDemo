package com.xue.flink.demo.processfunction;

import com.xue.flink.demo.mySource.DateEventDTO;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Random;

/**
 * @author: xuexiong@souche.com
 * @date: 2023/1/16 18:10
 * @description: 使用 processFunction 实现统计每小时 访问量前3名的逻辑
 */
public class TopCountDemo {
    public static void main(String[] args) throws Exception {
        /*
        get the execution environment 准备环境
         */
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //自定义数据源
        DataStreamSource<DateEventDTO> dataStreamSource = env.addSource(new SourceFunction<DateEventDTO>() {
            private boolean isRunning = true;

            @Override
            public void run(SourceContext<DateEventDTO> sourceContext) throws Exception {
                //姓
                List<String> randomXing = Arrays.asList("张", "李", "王", "赵");
                //名
                List<String> randomMing = Arrays.asList("一", "二", "三", "四");

                while (isRunning) {
                    sourceContext.collect(
                            new DateEventDTO(
                                    //随机生成姓名
                                    randomXing.get(new Random().nextInt(randomXing.size() - 1))
                                            + randomMing.get(new Random().nextInt(randomMing.size() - 1)),
                                    "百度",
                                    new Date()
                            )
                    );
                    Thread.sleep(10);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });


                /*
        flink内置的水位线 生成器 无序
        乱序需要设置延时时间 确保超过了延时时间的数据能够正常到达
         */
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
        统计 top 3
         */
        SingleOutputStreamOperator<String> process = singleOutputStreamOperatoBoundedOut
                //开全窗 10秒
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(10)))
                .process(new ProcessAllWindowFunction<DateEventDTO, String, TimeWindow>() {
                             @Override
                             public void process(Context context, Iterable<DateEventDTO> elements, Collector<String> out) throws Exception {
                                 // 用户数量统计 map
                                 HashMap<String, Long> userCountMap = new HashMap<>();
                                 //添加数据到map中
                                 for (DateEventDTO dateEventDTO : elements) {
                                     if (userCountMap.containsKey(dateEventDTO.getUser())) {
                                         userCountMap.put(dateEventDTO.getUser(),
                                                 userCountMap.get(dateEventDTO.getUser()) + 1);
                                     } else {
                                         userCountMap.put(dateEventDTO.getUser(), 1L);
                                     }
                                 }

                                 //返回top3
                                 List<Tuple2<String, Long>> mapList = new ArrayList<Tuple2<String, Long>>();
                                 for (String key : userCountMap.keySet()) {
                                     mapList.add(Tuple2.of(key, userCountMap.get(key)));
                                 }
                                 mapList.sort((o1, o2) -> o2.f1.intValue() - o1.f1.intValue());
                                 // 取排序后的前两名，构建输出结果
                                 StringBuilder result = new StringBuilder();
                                 result.append("========================================\n");
                                 for (int i = 0; i < 2; i++) {
                                     Tuple2<String, Long> temp = mapList.get(i);
                                     String info = "浏览量No." + (i + 1) +
                                             " 用户：" + temp.f0 +
                                             " 浏览量：" + temp.f1 +
                                             " 窗口结束时间：" + new Timestamp(context.window().getEnd()) + "\n";

                                     result.append(info);
                                 }
                                 result.append("========================================\n");
                                 out.collect(result.toString());


                             }
                         }
                );

        //开始运行
        process.print();

        env.execute();


    }
}
