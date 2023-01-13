package com.xue.flink.demo;

import com.xue.flink.demo.mySource.DateEventDTO;
import com.xue.flink.demo.utils.DateUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Date;
import java.util.Optional;

/**
 * @author: xuexiong@souche.com
 * @date: 2023/1/12 17:02
 * @description: 水位线 例子
 */
public class TimeWaterDemo {
    public static void main(String[] args) {
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
        );
        /*
        设置 自定义 水位线waterTime 生成器
         */
        SingleOutputStreamOperator<DateEventDTO> customWaterMark =
                dataStreamSource.assignTimestampsAndWatermarks(new WatermarkStrategy<DateEventDTO>() {
                    /**
                     * 主要负责从流中数据元素的某个字段中提取时间戳，并分配给元素。时间戳的分配是生成水位线的基础。
                     */
                    @Override
                    public TimestampAssigner<DateEventDTO> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                        //设置 水位线以 传入记录的 happenDate 字段记录的时间为 时间
                        return (element, recordTimestamp) -> Optional.ofNullable(element).map(DateEventDTO::getHappenDate).map(Date::getTime).orElse(0L);
                    }

                    /**
                     * 主要负责按照既定的方式， 基于时间戳生成水位线。在WatermarkGenerator 接口中，主要又有两个方法：onEvent()和 onPeriodicEmit()。
                     */
                    @Override
                    public WatermarkGenerator<DateEventDTO> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new WatermarkGenerator<DateEventDTO>() {
                            /**
                             * 每个事件（数据）到来都会调用的方法，它的参数有当前事件、时间戳， 以及允许发出水位线的一个 WatermarkOutput，可以基于事件做各种操作
                             */
                            @Override
                            public void onEvent(DateEventDTO event, long eventTimestamp, WatermarkOutput output) {

                            }

                            /**
                             * 周期性调用的方法，可以由 WatermarkOutput 发出水位线。周期时间为处理时间，可以调用环境配置的.setAutoWatermarkInterval()方法来设置，默认为200ms。
                             */
                            @Override
                            public void onPeriodicEmit(WatermarkOutput output) {

                            }
                        };
                    }
                });
        /*
        flink内置的水位线 生成器 有序
         */
        SingleOutputStreamOperator<DateEventDTO> singleOutputStreamOperatorMonotonous = dataStreamSource.assignTimestampsAndWatermarks(
                //有序 水位线生成
                WatermarkStrategy
                        //指定有序生成器
                        .<DateEventDTO>forMonotonousTimestamps()
                        //设置获取时间戳逻辑
                        .withTimestampAssigner(new SerializableTimestampAssigner<DateEventDTO>() {
                            @Override
                            public long extractTimestamp(DateEventDTO element, long recordTimestamp) {
                                //自定义取出 数据中的时间 这里取得是数据中的happenDate 字段
                                return Optional.ofNullable(element).map(DateEventDTO::getHappenDate).map(Date::getTime).orElse(0L);
                            }
                        })
        );
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


    }
}
