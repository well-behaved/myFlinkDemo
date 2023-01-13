package com.xue.flink.demo.mySource;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial
 * <p>
 * Created by  wushengran
 */

import com.xue.flink.demo.utils.DateUtils;
import lombok.Getter;
import lombok.Setter;

import java.util.Date;

/**
 * 数据源 类
 */
@Getter
@Setter
public class DateEventDTO {
    /**
     * 用户姓名
     */
    public String user;
    /**
     * 访问的地址
     */
    public String url;
    /**
     * 访问的 时间戳
     */
    public Date happenDate;

    public DateEventDTO() {
    }

    public DateEventDTO(String user, String url, Date timestamp) {
        this.user = user;
        this.url = url;
        this.happenDate = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + DateUtils.dateToStr(happenDate) +
                '}';
    }
}
