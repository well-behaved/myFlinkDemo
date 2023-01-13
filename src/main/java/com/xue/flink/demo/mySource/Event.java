package com.xue.flink.demo.mySource;

import java.sql.Date;

/**
 * @author: xuexiong@souche.com
 * @date: 2023/1/12 17:15
 * @description:
 */
public class Event {
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
    public Long timestamp;

    public Event() {
    }

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Date(timestamp) +
                '}';
    }
}
