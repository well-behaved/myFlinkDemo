package com.xue.flink.demo.mySource;

import com.xue.flink.demo.utils.DateUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Date;

/**
 * @author: xuexiong@souche.com
 * @date: 2023/1/13 14:23
 * @description: 用户点击统计
 */
@Getter
@Setter
public class DateUserCountDTO {
    /**
     * 用户姓名
     */
    public String user;
    /**
     * 用户姓名
     */
    public int count;
    /**
     * @return
     */
    public Date timeStamp;

    @Override
    public String toString() {
        return "DateUserCountDTO{" +
                "user='" + user + '\'' +
                ", count=" + count +
                ", timeStamp=" + DateUtils.dateToStr(timeStamp) +
                '}';
    }
}
