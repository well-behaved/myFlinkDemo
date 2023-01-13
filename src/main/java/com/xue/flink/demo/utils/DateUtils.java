package com.xue.flink.demo.utils;

import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.util.*;

/**
 * 时间工具类
 */
public class DateUtils {
    public static void main(String[] args) {
        System.out.println(DateUtils.strToDate("2023-01-12 17:17:14", DateUtils.DATETIME_FORMAT));

        System.out.println(DateUtils.dateToStr(new Date()));
    }


    public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String DATE_SIMPLE_FORMAT = "yyyyMMdd";

    //====================================时间转换=======================================>>

    /**
     * 时长转换
     *
     * @param time 通话时长（秒）
     * @return 几小时几分钟几秒
     */
    public static String getStrTime(Integer time) {
        if (time == null || time == 0) return "";
        StringBuilder str = new StringBuilder();
        int hour = time / 3600;
        if (hour != 0) str.append(hour).append("小时");
        int minutes = (time - hour * 3600) / 60;
        if (minutes != 0) str.append(minutes).append("分钟");
        int seconds = time - hour * 3600 - minutes * 60;
        if (seconds != 0) str.append(seconds).append("秒");
        return str.toString();
    }

    public static String millisToStr(long time) {
        if (time <= 0) return "0 秒";
        if (time < 1000) {
            return (time / 1000.0f) + "秒";
        }
        return getStrTime((int) time / 1000);
    }

    public static String millisDiff(long time) {
        return millisToStr(System.currentTimeMillis() - time);
    }

    /**
     * 日期转换为字符串 默认"yyyy-MM-dd HH:mm:ss"
     *
     * @param date 日期
     */
    public static String dateToStr(Date date) {
        return dateToStr(date, null);
    }

    /**
     * 日期转换为字符串
     *
     * @param date   日期
     * @param format 日期格式
     */
    public static String dateToStr(Date date, String format) {
        if (date == null) return null;
        // 如果没有指定字符串转换的格式，则用默认格式进行转换
        if (null == format || "".equals(format) || "Datetime".equals(format)) {
            format = DATETIME_FORMAT;
        } else if ("Timestamp".equals(format)) {
            format = TIMESTAMP_FORMAT;
        } else if ("Date".equals(format)) {
            format = DATE_FORMAT;
        } else if ("DateSimple".equals(format)) {
            format = DATE_SIMPLE_FORMAT;
        } else if ("Simple".equals(format)) {
            format = DATE_SIMPLE_FORMAT;
        }
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(date);
    }

    /**
     * 任意类型日期字符串转时间
     */
    public static Date strToDate(String time) {
        if (StringUtils.isBlank(time)) return null;
        SimpleDateFormat formatter;
        int tempPos = time.indexOf("AD");
        time = time.trim();
        formatter = new SimpleDateFormat("yyyy.MM.dd G 'at' hh:mm:ss z");
        if (tempPos > -1) {
            time = time.substring(0, tempPos) +
                    "公元" + time.substring(tempPos + "AD".length());//china
            formatter = new SimpleDateFormat("yyyy.MM.dd G 'at' hh:mm:ss z");
        }
        if (time.contains(".")) time = time.replaceAll("\\.", "/");
        if (time.contains("-")) time = time.replaceAll("-", "/");
        if (!time.contains("/") && !time.contains(" ")) {
            formatter = new SimpleDateFormat("yyyyMMddHHmmss");
        } else if (time.contains("/")) {
            if (time.contains("am") || time.contains("pm")) formatter = new SimpleDateFormat("yyyy/MM/dd KK:mm:ss a");
            else if (time.contains(" ")) formatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            else {
                String[] dateArr = time.split("/");
                if (dateArr.length == 2 || (dateArr.length == 3 && StringUtils.isBlank(dateArr[2])))
                    formatter = new SimpleDateFormat("yyyy/MM");
                else formatter = new SimpleDateFormat("yyyy/MM/dd");
            }
        }
        ParsePosition pos = new ParsePosition(0);
        return formatter.parse(time, pos);
    }

    public static Date strToDate(String time, String formatterString) {
        if (StringUtils.isBlank(time) || StringUtils.isBlank(formatterString)) return null;
        SimpleDateFormat formatter = new SimpleDateFormat(formatterString);
        ParsePosition pos = new ParsePosition(0);
        return formatter.parse(time, pos);
    }

    public static Date strToDateThrow(String time) {
        if (StringUtils.isBlank(time)) return null;
        Date value = strToDate(time);
        return value;
    }

    //====================================时间获取=======================================>>

    /**
     * 获取当前对应格式的日期
     */
    public static String getDateStr() {
        return getDateStr(null);
    }

    /**
     * 获取当前对应格式的日期 默认"yyyyMMddHHmmssSSS"
     *
     * @param format 日期格式
     */
    public static String getDateStr(String format) {
        if (null == format || "".equals(format)) {
            format = "yyyyMMddHHmmssSSS";
        }
        Date date = new Date();
        SimpleDateFormat df = new SimpleDateFormat(format);
        return df.format(date);
    }


    /**
     * 获取星期几，java中一周中的数字 转 常规一周中的数字
     * java中 周日=1 周六=7
     * eg:
     * 周一 -> 1
     * 周日 -> 7
     *
     * @param dayOfWeek java中一周中的数字
     */
    public static int getIsoDayOfWeek(int dayOfWeek) {
        int result;
        if (dayOfWeek == 1) {
            result = 7;
        } else {
            result = dayOfWeek - 1;
        }
        return result;
    }


    /**
     * 根据日期获取星期
     */
    public static String getWeekOfDate(Date date) {
        String[] weekDays = {"sun", "mon", "tue", "wed", "thu", "fri", "sat"};
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        int w = cal.get(Calendar.DAY_OF_WEEK) - 1;
        if (w < 0) w = 0;
        return weekDays[w];
    }

    //获取一个月的开始和结束时间
    public static List<Date> getMonthFirstAndEndDay(Date date) {
        List<Date> result = new ArrayList<>();
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);

        c.set(Calendar.DAY_OF_MONTH, 1);//设置为1号
        result.add(c.getTime());

        c.add(Calendar.MONTH, 1);//加一个月
        c.add(Calendar.MILLISECOND, -1);//减一毫秒
        result.add(c.getTime());

        return result;
    }

    //获取一年的开始和结束时间
    public static List<Date> getYearFirstAndEndDay(Date date) {
        List<Date> result = new ArrayList<>();
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.set(Calendar.MONTH, 0);
        c.set(Calendar.DATE, 1);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);

        result.add(c.getTime());

        c.add(Calendar.YEAR, 1);//加一年
        c.add(Calendar.MILLISECOND, -1);//减一毫秒
        result.add(c.getTime());

        return result;
    }

    public static Date getYearStartTime(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.YEAR, 0);
        calendar.add(Calendar.DATE, 0);
        calendar.add(Calendar.MONTH, 0);
        calendar.set(Calendar.DAY_OF_YEAR, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime();
    }

    /**
     * 获取季度
     */
    public static int getQuarter(Date date) {
        int quarter = 0;
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        int month = c.get(Calendar.MONTH);
        switch (month) {
            case Calendar.JANUARY:
            case Calendar.FEBRUARY:
            case Calendar.MARCH:
                quarter = 1;
                break;
            case Calendar.APRIL:
            case Calendar.MAY:
            case Calendar.JUNE:
                quarter = 2;
                break;
            case Calendar.JULY:
            case Calendar.AUGUST:
            case Calendar.SEPTEMBER:
                quarter = 3;
                break;
            case Calendar.OCTOBER:
            case Calendar.NOVEMBER:
            case Calendar.DECEMBER:
                quarter = 4;
                break;
        }
        return quarter;
    }

    /**
     * 获取某月 所有日期（yyyy-mm-dd格式字符串）
     */
    public static List<String> getMonthFullDay(Date date) {
        SimpleDateFormat dateFormatYYYYMMDD = new SimpleDateFormat(DATE_FORMAT);
        List<String> fullDayList = new ArrayList<>();
        // 获得当前日期对象
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        // 当月1号
        c.set(Calendar.DAY_OF_MONTH, 1);
        int count = c.getActualMaximum(Calendar.DAY_OF_MONTH);
        for (int j = 1; j <= count; j++) {
            fullDayList.add(dateFormatYYYYMMDD.format(c.getTime()));
            c.add(Calendar.DAY_OF_MONTH, 1);
        }
        return fullDayList;
    }


    /**
     * 获取一周的开始和结束时间
     */
    public static List<Date> dateToWeekStartAndEnd(Date date) {
        List<Date> result = new ArrayList<>();
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);

        c.set(Calendar.DAY_OF_WEEK, 2);//设置为星期1
        result.add(c.getTime());

        c.add(Calendar.DAY_OF_WEEK_IN_MONTH, 1);//加一个周
        c.add(Calendar.MILLISECOND, -1);//减一毫秒
        result.add(c.getTime());

        return result;
    }

    /**
     * 获取某周 所有日期（yyyy-mm-dd格式字符串）
     */
    public static List<String> getWeekFullDay(Date date) {
        SimpleDateFormat dateFormatYYYYMMDD = new SimpleDateFormat(DATE_FORMAT);
        List<String> fullDayList = new ArrayList<>();
        // 获得当前日期对象
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        //周一
        c.set(Calendar.DAY_OF_WEEK, 2);
        int count = c.getActualMaximum(Calendar.DAY_OF_WEEK);
        for (int j = 1; j <= count; j++) {
            fullDayList.add(dateFormatYYYYMMDD.format(c.getTime()));
            c.add(Calendar.DAY_OF_WEEK, 1);
        }
        return fullDayList;
    }

    /**
     * 获取当前date的上周的周一
     */
    public static Date getLastWeekMonday(Date date) {
        Date monday = getWeekMonday(date);
        Date lastDate = getLastSec(monday);
        return getWeekMonday(lastDate);
    }

    /**
     * 获取当前date的所在周的周一
     */
    public static Date getWeekMonday(Date date) {
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime();
    }

    /**
     * 获取一天的开始时间和结束时间
     */
    public static List<Date> dateToDayStartAndEnd(Date date) {
        List<Date> result = new ArrayList<>();
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        //将时分秒,毫秒域清零
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        result.add(c.getTime());

        c.add(Calendar.DAY_OF_YEAR, 1);
        c.add(Calendar.SECOND, -1);
        result.add(c.getTime());
        return result;
    }

    /**
     * 获取今天一天的开始时间和结束时间
     */
    public static List<Date> getToday() {
        return dateToDayStartAndEnd(new Date());
    }

    /**
     * 获取 时间段内 所有日期（yyyy-mm-dd格式字符串）
     */
    public static List<String> getFullDay(Date startTime, Date endTime) {
        SimpleDateFormat dateFormatYYYYMMDD = new SimpleDateFormat(DATE_FORMAT);
        List<String> fullDayList = new ArrayList<>();
        // 获得当前日期对象
        Calendar s = Calendar.getInstance();
        s.setTime(startTime);
        s.set(Calendar.HOUR_OF_DAY, 0);
        s.set(Calendar.MINUTE, 0);
        s.set(Calendar.SECOND, 0);
        s.set(Calendar.MILLISECOND, 0);
        Calendar e = Calendar.getInstance();
        e.setTime(endTime);
        e.set(Calendar.HOUR_OF_DAY, 0);
        e.set(Calendar.MINUTE, 0);
        e.set(Calendar.SECOND, 0);
        e.set(Calendar.MILLISECOND, 0);

        int daysBetween = getDaysBetween(s.getTime(), e.getTime());
        for (int i = 0; i <= daysBetween; i++) {
            fullDayList.add(dateFormatYYYYMMDD.format(s.getTime()));
            s.add(Calendar.DAY_OF_MONTH, 1);
        }
        return fullDayList;
    }

    //====================================时间计算=======================================>>

    /**
     * 计算两个日期之间相差的秒数
     *
     * @param smdate 较小的时间
     * @param bdate  较大的时间
     * @return 相差秒数
     */
    public static int getSecsBetween(Date smdate, Date bdate) {
        SimpleDateFormat sdf = new SimpleDateFormat(DATETIME_FORMAT);
        try {
            smdate = sdf.parse(sdf.format(smdate));
            bdate = sdf.parse(sdf.format(bdate));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(smdate);
        long time1 = cal.getTimeInMillis();
        cal.setTime(bdate);
        long time2 = cal.getTimeInMillis();
        long between_days = (time2 - time1) / 1000;

        return Integer.parseInt(String.valueOf(between_days));
    }

    /**
     * 计算两个日期之间相差的天数
     *
     * @param smdate 较小的时间
     * @param bdate  较大的时间
     * @return 相差天数
     */
    public static int getDaysBetween(Date smdate, Date bdate) {
        int second = getSecsBetween(smdate, bdate);
        long between_days = second / 3600 / 24;

        return Integer.parseInt(String.valueOf(between_days));
    }

    /**
     * 根据日期判断是上午下午还是晚上
     *
     * @return 1:上午 2:下午 3:晚上
     */
    public static Integer getTimeByDate(Date date) {
        if (date == null) return null;
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        int hour = c.get(Calendar.HOUR_OF_DAY);
        if (hour <= 12) return 1;
        else if (hour <= 18) return 2;
        else return 3;
    }


    /**
     * 该天是否为月末最后一天
     */
    public static boolean isLastDayOfMonth(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.DATE, (calendar.get(Calendar.DATE) + 1));
        return calendar.get(Calendar.DAY_OF_MONTH) == 1;
    }

    /**
     * 获取该月有多少天
     */
    public static int getDaysOfMonth(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
    }


    /**
     * 计算得到2个时间的差值，并转成中文
     *
     * @param startDate 开始时间
     * @param endDate   结束时间
     */
    public static String getDateDiff(Date startDate, Date endDate) {
        long nd = 1000 * 24 * 60 * 60;
        long nh = 1000 * 60 * 60;
        long nm = 1000 * 60;
        long ns = 1000;
        //long ns = 1000;
        //获得两个时间的毫秒时间差异
        long diff = endDate.getTime() - startDate.getTime();
        //计算差多少天
        long day = diff / nd;
        //计算差多少小时
        long hour = diff % nd / nh;
        //计算差多少分钟
        long min = diff % nd % nh / nm;
        //计算差多少秒//输出结果
        long sec = diff % nd % nh % nm / ns;
        String result = "";
        if (day > 0) result += day + "天";
        if (hour > 0) result += hour + "小时";
        if (min > 0) result += min + "分钟";
        if (result.isEmpty()) result = sec + "秒";
        return result;
    }

    /**
     * 计算开始时间和结束时间的差值，转成多少小时
     *
     * @param startDate 开始时间
     * @param endDate   结束时间
     * @return eg: 3.21 (小时)
     */
    public static BigDecimal getDateDiffHour(Date startDate, Date endDate) {
        BigDecimal nh = new BigDecimal(1000 * 60 * 60);
        BigDecimal diff = new BigDecimal(endDate.getTime() - startDate.getTime());
        return diff.divide(nh, 2, BigDecimal.ROUND_HALF_UP);
    }

    /**
     * 计算开始时间和结束时间的差值，转成多少天
     *
     * @param startDate 开始时间
     * @param endDate   结束时间
     * @return eg: 3.21 (天)
     */
    public static BigDecimal getDateDiffDay(Date startDate, Date endDate) {
        BigDecimal nh = new BigDecimal(1000 * 60 * 60 * 24);
        BigDecimal diff = new BigDecimal(endDate.getTime() - startDate.getTime());
        return diff.divide(nh, 2, BigDecimal.ROUND_HALF_UP);
    }


    //====================================获取时间段=======================================>>

    /**
     * 获取这一周的date，从周一到周日
     */
    public static List<Date> dateToWeek(Date mdate) {
        Calendar c = Calendar.getInstance();
        c.setTime(mdate);
        int b = c.get(Calendar.DAY_OF_WEEK) - 1;

        Date fdate;
        List<Date> list = new ArrayList<>();
        long fTime = mdate.getTime() - b * 24 * 3600000;
        for (int a = 1; a <= 7; a++) {
            fdate = new Date();
            fdate.setTime(fTime + (a * 24 * 3600000));
            list.add(a - 1, fdate);
        }
        return list;
    }

    /**
     * 获取前几天的date
     */
    public static List<Date> getDateListBefore(Date date, Integer num) {
        if (date == null) return null;
        List<Date> result = new ArrayList<>();
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.DATE, -num);
        for (int a = 1; a <= num; a++) {
            c.add(Calendar.DATE, 1);
            Date temp = c.getTime();
            result.add(temp);
        }
        return result;
    }

    /**
     * 获取一周、月、年的date
     */
    public static List<Date> getDateListByKind(Date date, String kind) {
        if (date == null || kind == null) return null;

        Calendar end = Calendar.getInstance();

        Calendar c = Calendar.getInstance();
        c.setFirstDayOfWeek(Calendar.MONDAY);
        c.setTime(date);
        //将时分秒,毫秒域清零
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        switch (kind) {
            case "week":
                c.add(Calendar.DATE, c.getFirstDayOfWeek() - c.get(Calendar.DAY_OF_WEEK));
                end.setTime(c.getTime());
                end.add(Calendar.DATE, 7);
                break;
            case "month":
                c.set(Calendar.DAY_OF_MONTH, 1);
                end.setTime(c.getTime());
                end.add(Calendar.MONTH, 1);
                break;
            case "half":
                c.add(Calendar.MONTH, -6);
                c.set(Calendar.DAY_OF_MONTH, 1);
                end.setTime(date);
                end.set(Calendar.DAY_OF_MONTH, 1);
                end.add(Calendar.MONTH, 1);
                end.add(Calendar.DAY_OF_MONTH, -1);
                break;
            case "year":
                c.set(Calendar.DAY_OF_YEAR, 1);
                end.setTime(c.getTime());
                end.add(Calendar.YEAR, 1);
                break;
            default:
                return null;
        }

        boolean setSec = false;
        List<Date> result = new ArrayList<>();
        while (c.before(end)) {
            if (result.size() != 0 && !setSec) {
                //将时分秒,毫秒域清零
                c.set(Calendar.HOUR_OF_DAY, 23);
                c.set(Calendar.MINUTE, 59);
                c.set(Calendar.SECOND, 59);
                c.set(Calendar.MILLISECOND, 999);
                setSec = true;
            }
            result.add(c.getTime());
            c.add(Calendar.DATE, 1);
        }
        return result;
    }

    //====================================修改时间=======================================>>

    /**
     * 获取前一秒
     */
    public static Date getLastSec(Date date) {
        if (date == null) return null;
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.SECOND, -1);
        return c.getTime();
    }

    /**
     * 添加秒
     */
    public static Date getWhichSecs(Date date, int secs) {
        if (date == null) return null;
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.SECOND, secs);
        return c.getTime();
    }

    /**
     * 添加分
     */
    public static Date getWhichMinute(Date date, int minute) {
        if (date == null) return null;
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.MINUTE, minute);
        return c.getTime();
    }

    /**
     * 获取后一天
     */
    public static Date getNextDate(Date date) {
        return addDay(date, 1);
    }

    /**
     * 添加天
     */
    public static Date addDay(Date date, Integer day) {
        if (date == null || day == null) return null;
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.DATE, day);
        return c.getTime();
    }

    /**
     * 添加月
     */
    public static Date addMonth(Date date, Integer month) {
        if (date == null || month == null) return null;
        if (month == 0) return date;
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.MONTH, month);
        return c.getTime();
    }

    /**
     * 添加年
     */
    public static Date addYear(Date date, Integer year) {
        if (date == null || year == null) return null;
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.YEAR, year);
        return c.getTime();
    }

    /**
     * 清除时分秒
     */
    public static Date clearTime(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        //将时分秒,毫秒域清零
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        return c.getTime();
    }

    /**
     * 清除秒
     */
    public static Date clearSec(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        return c.getTime();
    }

    /**
     * 将时分秒设置成当前时间
     */
    public static Date setTimeToNow(Date date) {
        return setTime(date, new Date());
    }

    /**
     * 将时分秒设置成新时间
     */
    public static Date setTime(Date date, Date newDate) {
        if (date == null) return null;
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        Calendar now = Calendar.getInstance();
        now.setTime(newDate);
        //将时分秒,毫秒域清零
        c.set(Calendar.HOUR_OF_DAY, now.get(Calendar.HOUR_OF_DAY));
        c.set(Calendar.MINUTE, now.get(Calendar.MINUTE));
        c.set(Calendar.SECOND, now.get(Calendar.SECOND));
        c.set(Calendar.MILLISECOND, now.get(Calendar.MILLISECOND));
        return c.getTime();
    }

    /**
     * Timestamp时间戳转date
     *
     * @param timestamp 时间戳
     */
    public static Date timeToDate(Timestamp timestamp) {
        // Timestamp -> String
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateString = formatter.format(timestamp);
        return DateUtils.strToDate(dateString);
    }

    public static boolean isWorkTime() {
        LocalTime now = LocalTime.now();
        return (now.isAfter(LocalTime.of(5, 0)) && now.isBefore(LocalTime.of(18, 0))) ||
                now.isAfter(LocalTime.of(0, 0)) && now.isBefore(LocalTime.of(1, 0));
    }

    public static String ehrTimeTransform(String time) {
        if (StringUtils.isBlank(time)) return null;
        String format;
        if (time.length() == 8) {
            format = "yyyyMMdd";
        } else if (time.length() == 14) {
            format = "yyyyMMddHHmmss";
        } else {
            format = "yyyy-MM-dd HH:mm:ss";
        }
        return DateUtils.dateToStr(DateUtils.strToDate(time, format), "yyyy-MM-dd HH:mm:ss");
    }


    public static void awaitFreeTime() {
        while (isWorkTime()) {
            try {
                Thread.sleep(600000);
            } catch (InterruptedException e) {
                throw new IllegalArgumentException("任务等待失败");
            }
        }
        System.out.println("等待结束");
    }

}