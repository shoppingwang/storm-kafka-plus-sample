package com.mllearn.dataplatform.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 时间格式化
 * <p/>
 * Author   : wangxp
 * <p/>
 * DateTime : 2015-11-07 17:05
 */
public class DateTimeUtil {
    public static final String DATE_FORMAT_PATTERN_TO_MINUTE = "yyyy-MM-dd HH:mm";

    public static String format(Date date, String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.format(date);
    }

    public static Date parse(String date, String pattern) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.parse(date);
    }
}
