package storm.kafka;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 日期格式化测试
 * <p/>
 * Author   : wangxp
 * <p/>
 * DateTime : 2015-11-07 16:14
 */
public class TestDateFormat {
    public static void main(String[] args) throws ParseException {
        Calendar ca = Calendar.getInstance();
        int currentMinute = ca.get(Calendar.MINUTE);
        int currentFiveMinute = currentMinute - (currentMinute % 5);
        ca.set(Calendar.MINUTE, currentFiveMinute);
        ca.add(Calendar.MINUTE, 5);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");

        Date execTime = sdf.parse(sdf.format(ca.getTime()));

        System.out.println(execTime);
    }
}
