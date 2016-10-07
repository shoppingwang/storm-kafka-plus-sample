package storm.kafka;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 正则测试
 * <p/>
 * Author   : wangxp
 * <p/>
 * DateTime : 2015-11-06 20:45
 */
public class TestRegex {
    public static void main(String[] args) {
        String testStr = "12839322212";
        Pattern pattern = Pattern.compile("\\d{11}");
        Matcher matcher = pattern.matcher(testStr);
        if (matcher.matches()){
            System.out.println(testStr);
        }
    }
}
