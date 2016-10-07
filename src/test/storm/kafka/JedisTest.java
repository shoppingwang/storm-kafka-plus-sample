package storm.kafka;

import com.mllearn.dataplatform.util.RedisUtil;

/**
 * Jedis测试
 * <p/>
 * Author   : wangxp
 * <p/>
 * DateTime : 2015-11-08 12:36
 */
public class JedisTest {
    public static void main(String[] args) {
        RedisUtil redisUtil = new RedisUtil("172.16.170.151", 6379);
        redisUtil.set("testkey", "testValue");

        redisUtil.incrBy("kk", 39l);
    }
}
