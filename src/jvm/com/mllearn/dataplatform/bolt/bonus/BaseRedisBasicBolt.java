package com.mllearn.dataplatform.bolt.bonus;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseBasicBolt;
import com.mllearn.dataplatform.mark.ConfigPropertiesHolderSet;
import com.mllearn.dataplatform.util.ConfigPropertiesHolder;
import com.mllearn.dataplatform.util.RedisUtil;
import org.apache.avro.util.Utf8;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * Redis连接
 * <p/>
 * Author   : wangxp
 * <p/>
 * DateTime : 2015-11-08 12:40
 */
public abstract class BaseRedisBasicBolt extends BaseBasicBolt implements ConfigPropertiesHolderSet {

    protected RedisUtil redisUtil;

    protected ConfigPropertiesHolder propertiesHolder;

    protected static final Pattern PHONE_NO_PATTERN = Pattern.compile("\\d{11}");

    protected static final Utf8 CUST_PHONE_NO_FIELD_KEY = new Utf8("phone");

    protected static final Utf8 CUST_CITY_ID_FIELD_KEY = new Utf8("city_id");

    protected static final Utf8 BONUS_PHONE_NUM_FIELD_KEY = new Utf8("customer_phone");

    protected static final Utf8 BONUS_BIND_MONEY_FIELD_KEY = new Utf8("balance");

    protected static final Utf8 BONUS_ORDER_ID_FIELD_KEY = new Utf8("order_id");

    protected static final Utf8 BONUS_USE_MONEY_FIELD_KEY = new Utf8("use_money");

    protected static final Utf8 BONUS_USED_TIME_FIELD_KEY = new Utf8("used");

    protected static final String PHONE_NO_REDIS_KEY_PREFIX = "PHONENO_CITY_";

    protected static final String ORDER_ID_REDIS_KEY_PREFIX = "ORDER_CITY_";

    protected static final String BONUS_BIND_NUM_REDIS_KEY_PREFIX = "BONUS_BIND_NUM_";

    protected static final String BONUS_BIND_MONEY_REDIS_KEY_PREFIX = "BONUS_BIND_MONEY_";

    protected static final String BONUS_USE_NUM_REDIS_KEY_PREFIX = "BONUS_USE_NUM_";

    protected static final String BONUS_USE_MONEY_REDIS_KEY_PREFIX = "BONUS_USE_MONEY_";

    protected static final String BONUS_USE_PHONENO_NEW_REDIS_KEY_PREFIX = "BONUS_USE_PHONENO_NEW_";

    protected static final String BONUS_USE_PHONENO_OLD_REDIS_KEY_PREFIX = "BONUS_USE_PHONENO_OLD_";

    protected static final String BONUS_USE_PERSON_NEW_REDIS_KEY_PREFIX = "BONUS_USE_PERSON_NEW_";

    protected static final String BONUS_USE_PERSON_OLD_REDIS_KEY_PREFIX = "BONUS_USE_PERSON_OLD_";

    protected static final String NEW_CUST_FIRST_ORDER_ID_PREFIX = "NEW_CUST_FIRST_ORDER_ID_";

    protected static final String CITY_ID_FOR_ALL = "0";

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);

            String redisHost = propertiesHolder.getStringValue("bonus.redis.host");
            int redisPort = propertiesHolder.getIntValue("bonus.redis.port");

            redisUtil = new RedisUtil(redisHost, redisPort);
    }

    @Override
    public void setConfigPropertiesHolder(ConfigPropertiesHolder config) {
        this.propertiesHolder = config;
    }
}
