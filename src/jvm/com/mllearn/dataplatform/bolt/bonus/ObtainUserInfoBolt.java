package com.mllearn.dataplatform.bolt.bonus;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.mllearn.dataplatform.common.RedisOp;
import mypipe.avro.InsertMutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 获取用户号码对应的ID信息
 * <p/>
 * Author   : wangxp
 * <p/>
 * DateTime : 2015-11-08 11:07
 */
public class ObtainUserInfoBolt extends BaseRedisBasicBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(ObtainUserInfoBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

        Object value = tuple.getValue(0);
        LOGGER.debug("ObtainUserInfoBolt msg is {}", value);

        if (null != value && value instanceof InsertMutation) {
            InsertMutation mutation = (InsertMutation) value;

            String phoneNo = mutation.getStrings().get(CUST_PHONE_NO_FIELD_KEY).toString();
            Integer cityId = mutation.getIntegers().get(CUST_CITY_ID_FIELD_KEY);

            if (PHONE_NO_PATTERN.matcher(phoneNo).matches() && cityId != null) {
//                LOGGER.info("Put phone no.({}) city({}) info to redis.", phoneNo, cityId);
                collector.emit(new Values(PHONE_NO_REDIS_KEY_PREFIX + phoneNo, cityId.toString(), RedisOp.SET));
            }
        } else {
            LOGGER.info("Ignore ObtainUserInfoBolt msg is {}", value);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("redisKey", "redisValue", "opType"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
    }
}
