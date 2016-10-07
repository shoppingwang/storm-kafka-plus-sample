package com.mllearn.dataplatform.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.mllearn.dataplatform.bolt.bonus.BaseRedisBasicBolt;
import com.mllearn.dataplatform.common.RedisOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 保存数据至Redis
 * <p/>
 * Author   : wangxp
 * <p/>
 * DateTime : 2015-11-07 15:43
 */
public class SaveRedisBolt extends BaseRedisBasicBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaveRedisBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String redisKey = tuple.getStringByField("redisKey");
        String redisValue = tuple.getStringByField("redisValue");
        String opType = tuple.getStringByField("opType");

        if (RedisOp.SET.equals(opType)) {
            redisUtil.set(redisKey, redisValue);
        } else if (RedisOp.INCRBY.equals(opType)) {
            redisUtil.incrBy(redisKey, Long.parseLong(redisValue));
        } else {
            LOGGER.error("SaveRedisBolt opType({}) error.", opType);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
