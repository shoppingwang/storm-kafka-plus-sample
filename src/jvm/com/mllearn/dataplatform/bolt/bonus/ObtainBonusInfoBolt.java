package com.mllearn.dataplatform.bolt.bonus;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.mllearn.dataplatform.common.RedisOp;
import mypipe.avro.InsertMutation;
import mypipe.avro.UpdateMutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 获取优惠券相关信息
 * <p/>
 * Author   : wangxp
 * <p/>
 * DateTime : 2015-11-08 11:07
 */
public class ObtainBonusInfoBolt extends BaseRedisBasicBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(ObtainBonusInfoBolt.class);

    private List<String> cityIds;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

        Object value = tuple.getValue(0);
        LOGGER.debug("ObtainBonusInfoBolt msg is {}", value);

        if (null != value && value instanceof InsertMutation) {
            InsertMutation mutation = (InsertMutation) value;

            String phoneNo = mutation.getStrings().get(BONUS_PHONE_NUM_FIELD_KEY).toString();
            Integer bandMoney = mutation.getIntegers().get(BONUS_BIND_MONEY_FIELD_KEY);

            String cityId = redisUtil.get(PHONE_NO_REDIS_KEY_PREFIX + phoneNo);
            //重试获取绑定用户城市信息
            int retryIndex = 0;
            while (cityId == null && retryIndex < 3) {
                retryIndex++;

                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    LOGGER.error("Obtain band bonus retry error.", e);
                }

                cityId = redisUtil.get(PHONE_NO_REDIS_KEY_PREFIX + phoneNo);
            }

            if (null != bandMoney) {
                //全国数据
                emitBonusBandData(collector, bandMoney, CITY_ID_FOR_ALL);

                //城市数据
                if (null != cityId) {
                    if (cityIds.contains(cityId)) {
                        emitBonusBandData(collector, bandMoney, cityId);
                    } else {
                        LOGGER.warn("The city({}) for band bonus was not be monitored.", cityId);
                    }
                } else {
                    LOGGER.warn("Can not find band bonus phone no.({}) city info.", phoneNo);
                }
            }
        } else if (value instanceof UpdateMutation) {
            UpdateMutation mutation = (UpdateMutation) value;

            Integer orderId = mutation.getNewIntegers().get(BONUS_ORDER_ID_FIELD_KEY);
            CharSequence phoneNo = mutation.getNewStrings().get(BONUS_PHONE_NUM_FIELD_KEY);
            Integer useMoney = mutation.getNewIntegers().get(BONUS_USE_MONEY_FIELD_KEY);
            Integer usedTime = mutation.getNewIntegers().get(BONUS_USED_TIME_FIELD_KEY);
            String cityId = redisUtil.get(ORDER_ID_REDIS_KEY_PREFIX + orderId);

            //判断是否为新客
            String firstOrderId = redisUtil.get(NEW_CUST_FIRST_ORDER_ID_PREFIX + phoneNo);
            boolean isNewCust = (null == firstOrderId || firstOrderId.equals(orderId.toString()));

            if (null != useMoney && null != usedTime && usedTime > 0 && null != orderId && null != phoneNo) {
                //全国数据
                emitBonusUserData(collector, phoneNo.toString(), useMoney, CITY_ID_FOR_ALL, isNewCust);

                //城市数据
                if (null != cityId) {
                    if (cityIds.contains(cityId))
                    {
                        emitBonusUserData(collector, phoneNo.toString(), useMoney, cityId, isNewCust);
                    } else {
                        LOGGER.warn("The city({}) for use bonus was not be monitored.", cityId);
                    }

                } else {
                    LOGGER.warn("Can not find use bonus order no.({}) city info.", orderId);
                }
            }
        } else {
            LOGGER.info("Ignore ObtainBonusInfoBolt msg is {}", value);
        }
    }

    private void emitBonusBandData(BasicOutputCollector collector, Integer bandMoney, String cityId) {
        collector.emit(new Values(BONUS_BIND_NUM_REDIS_KEY_PREFIX + cityId, String.valueOf(1), RedisOp.INCRBY));
        collector.emit(new Values(BONUS_BIND_MONEY_REDIS_KEY_PREFIX + cityId, String.valueOf(bandMoney), RedisOp.INCRBY));
    }

    private void emitBonusUserData(BasicOutputCollector collector, String phoneNo, Integer useMoney, String cityId, boolean isNewCust) {
        collector.emit(new Values(BONUS_USE_NUM_REDIS_KEY_PREFIX + cityId, String.valueOf(1), RedisOp.INCRBY));
        collector.emit(new Values(BONUS_USE_MONEY_REDIS_KEY_PREFIX + cityId, String.valueOf(useMoney), RedisOp.INCRBY));
        if (isNewCust) {
            long added = redisUtil.sadd(BONUS_USE_PHONENO_NEW_REDIS_KEY_PREFIX + cityId, phoneNo);
            collector.emit(new Values(BONUS_USE_PERSON_NEW_REDIS_KEY_PREFIX + cityId, String.valueOf(added), RedisOp.INCRBY));
        } else {
            long added = redisUtil.sadd(BONUS_USE_PHONENO_OLD_REDIS_KEY_PREFIX + cityId, phoneNo);
            collector.emit(new Values(BONUS_USE_PERSON_OLD_REDIS_KEY_PREFIX + cityId, String.valueOf(added), RedisOp.INCRBY));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("redisKey", "redisValue", "opType"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);

        cityIds = new ArrayList<>();
        //添加全国城市
        cityIds.add(CITY_ID_FOR_ALL);
        //添加具体城市
        cityIds.addAll(Arrays.asList(propertiesHolder.getStringValue("bonus.cityids").split(",")));
    }
}
