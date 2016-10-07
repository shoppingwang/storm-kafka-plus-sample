package com.mllearn.dataplatform.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 打印消息BOLT
 * <p/>
 * Author   : wangxp
 * <p/>
 * DateTime : 2015-11-07 10:20
 */
public class PrintMsgBolt extends BaseBasicBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(PrintMsgBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        LOGGER.info("PrintMsgBolt msg is {}", tuple.getValue(0));

        collector.emit(new Values(tuple.getValue(0)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("msg"));
    }
}
