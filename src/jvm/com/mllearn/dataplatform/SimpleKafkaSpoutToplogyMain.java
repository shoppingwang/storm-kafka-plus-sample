package com.mllearn.dataplatform;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.TopologyBuilder;
import com.mllearn.dataplatform.mark.ConfigPropertiesHolderSet;
import com.mllearn.dataplatform.spout.scheme.MySQLBinLogAvroMessageScheme;
import com.mllearn.dataplatform.util.ConfigPropertiesHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.Arrays;

/**
 * 优惠券实时统计
 * <p/>
 * Author   : wangxp
 * <p/>
 * DateTime : 2015-11-06 16:35
 */
public class SimpleKafkaSpoutToplogyMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaSpoutToplogyMain.class);

    private static final ConfigPropertiesHolder propertiesHolder = new ConfigPropertiesHolder();

    public static void main(String[] args) {
        try {
            //日志配置文件加载
            //LogBackConfigLoader.load("conf/logback.xml");

            //获取配置文件参数名称
            if (args != null && args.length > 0) {
                propertiesHolder.init(args[0]);
            } else {
                propertiesHolder.init("com.mllearn.dp.config.local.all.conf");
            }

            StormTopology topology = buildTopology();

            Config config = new Config();
            if (args != null && args.length > 1) {
                config.setDebug(false);
                config.setNumWorkers(propertiesHolder.getIntValue("storm.topology.workers"));

                StormSubmitter.submitTopology(args[1], config, topology);
            } else {
                config.setDebug(true);
                config.setMaxTaskParallelism(3);

                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology(SimpleKafkaSpoutToplogyMain.class.getSimpleName(), config, topology);

                Thread.sleep(500000);

                cluster.shutdown();
            }
        } catch (Exception e) {
            LOGGER.error("Storm main class execute error.", e);
            System.exit(-1);
        }
    }

    /**
     * 构造STORM TOPOLOGY描述信息
     *
     * @return TOPOLOGY描述信息
     */
    private static StormTopology buildTopology() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        String zkServers = propertiesHolder.getStringValue("kafka.zk.servers").trim();
        int zkPort = propertiesHolder.getIntValue("kafka.zk.port");

        String kafkaZKHosts = propertiesHolder.getStringValue("kafka.zk.hosts").trim();
        String brokerZKPath = propertiesHolder.getStringValue("kafka.zk.broker.path").trim();
        BrokerHosts brokerHosts = new ZkHosts(kafkaZKHosts, brokerZKPath);

        String zkRoot = propertiesHolder.getStringValue("kafka.zk.root").trim();
        String spoutParams = propertiesHolder.getStringValue("kafka.spout.params").trim();

        //添加SPOUT信息
        for (String spoutParam : spoutParams.split(",")) {
            String[] concreteSpoutParams = spoutParam.split(":");
            SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, concreteSpoutParams[0].trim(), zkRoot, concreteSpoutParams[0].trim());

            kafkaConfig.scheme = new SchemeAsMultiScheme(new MySQLBinLogAvroMessageScheme());
            //kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
            kafkaConfig.zkServers = Arrays.asList(zkServers.split(":"));
            kafkaConfig.zkPort = zkPort;

            //以KAFKA主题为SPOUT的ID
            builder.setSpout(concreteSpoutParams[0].trim(), new KafkaSpout(kafkaConfig), Integer.parseInt(concreteSpoutParams[1].trim()))
                    .setNumTasks(Integer.parseInt(concreteSpoutParams[2].trim()));
        }

        //添加BOLT信息
        String boltTopics = propertiesHolder.getStringValue("storm.bolt.params").trim();
        String[] boltDescs = boltTopics.split(",");
        for (String boltDesc : boltDescs) {
            String[] boltParam = boltDesc.split(":");
            Object boltInst = Class.forName(boltParam[1].trim()).newInstance();
            if (boltInst instanceof ConfigPropertiesHolderSet) {
                ((ConfigPropertiesHolderSet) boltInst).setConfigPropertiesHolder(propertiesHolder);
            }
            BoltDeclarer boltDeclarer = builder.setBolt(boltParam[0].trim(), (IBasicBolt) boltInst, Integer.parseInt(boltParam[2]))
                    .setNumTasks(Integer.parseInt(boltParam[3].trim()));
            for (String srcCompnetId : boltParam[4].trim().split("\\|")) {
                boltDeclarer.shuffleGrouping(srcCompnetId.trim());
            }
        }

        return builder.createTopology();
    }
}