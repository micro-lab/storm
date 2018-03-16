package com.zhazha.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import kafka.api.OffsetRequest;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Arrays;

public class SpanToplogic {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        // 配置spout的kafka信息
        //生产zookeeper
        //String zks ="bj02-im-hdp01.pro.gomeplus.com:2181,bj02-im-hdp02.pro.gomeplus.com:2181,bj02-im-hdp03.pro.gomeplus.com:2181,bj02-im-hdp04.pro.gomeplus.com:2181,bj02-im-hdp05.pro.gomeplus.com:2181";//pro
        //预生产zookeeper
        String zks = "VM-10-115-3-115:2181,VM-10-115-3-116:2181,VM-10-115-3-117:2181,VM-10-115-3-118:2181,VM-10-115-3-119:2181";// pre
        String topic = "test";

        // 配置用于记录storm消费位移的zk目录
        String zkRoot = "/kafkaspout_offest";
        String id = "test";

        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);

        // 配置用于记录storm消费位移的zk,可以和kafka的zk分离
        //预生产zookeeper
        spoutConf.zkServers = Arrays.asList("VM-10-115-3-115,VM-10-115-3-116,VM-10-115-3-117,VM-10-115-3-118,VM-10-115-3-119".split(",")); // pre
        //生产zookeeper
//		 spoutConf.zkServers =Arrays.asList("bj02-im-hdp01.pro.gomeplus.com,bj02-im-hdp02.pro.gomeplus.com,bj02-im-hdp03.pro.gomeplus.com,bj02-im-hdp04.pro.gomeplus.com,bj02-im-hdp05.pro.gomeplus.com".split(","));
        //开发zookeeper
        // spoutConf.zkServers =Arrays.asList("bj01-im-data01,bj01-im-data02,bj01-im-data03".split(","));
        spoutConf.zkPort = 2181;

        // 配置spout的输出类型
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());


        // 创建拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", new KafkaSpout(spoutConf), 4);
//        builder.setBolt("vBolt", new MessageBolt(), 4).shuffleGrouping("kafka-spout");
//        builder.setBolt("message-bolt", messageBolt, 4).shuffleGrouping("vBolt", "message");
//        builder.setBolt("login-bolt", loginBolt, 4).shuffleGrouping("vBolt", "login");
//        builder.setBolt("group-bolt", groupBolt, 2).shuffleGrouping("vBolt", "group");
//        builder.setBolt("file-bolt", fileBolt, 2).shuffleGrouping("vBolt", "file");
//        builder.setBolt("log-kafka-bolt", kkBolt, 4).shuffleGrouping("vBolt", "log");

        Config config = new Config();
        // 设置并行度
        config.setNumWorkers(4);

        // 设置kafkaBolt的输出topic
        config.put("topic", "log");

        // 设置拓扑名
        String name = "test";

        // 提交任务
		 new LocalCluster().submitTopology(name, config,builder.createTopology());
//        StormSubmitter.submitTopologyWithProgressBar(name, config, builder.createTopology());
    }
}
