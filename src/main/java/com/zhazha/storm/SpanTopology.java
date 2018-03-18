package com.zhazha.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Arrays;

public class SpanTopology {
    public static void main(String[] args) {
        // 配置spout的kafka信息
        //zookeeper
        String zks = "localhost:2181";
        String topic = "test";

        // 配置用于记录storm消费位移的zk目录
        String zkRoot = "/kafkaspout_offest";
        String id = "test";

        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);

        // 配置用于记录storm消费位移的zk,可以和kafka的zk分离
        //zookeeper
        spoutConf.zkServers =Arrays.asList("localhost".split(","));
        spoutConf.zkPort = 2181;

        // 配置spout的输出类型
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());

        // 创建拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", new KafkaSpout(spoutConf));
        builder.setBolt("vBolt", new SplitBolt()).shuffleGrouping("kafka-spout");
        builder.setBolt("message-bolt", new SpanBolt(),2).fieldsGrouping("vBolt",new Fields("traceId","id"));

        Config config = new Config();
        // 设置并行度
        config.setNumWorkers(2);

        // 设置kafkaBolt的输出topic
        config.put("topic", "log");

        // 设置拓扑名
        String name = "test";

        // 提交任务
        new LocalCluster().submitTopology(name, config, builder.createTopology());
//        StormSubmitter.submitTopologyWithProgressBar(name, config, builder.createTopology());
    }
}
