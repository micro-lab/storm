package com.zhazha.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SplitBolt extends BaseRichBolt {
    private OutputCollector collector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        String spanInfo = tuple.getString(0);
//        测试时空tuple太多
//        if(spanInfo.trim().equals("")){
//            collector.ack(tuple);
//            return;
//        }

        try {
            String[] span = spanInfo.split(",");
            long traceId = Long.parseLong(span[0]);
            long id = Long.parseLong(span[1]);
            long parentId = span[2].equals("") ? 0 : Long.parseLong(span[2]);//没有parentId时为根节点,用0代指根节点
            int state = Integer.parseInt(span[3]);//0:开始,1:结束
            String service = span[4];
            String hostIp = span[5];
            Long timeStamp = Long.parseLong(span[6]);
            long spanId;
            if (state == 0) {
                spanId = id;
            } else {
                spanId = parentId;
            }
            System.out.println(traceId + "|" + spanId + "|" + id + "|" + parentId + "|" + state + "|" + service + "|" + hostIp + "|" + timeStamp);
            collector.emit(new Values(traceId, spanId, id, parentId, state, service, hostIp, timeStamp));
        } catch (Exception e) {
        }
        collector.ack(tuple);
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("traceId","id","parentId","state","service","hostIp","errorCode","errorStack","timeStamp"));

    }

}
