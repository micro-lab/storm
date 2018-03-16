package com.zhazha.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SplitBolt extends BaseRichBolt {
    private OutputCollector collector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        String spanInfo = tuple.getString(0);
        String[] span = spanInfo.split(",");
        int traceId = Integer.parseInt(span[0]);
        int id = Integer.parseInt(span[1]);
        int parentId = Integer.parseInt(span[3]);//可能为null
        int state = Integer.parseInt(span[4]);;//0:开始,1:结束
        String service = span[5];
        String hostIp = span[6];
        Long timeStamp = Long.parseLong(span[7]);
        int spanId;
        if(state==0){
            spanId = id;
        }else{
            spanId = parentId;
        }
        collector.emit(new Values(traceId,spanId,id,parentId,state,service,hostIp,timeStamp));
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("traceId", "spanId", "id", "parentId", "state", "service", "hostIp", "timeStamp"));
    }

}
