package com.zhazha.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

public class SpanBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Tuple StartTuple;
    private List<Tuple> endTuples = new LinkedList<Tuple>();

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        int state = (Integer) tuple.getValue(4);
        if (state == 0) {//开始状态
            StartTuple = tuple;
            if (endTuples.size() != 0) {
                calcSpan();
            }
        } else {//结束状态
            endTuples.add(tuple);
            if (this.StartTuple != null) {
                calcSpan();
            }

        }


    }

    private void calcSpan(){
        if(endTuples.size()!=0){
            Tuple tuple = endTuples.get(0);
            int traceId = (Integer) tuple.getValue(0);
            int spanId = (Integer) tuple.getValue(1);
            int id = (Integer) tuple.getValue(2);
            int parentId = (Integer) tuple.getValue(3);
            int state = (Integer) tuple.getValue(4);
            String service = (String) tuple.getValue(5);
            String hostIp = (String) tuple.getValue(6);
            Long timeStamp = (Long) tuple.getValue(7);
            int StrartId = (Integer) StartTuple.getValue(2);
            String StartService = (String) StartTuple.getValue(5);
            String StrartHostIp = (String) StartTuple.getValue(6);
            Long StartTimeStamp = (Long) StartTuple.getValue(7);
            Long during =StartTimeStamp -timeStamp;
            System.out.println(" StartService:"+StartService+" EndService:"+service+" StrartHostIp:"+StrartHostIp+" EndHostIp:"+hostIp+" during:"+ during);
            endTuples.remove(0);
            calcSpan();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
