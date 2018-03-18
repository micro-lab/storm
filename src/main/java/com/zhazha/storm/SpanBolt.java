package com.zhazha.storm;

import com.sun.org.apache.xpath.internal.SourceTree;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class SpanBolt extends BaseRichBolt implements Runnable {
    private final static Logger LOG = LoggerFactory.getLogger(SpanBolt.class);
    private OutputCollector collector;
    private Map<String, List<Tuple>> spanMap = new HashMap<String, List<Tuple>>();


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        Long traceId = (Long) tuple.getValue(0);
        Long id = (Long) tuple.getValue(1);
//        Long parentId = (Long) tuple.getValue(2);
        int state = (Integer) tuple.getValue(3);
        String key = traceId + "_" + id;

        if (state == 0) {
            List<Tuple> tuples = spanMap.get(key);
            if (tuples == null) {
                tuples = new LinkedList<Tuple>();
                tuples.add(tuple);
                spanMap.put(key, tuples);
            } else {
                tuples.add(tuple);
                spanMap.put(key, tuples);
                calcSpan(key);
            }

            spanMap.put(key, tuples);
        } else if (state == 1) {
            List<Tuple> tuples = spanMap.get(key);
            if (tuples == null) {
                tuples = new LinkedList<Tuple>();
                tuples.add(tuple);
                spanMap.put(key, tuples);
            } else {
                tuples.add(tuple);
                spanMap.put(key, tuples);
                calcSpan(key);
            }


        }
    }

    private void calcSpan(String key) {
        if(spanMap.get(key).size()!=2){
            LOG.error("这个BUG严重了~");
        }else{
            Tuple startTuple = null;
            Tuple endTuple = null;
            List<Tuple> tuples = spanMap.get(key);
            for(Tuple tuple:tuples){
                if((Integer)tuple.getValue(3)==0){
                    startTuple = tuple;
                }else{
                    endTuple = tuple;
                }
            }
            long startTamp = (Long)startTuple.getValue(8);
            long endTamp = (Long)endTuple.getValue(8);
            System.out.println("");

        }
        spanMap.remove(key);

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public void run() {
//        long StartTime = (Long) StartTuple.getValue(7);
//        long currentTime = System.currentTimeMillis();
//        if (currentTime - StartTime > 120000) {
//            StartTuple = null;
//            endTuples.removeAll(endTuples);
//        }

    }
}
