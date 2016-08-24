package no.rolflekang.storm.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitBolt extends BaseBasicBolt {
    private String pattern;

    public SplitBolt () {
       this.pattern = " ";
    }
    public SplitBolt (String pattern) {
        this.pattern = pattern;
    }

  
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        for(String word:tuple.getString(0).split(pattern)) {
            collector.emit(new Values(word));
        }
    }

    
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
