package no.rolflekang.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitBolt extends BaseBasicBolt {
    private String pattern;

    public SplitBolt () {
       this.pattern = " ";
    }
    public SplitBolt (String pattern) {
        this.pattern = pattern;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        for(String word:tuple.getString(0).split(pattern)) {
            collector.emit(new Values(word));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
