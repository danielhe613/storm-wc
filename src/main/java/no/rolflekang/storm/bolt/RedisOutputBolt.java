package no.rolflekang.storm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

import java.util.Map;

public class RedisOutputBolt extends BaseBasicBolt {
    Jedis jedis;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        jedis = new Jedis("localhost");
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        jedis.publish("storm-wc", tuple.getString(0) + ": " + tuple.getInteger(1));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) { }
}
