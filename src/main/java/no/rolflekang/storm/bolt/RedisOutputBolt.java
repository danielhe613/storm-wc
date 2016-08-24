package no.rolflekang.storm.bolt;


import redis.clients.jedis.Jedis;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class RedisOutputBolt extends BaseBasicBolt {
    Jedis jedis;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
//        jedis = new Jedis("192.168.56.2");
    }

 
    public void execute(Tuple tuple, BasicOutputCollector collector) {
//    	System.out.println(tuple.getString(0) + ": " + tuple.getInteger(1));
//        jedis.publish("storm-wc", tuple.getString(0) + ": " + tuple.getInteger(1));
    	
//    	jedis.lpush("storm-wc", tuple.getString(0) + ": " + tuple.getInteger(1));
    	
    	   System.out.println(Thread.currentThread().getName() + ":" + tuple.getString(0));
    }

 
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) { }
}
