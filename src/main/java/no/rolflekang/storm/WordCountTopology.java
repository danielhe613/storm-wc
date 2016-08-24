package no.rolflekang.storm;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import no.rolflekang.storm.bolt.RedisOutputBolt;
import no.rolflekang.storm.bolt.SplitBolt;
import no.rolflekang.storm.bolt.WordCountBolt;
import no.rolflekang.storm.spout.RandomSentenceSpout;

public class WordCountTopology {

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", (IRichSpout) new RandomSentenceSpout(), 5);

   
    
    builder.setBolt("split", new SplitBolt(), 8).shuffleGrouping("spout");
    builder.setBolt("count", new WordCountBolt(), 12).fieldsGrouping("split", new Fields("word"));
    builder.setBolt("redis", new RedisOutputBolt(), 12).fieldsGrouping("count", new Fields("word"));

    Config conf = new Config();
    conf.setDebug(false);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(20);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

      Thread.sleep(60000);

      cluster.shutdown();
    }
  }
}
