package storm.apache;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class WordCountTopology {
	
	public static void main(String[] args) throws InterruptedException {
		Config config = new Config();
		config.put("inputFile", "F:/demo.txt");
		config.put("outputFile", "F:/demoOut.txt");
		config.setDebug(true);
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("line-reader-spout", new LineReaderSpout());
		builder.setBolt("word-spitter-bolt", new wordSpitterBolt()).shuffleGrouping("line-reader-spout");
		builder.setBolt("word-count-bolt", new wordCountBolt()).shuffleGrouping("word-spitter-bolt");
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("WordCountTopology", config, builder.createTopology());
		
		Thread.sleep(10000);
		
		cluster.shutdown();
	}
}
