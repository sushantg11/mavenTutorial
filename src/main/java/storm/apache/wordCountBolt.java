package storm.apache;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class wordCountBolt implements IRichBolt {

	OutputCollector collector;
	FileWriter filewriter;
	Map<String, Integer> counter;
	
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		
		try {
			this.filewriter = new FileWriter(conf.get("outputFile").toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.collector = collector;
		counter = new HashMap<String, Integer>();
	}
	
	public void cleanup() { 
		BufferedWriter writer = new BufferedWriter(filewriter);
		for(Map.Entry<String, Integer> entry : counter.entrySet()) {
			try {
				String str = entry.getKey() + " : " + entry.getValue().toString();
				System.out.println(str);
				writer.write(str);
				writer.newLine();
				writer.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		try {
			writer.close();
			filewriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void execute(Tuple tuple) {
		String word = tuple.getString(0);
		if(word != null) {
			int value = 1;
			if(counter.containsKey(word))
				value += counter.get(word);
			counter.put(word, value);
		}
		collector.ack(tuple);
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
