package storm.apache;

import java.util.Map;
import java.io.*;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class LineReaderSpout implements IRichSpout {
	
	private SpoutOutputCollector collector;
	private TopologyContext context;
	private FileReader filereader;
	private boolean completed = false;

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		
		try {
			this.filereader = new FileReader(conf.get("inputFile").toString());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		this.collector = collector;
	}
	
	
	public void ack(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	public void activate() {
		// TODO Auto-generated method stub
		
	}

	public void close() {
		try {
			filereader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	public void fail(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	public void nextTuple() {
		
		if(completed) {
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		BufferedReader reader = new BufferedReader(filereader);
		String line;
		try {
			while((line = reader.readLine()) != null) {
				collector.emit(new Values(line), line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		completed = true;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
