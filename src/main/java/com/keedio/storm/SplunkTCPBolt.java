package com.keedio.storm;

//import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class SplunkTCPBolt implements IBasicBolt {
	public static final Logger LOG = LoggerFactory
			.getLogger(SiemUKTopology.class);
	
	private Socket socket;
	//DataInputStream input;
	DataOutputStream output;
	
	@Override
	public void cleanup() {
		try {
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		String host = (String) stormConf.get("splunk.host");
		int port = Integer.parseInt((String) stormConf.get("splunk.port"));
		
		LOG.info("Conectando con socket");
		
		try {
			socket = new Socket(host,port);
			//input = new DataInputStream(socket.getInputStream());
			output = new DataOutputStream(socket.getOutputStream());
			LOG.info("Conectado");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//declarer.declare(new Fields("Message"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		try {
			LOG.info("EXECUTE");
			output.writeBytes(new String(input.getBinary(0)));
			LOG.info("Despues de writeBytes");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
