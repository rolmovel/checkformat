package com.keedio.storm;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class TCPBolt implements IBasicBolt {

	private static final long serialVersionUID = 8831211985061474513L;

	public static final Logger LOG = LoggerFactory
			.getLogger(TCPBolt.class);
	
	private Socket socket;
	private DataOutputStream output;
	private String host;
	private int port;
	
	@Override
	public void cleanup() {
		try {
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		loadBoltProperties(stormConf);
		connectToHost();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		try {
			output.writeBytes(new String(input.getBinary(0)) + "\n");		
		} catch (SocketException se){
			LOG.error("Connection with server lost");
			connectToHost();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("rawtypes")
	private void loadBoltProperties(Map stormConf){
		host = (String) stormConf.get("tcp.bolt.host");
		port = Integer.parseInt((String) stormConf.get("tcp.bolt.port"));
	}
	
	private void connectToHost(){
		
		int retryDelay=1;
		boolean connected = false;
		
		while (!connected){
			try {
				LOG.info("Trying to establish connection with host: "+host+" port: "+port);
				socket = new Socket(host,port);
				output = new DataOutputStream(socket.getOutputStream());
				connected = true;
			}
			catch (ConnectException e){
				LOG.warn("Error establising TCP connection with host: "+host+" port: "+port);
				try{			
					Thread.sleep(retryDelay*1000);
					if (retryDelay < 120)
							retryDelay*=2;
					continue;
				}
				catch (InterruptedException ie){
					ie.printStackTrace();
				}
			} 
			catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
		
}
