package com.keedio.storm;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketException;
import java.util.Date;
import java.util.Map;

import backtype.storm.metric.api.*;

import com.github.staslev.storm.metrics.yammer.StormYammerMetricsAdapter;
import com.github.staslev.storm.metrics.yammer.YammerFacadeMetric;
import com.keedio.storm.metric.ThroughputReducer;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricsRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class TCPBolt extends BaseRichBolt {

	private static final long serialVersionUID = 8831211985061474513L;

	public static final Logger LOG = LoggerFactory
			.getLogger(TCPBolt.class);
	
	private Socket socket;
	private DataOutputStream output;
	private String host;
	private int port;
	private OutputCollector collector;
    private Date lastExecution = new Date();
    
    // Declaramos el adaptador y las metricas de yammer
    private StormYammerMetricsAdapter yammerAdapter;
	private Counter errors;
    private Histogram throughput;
	
    public Counter getErrors() {
		return errors;
	}

	public void setErrors(Counter errors) {
		this.errors = errors;
	}

	public Histogram getThroughput() {
		return throughput;
	}

	public void setThroughput(Histogram throughput) {
		this.throughput = throughput;
	}


	@Override
	public void cleanup() {
		try {
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		loadBoltProperties(stormConf);
		connectToHost();
		this.collector = collector;
        
		// Tiempo de notificacion de metricas en los diferentes bolts
        //stormConf.put(YammerFacadeMetric.FACADE_METRIC_TIME_BUCKET_IN_SEC, 10);
        
        yammerAdapter = StormYammerMetricsAdapter.configure(stormConf, context, new MetricsRegistry());
        errors = yammerAdapter.createCounter("error", "");
        throughput = yammerAdapter.createHistogram("throughput", "", false);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public void execute(Tuple input) {
		try {
			output.writeBytes(input.getBinary(0) + "\n");
            collector.ack(input);

            // Añadimos al throughput e inicializamos el date
            Date actualDate = new Date();
            long aux = (actualDate.getTime() - lastExecution.getTime())/1000;
            lastExecution = actualDate;
            
            // Registramos para calculo de throughput
            throughput.update(aux);
            
		} catch (SocketException se){
            errors.inc();
            collector.reportError(se);
            collector.fail(input);
			LOG.error("Connection with server lost");
			connectToHost();
		} catch (IOException e) {
			collector.reportError(e);
			collector.fail(input);
            errors.inc();
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("rawtypes")
	private void loadBoltProperties(Map stormConf){
		host = (String) stormConf.get("tcp.bolt.host");
		try {
			port = Integer.parseInt((String) stormConf.get("tcp.bolt.port"));
		}catch (NumberFormatException e){
			LOG.error("Error parsing tcp bolt from config file");
			e.printStackTrace();
			throw new NumberFormatException();
		}
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
					if (retryDelay < 60)
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
