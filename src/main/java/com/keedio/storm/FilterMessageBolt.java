package com.keedio.storm;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import static backtype.storm.utils.Utils.tuple;

public class FilterMessageBolt implements IBasicBolt {

	public static final Logger LOG = LoggerFactory
			.getLogger(FilterMessageBolt.class);

	String allowMessages, denyMessages;
		
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tupleValue"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		allowMessages = (String) stormConf.get("filter.bolt.allow");
		denyMessages = (String) stormConf.get("filter.bolt.deny");
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		
		LOG.debug("FilterMessage: execute");
		
		String message = new String(input.getBinary(0));
		
		if (!allowMessages.isEmpty()){
			Pattern patternAllow = Pattern.compile(allowMessages);
			Matcher matcherAllow = patternAllow.matcher(message);
			if (matcherAllow.matches()) {
				LOG.debug("Emiting tuple(allowed): " + message.toString());
				collector.emit(tuple(message));
			}else{
				LOG.debug("NOT Emiting tuple(not allowed): " + message.toString());
			}
			
		}
		else if (!denyMessages.isEmpty()){
			Pattern patternDeny = Pattern.compile(denyMessages);
			Matcher matcherDeny = patternDeny.matcher(message);
			if (!matcherDeny.matches()) {
				LOG.debug("Emiting tuple(not denied): " + message.toString());
				collector.emit(tuple(message));
			}else
				LOG.debug("NOT Emiting tuple(denied): " + message.toString());
		}
		else{
			LOG.debug("Emiting tuple(no filter): " + message.toString());
			collector.emit(tuple(message));
		}

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

}
