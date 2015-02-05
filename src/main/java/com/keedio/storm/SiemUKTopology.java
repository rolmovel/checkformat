package com.keedio.storm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;


public class SiemUKTopology {
	public static final Logger LOG = LoggerFactory
			.getLogger(SiemUKTopology.class);

	private final TopologyProperties topologyProperties;

	public SiemUKTopology(TopologyProperties topologyProperties) {
		this.topologyProperties = topologyProperties;
	}
	
	public void runTopology() throws Exception{

		StormTopology stormTopology = buildTopology();
		String stormExecutionMode = topologyProperties.getStormExecutionMode();
	
		switch (stormExecutionMode){
			case ("cluster"):
				StormSubmitter.submitTopology(topologyProperties.getTopologyName(), topologyProperties.getStormConfig(), stormTopology);
				break;
			case ("local"):
			default:
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology(topologyProperties.getTopologyName(), topologyProperties.getStormConfig(), stormTopology);
				Thread.sleep(topologyProperties.getLocalTimeExecution());
				cluster.killTopology(topologyProperties.getTopologyName());
				cluster.shutdown();
				System.exit(0);
		}	
	}
	
	private StormTopology buildTopology()
	{
		BrokerHosts kafkaBrokerHosts = new ZkHosts(topologyProperties.getZookeeperHosts());
		SpoutConfig kafkaConfig = new SpoutConfig(kafkaBrokerHosts, topologyProperties.getKafkaTopic(), "",	"storm");
		
		//TODO : configurable!!
		kafkaConfig.forceFromStart = true;
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), 1);
		builder.setBolt("SplunkTCPBolt", new SplunkTCPBolt(), 1).shuffleGrouping("KafkaSpout");
		return builder.createTopology();
	}
}
