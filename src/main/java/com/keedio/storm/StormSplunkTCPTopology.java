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


public class StormSplunkTCPTopology {
	public static final Logger LOG = LoggerFactory
			.getLogger(StormSplunkTCPTopology.class);

	private final TopologyProperties topologyProperties;

	public StormSplunkTCPTopology(TopologyProperties topologyProperties) {
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
		String kafkaTopic = topologyProperties.getKafkaTopic();
		SpoutConfig kafkaConfig = new SpoutConfig(kafkaBrokerHosts, kafkaTopic, "/storm/kafka",	kafkaTopic);
		kafkaConfig.forceFromStart = topologyProperties.isKafkaStartFromBeginning();
		
		// NOTE: This is for avoid a bug in kafka storm config default value

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), 4);
		builder.setBolt("FilterBolt", new FilterMessageBolt(),1).shuffleGrouping("KafkaSpout");
		builder.setBolt("SplunkTCPBolt", new TCPBolt(), 1).shuffleGrouping("FilterBolt");

		return builder.createTopology();
	}
	
	public static void main(String[] args) throws Exception {
		String propertiesFile = args[0];
		System.out.println(propertiesFile);
		TopologyProperties topologyProperties = new TopologyProperties(propertiesFile);
		StormSplunkTCPTopology topology = new StormSplunkTCPTopology(topologyProperties);
		topology.runTopology();
	}
}
