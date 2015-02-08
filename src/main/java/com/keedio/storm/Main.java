package com.keedio.storm;

public class Main {


	public static void main(String[] args) throws Exception {
		String propertiesFile = args[0];
		System.out.println(propertiesFile);
		TopologyProperties topologyProperties = new TopologyProperties(propertiesFile);
		StormSplunkTCPTopology topology = new StormSplunkTCPTopology(topologyProperties);
		topology.runTopology();
	}
}