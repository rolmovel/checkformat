package com.keedio.storm;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import static org.mockito.Mockito.*;
import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

public class testYammerMetrics {

	ServerSocket ss;
	private TCPBolt bolt;
	
	@Mock
	private TopologyContext topologyContext = mock(TopologyContext.class);

	@Mock
	private OutputCollector collector = mock(OutputCollector.class);

	@Before
	public void setUp() throws UnknownHostException, IOException {
		ss = new ServerSocket(8888);
		bolt = new TCPBolt();
		Config conf = new Config();
		conf.put("tcp.bolt.host", "localhost");
		conf.put("tcp.bolt.port", "8888");
		bolt.prepare(conf, topologyContext, collector);
	}

	@After
	public void finish() throws IOException {
		ss.close();
	}
	
	@Test
	public void testThroughput() throws InterruptedException {
		Tuple tuple = mock(Tuple.class);
	    when(tuple.getString(anyInt())).thenReturn("Output fake");
		bolt.execute(tuple);
		bolt.execute(tuple);
		bolt.execute(tuple);
		bolt.execute(tuple);
		bolt.execute(tuple);
		
		Assert.assertTrue("Se han capturado cuatro observaciones", bolt.getThroughput().getSnapshot().getValues().length == 5);
		Assert.assertTrue("La media esta por debajo de los 3 segundos", bolt.getThroughput().mean() < 1);

	}
	
	@Test
	public void testErrors() throws InterruptedException {
		Tuple tuple = mock(Tuple.class);
	    when(tuple.getString(anyInt())).thenThrow(IOException.class);
		bolt.execute(tuple);
		bolt.execute(tuple);
		
		Assert.assertTrue("Se han capturado dos errores", bolt.getErrors().count() == 2);

	}
	
	
}
