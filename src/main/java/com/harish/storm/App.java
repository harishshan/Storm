package com.harish.storm;

import java.util.UUID;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
/**
 * Hello world!
 *
 */
public class App 
{
	public static void main( String[] args )
    {
    	try {
    		// Spring Sample example program
	    	/*ApplicationContext context = new ClassPathXmlApplicationContext("Core.xml");
	    	Spout obj = (Spout) context.getBean("helloBean");
			System.out.println(obj.getName());*/
    		BasicConfigurator.configure();
    		Logger logger = Logger.getLogger(App.class);
    		logger.info("UploadEBooks Servlet");
    		TopologyBuilder builder = new TopologyBuilder();
    		
    		String topicName = "event";
    		String zkhost = "localhost:2181";
	    	BrokerHosts hosts = new ZkHosts(zkhost);
	    	SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
	    	KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);   	
	    	builder.setSpout("spout", kafkaSpout, 2);
	    	builder.setBolt("log", new LogBolt(), 8).shuffleGrouping("spout");
	    	builder.setBolt("cassandra", new CassandraBolt(), 12).fieldsGrouping("log", new Fields("LogBolt"));
	    	Config conf = new Config();
	        conf.setDebug(true);	        
	        conf.setMaxTaskParallelism(3);
	        LocalCluster cluster = new LocalCluster();
	        cluster.submitTopology("word-count", conf, builder.createTopology());
	        Thread.sleep(10000);
	        cluster.shutdown();
	    	
	        //StormSubmitter.submitTopology("kafkaboltTest", conf, builder.createTopology());    
	        
	        
    	} catch(Exception ex){
    		System.out.println("Exception "+ex);
    	}
     }
}
