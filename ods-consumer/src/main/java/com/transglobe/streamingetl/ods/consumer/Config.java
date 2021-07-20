package com.transglobe.streamingetl.ods.consumer;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.builder.ToStringBuilder;


public class Config {

	public String sourceTableProductionDetail;
	public String sourceTableStreamingEtlHealthCdc;
	
	public String sourceDbDriver;
	public String sourceDbUrl;
	public String sourceDbUsername;
	public String sourceDbPassword;
	
	public String sinkDbDriver;
	public String sinkDbUrl;
//	public String sinkDbUsername;
//	public String sinkDbPassword;
	
	public String sinkTableProductionDetail;
	public String sinkTableStreamingEtlHealth;
	
	public String bootstrapServers;
	public String groupId;
	public List<String> topicList;
	
	public static Config getConfig(String fileName) throws Exception {
		ClassLoader loader = Thread.currentThread().getContextClassLoader();

		Properties prop = new Properties();
		try (InputStream input = loader.getResourceAsStream(fileName)) {

			// load a properties file
			prop.load(input);


			Config config = new Config();
			config.sourceTableProductionDetail = prop.getProperty("source.table.production_detail");
			
			config.sourceTableStreamingEtlHealthCdc = prop.getProperty("source.table.streaming.etl.health.cdc");

			config.sourceDbDriver = prop.getProperty("source.db.driver");
			config.sourceDbUrl = prop.getProperty("source.db.url");
			config.sourceDbUsername = prop.getProperty("source.db.username");
			config.sourceDbPassword = prop.getProperty("source.db.password");
					
			config.sinkDbDriver = prop.getProperty("sink.db.driver");
			config.sinkDbUrl = prop.getProperty("sink.db.url");
//			config.sinkDbUsername = prop.getProperty("sink.db.username");
//			config.sinkDbPassword = prop.getProperty("sink.db.password");
			
			config.sinkTableProductionDetail = prop.getProperty("sink.table.production_detail");
			config.sinkTableStreamingEtlHealth=prop.getProperty("sink.table.streaming_etl_health");
					
			config.bootstrapServers = prop.getProperty("bootstrap.servers");
			config.groupId = prop.getProperty("group.id");
			String[] topicArr = prop.getProperty("topics").split(",");
			config.topicList = Arrays.asList(topicArr);
			
			return config;
		} catch (Exception e) {
			throw e;
		} 
	}
	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}
}
