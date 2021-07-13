package com.transglobe.streamingetl.ods.load;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.builder.ToStringBuilder;


public class Config {
	public String sourceDbDriver;
	public String sourceDbUrl;
	public String sourceDbUsername;
	public String sourceDbPassword;
	
	public String sourceTableContractProductLog;
	public String sourceTableProductionDetail;
	
	public String healthDbDriver;
	public String healthDbUrl;
	public String healthDbUsername;
	public String healthDbPassword;
	
	public String healthTableStreamingEtlHealthCdc;
	
	public String sinkDbDriver;
	public String sinkDbUrl;

	public String sinkTableContractProductLog;
	public String sinkTableProductionDetail;
	public String sinkTableStreamingEtlHealth;
			

	public static Config getConfig(String fileName) throws Exception {
		ClassLoader loader = Thread.currentThread().getContextClassLoader();

		Properties prop = new Properties();
		try (InputStream input = loader.getResourceAsStream(fileName)) {

			// load a properties file
			prop.load(input);


			Config dbConfig = new Config();
			dbConfig.sourceDbDriver = prop.getProperty("source.db.driver");
			dbConfig.sourceDbUrl = prop.getProperty("source.db.url");
			dbConfig.sourceDbUsername = prop.getProperty("source.db.username");
			dbConfig.sourceDbPassword = prop.getProperty("source.db.password");
			
			dbConfig.sourceTableContractProductLog = prop.getProperty("source.table.contract_product_log");
			dbConfig.sourceTableProductionDetail = prop.getProperty("source.table.production_detail");
			
			dbConfig.healthDbDriver = prop.getProperty("health.db.driver");
			dbConfig.healthDbUrl = prop.getProperty("health.db.url");
			dbConfig.healthDbUsername = prop.getProperty("health.db.username");
			dbConfig.healthDbPassword = prop.getProperty("health.db.password");

			dbConfig.healthTableStreamingEtlHealthCdc = prop.getProperty("health.table.streaming_etl_health_cdc");
			
			dbConfig.sinkDbDriver = prop.getProperty("sink.db.driver");
			dbConfig.sinkDbUrl = prop.getProperty("sink.db.url");
			
			dbConfig.sinkTableContractProductLog = prop.getProperty("sink.table.contract_product_log");
			dbConfig.sinkTableProductionDetail = prop.getProperty("sink.table.production_detail");
			dbConfig.sinkTableStreamingEtlHealth = prop.getProperty("sink.table.streaming_etl_health");

			return dbConfig;
		} catch (Exception e) {
			throw e;
		} 
	}
	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}
}
