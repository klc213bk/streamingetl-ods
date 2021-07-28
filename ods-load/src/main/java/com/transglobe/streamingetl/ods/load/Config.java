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
	
	public String sourceTableProductionDetail;
	
	public String sinkDbDriver;
	public String sinkDbUrl;

	public String sinkTableProductionDetail;
	public String sinkTableSupplLogSync;
	
	public String logminerDbDriver;
	public String logminerDbUrl;
	public String logminerDbUsername;
	public String logminerDbPassword;
	
	public String logminerTableLogminerScn;
	
	public String streamingName;
	
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

			dbConfig.sourceTableProductionDetail = prop.getProperty("source.table.production_detail");
	
			dbConfig.sinkDbDriver = prop.getProperty("sink.db.driver");
			dbConfig.sinkDbUrl = prop.getProperty("sink.db.url");
			
			dbConfig.sinkTableProductionDetail = prop.getProperty("sink.table.production_detail");
			dbConfig.sinkTableSupplLogSync = prop.getProperty("sink.table.suppl_log_sync");
			
			dbConfig.logminerDbDriver = prop.getProperty("logminer.db.driver");
			dbConfig.logminerDbUrl = prop.getProperty("logminer.db.url");
			dbConfig.logminerDbUsername = prop.getProperty("logminer.db.username");
			dbConfig.logminerDbPassword = prop.getProperty("logminer.db.password");
			
			dbConfig.logminerTableLogminerScn = prop.getProperty("logminer.table.logminer_scn");
			
			dbConfig.streamingName = prop.getProperty("streaming.name");
			
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
