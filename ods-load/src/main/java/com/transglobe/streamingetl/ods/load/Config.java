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
	
	public String sourceTableCommisionFee;
	public String sourceTableContractExtendCx;
	public String sourceTableContractExtendLog;
	public String sourceTableContractProductLog;
	public String sourceTableImage;
	public String sourceTableJbpmVariableinstance;
	public String sourceTablePolicyChange;
	public String sourceTablePolicyPrintJob;
	public String sourceTableProductCommision;
	public String sourceTableProductionDetail;
	
	public String sinkDbDriver;
	public String sinkDbUrl;
	public String sinkDbUsername;
	public String sinkDbPassword;

	public String sinkTableCommisionFee;
	public String sinkTableContractExtendCx;
	public String sinkTableContractExtendLog;
	public String sinkTableContractProductLog;
	public String sinkTableImage;
	public String sinkTableJbpmVariableinstance;
	public String sinkTablePolicyChange;
	public String sinkTablePolicyPrintJob;
	public String sinkTableProductCommision;
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

			dbConfig.sourceTableCommisionFee = prop.getProperty("source.table.commision_fee");
			dbConfig.sourceTableContractExtendCx  = prop.getProperty("source.table.contract_extend_cx");
			dbConfig.sourceTableContractExtendLog = prop.getProperty("source.table.contract_extend_log");
			dbConfig.sourceTableContractProductLog = prop.getProperty("source.table.contract_product_log");
			dbConfig.sourceTableImage = prop.getProperty("source.table.image");
			dbConfig.sourceTableJbpmVariableinstance = prop.getProperty("source.table.jbpm_variableinstance");
			dbConfig.sourceTablePolicyChange = prop.getProperty("source.table.policy_change");
			dbConfig.sourceTablePolicyPrintJob = prop.getProperty("source.table.policy_print_job");
			dbConfig.sourceTableProductCommision = prop.getProperty("source.table.product_commision");
			dbConfig.sourceTableProductionDetail = prop.getProperty("source.table.production_detail");
			
			dbConfig.sinkDbDriver = prop.getProperty("sink.db.driver");
			dbConfig.sinkDbUrl = prop.getProperty("sink.db.url");
			dbConfig.sinkDbUsername = prop.getProperty("sink.db.username");
			dbConfig.sinkDbPassword = prop.getProperty("sink.db.password");
			
			dbConfig.sinkTableCommisionFee = prop.getProperty("sink.table.commision_fee");
			dbConfig.sinkTableContractExtendCx  = prop.getProperty("sink.table.contract_extend_cx");
			dbConfig.sinkTableContractExtendLog = prop.getProperty("sink.table.contract_extend_log");
			dbConfig.sinkTableContractProductLog = prop.getProperty("sink.table.contract_product_log");
			dbConfig.sinkTableImage = prop.getProperty("sink.table.image");
			dbConfig.sinkTableJbpmVariableinstance = prop.getProperty("sink.table.jbpm_variableinstance");
			dbConfig.sinkTablePolicyChange = prop.getProperty("sink.table.policy_change");
			dbConfig.sinkTablePolicyPrintJob = prop.getProperty("sink.table.policy_print_job");
			dbConfig.sinkTableProductCommision = prop.getProperty("sink.table.product_commision");
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
