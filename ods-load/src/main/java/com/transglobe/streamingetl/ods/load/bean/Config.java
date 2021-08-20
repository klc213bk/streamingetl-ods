package com.transglobe.streamingetl.ods.load.bean;

import java.io.InputStream;
import java.util.Properties;
import org.apache.commons.lang3.builder.ToStringBuilder;


public class Config {
	public String sourceDbDriver;
	public String sourceDbUrl;
	public String sourceDbUsername;
	public String sourceDbPassword;
	
	public String sourceTableTCommisionFee;
	public String sourceTableTContractExtendCx;
	public String sourceTableTContractExtendLog;
	public String sourceTableTContractProductLog;
	public String sourceTableTImage;
	public String sourceTableJbpmVariableinstance;
	public String sourceTableTPolicyChange;
	public String sourceTableTPolicyPrintJob;
	public String sourceTableTProductCommision;
	public String sourceTableTProductDetail;
	
	public String sinkDbDriver;
	public String sinkDbUrl;
	public String sinkDbUsername;
	public String sinkDbPassword;
	
	public String sinkTableTCommisionFee;
	public String sinkTableTContractExtendCx;
	public String sinkTableTContractExtendLog;
	public String sinkTableTContractProductLog;
	public String sinkTableTImage;
	public String sinkTableJbpmVariableinstance;
	public String sinkTableTPolicyChange;
	public String sinkTableTPolicyPrintJob;
	public String sinkTableTProductCommision;
	public String sinkTableTProductDetail;
	
	public String sinkTableSupplLogSync;
	
	public String logminerDbDriver;
	public String logminerDbUrl;
	public String logminerDbUsername;
	public String logminerDbPassword;
	
	public String streamingEtlNameTCommisionFee;
	public String streamingEtlNameTContractExtendCx;
	public String streamingEtlNameTContractExtendLog;
	public String streamingEtlNameTContractProductLog;
	public String streamingEtlNameTImage;
	public String streamingEtlNameJbpmVariableinstance;
	public String streamingEtlNameTPolicyChange;
	public String streamingEtlNameTPolicyPrintJob;
	public String streamingEtlNameTProductCommision;
	public String streamingEtlNameTProductDetail;
	
	public String sinkTableCreateFileKPolicyPrintJob;
	
	public String sinkTableIndexesFileKPolicyPrintJob;
	
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
			
			dbConfig.sourceTableTCommisionFee = prop.getProperty("source.table.t_commision_fee");
			dbConfig.sourceTableTContractExtendCx = prop.getProperty("source.table.t_contract_extend_cx");
			dbConfig.sourceTableTContractExtendLog = prop.getProperty("source.table.t_contract_extend_log");
			dbConfig.sourceTableTContractProductLog = prop.getProperty("source.table.t_c_ontract_product_log");
			dbConfig.sourceTableTImage = prop.getProperty("source.table.t_image=T_IMAGE");
			dbConfig.sourceTableJbpmVariableinstance = prop.getProperty("source.table.jbpm_variableinstance");
			dbConfig.sourceTableTPolicyChange = prop.getProperty("source.table.t_policy_change");
			dbConfig.sourceTableTPolicyPrintJob = prop.getProperty("source.table.t_policy_print_job");
			dbConfig.sourceTableTProductCommision = prop.getProperty("source.table.t_product_commision");
			dbConfig.sourceTableTProductDetail = prop.getProperty("source.table.t_production_detail");
					
			dbConfig.sinkDbDriver = prop.getProperty("sink.db.driver");
			dbConfig.sinkDbUrl = prop.getProperty("sink.db.url");
			dbConfig.sinkDbUsername = prop.getProperty("sink.db.username");
			dbConfig.sinkDbPassword = prop.getProperty("sink.db.password");
		
			dbConfig.sinkTableTCommisionFee = prop.getProperty("sink.table.t_commision_fee");
			dbConfig.sinkTableTContractExtendCx = prop.getProperty("sink.table.t_contract_extend_cx");
			dbConfig.sinkTableTContractExtendLog = prop.getProperty("sink.table.t_contract_extend_log");
			dbConfig.sinkTableTContractProductLog = prop.getProperty("sink.table.t_contract_product_log");
			dbConfig.sinkTableTImage = prop.getProperty("sink.table.t_image=K_IMAGE");
			dbConfig.sinkTableJbpmVariableinstance = prop.getProperty("sink.table.jbpm_variableinstance");
			dbConfig.sinkTableTPolicyChange = prop.getProperty("sink.table.t_policy_change");
			dbConfig.sinkTableTPolicyPrintJob = prop.getProperty("sink.table.t_policy_print_job");
			dbConfig.sinkTableTProductCommision = prop.getProperty("sink.table.t_product_commision");
			dbConfig.sinkTableTProductDetail = prop.getProperty("sink.table.t_production_detail");
					
			dbConfig.sinkTableSupplLogSync = prop.getProperty("sink.table.suppl_log_sync");
			
			dbConfig.logminerDbDriver = prop.getProperty("logminer.db.driver");
			dbConfig.logminerDbUrl = prop.getProperty("logminer.db.url");
			dbConfig.logminerDbUsername = prop.getProperty("logminer.db.username");
			dbConfig.logminerDbPassword = prop.getProperty("logminer.db.password");
			
			dbConfig.streamingEtlNameTCommisionFee = prop.getProperty("streaming.etl.name.t_commission_fee");

			dbConfig.streamingEtlNameTContractExtendCx = prop.getProperty("streaming.etl.name.t_contract_extend_cx");
			dbConfig.streamingEtlNameTContractExtendLog = prop.getProperty("streaming.etl.name.t_contract_extend_log");
			dbConfig.streamingEtlNameTContractProductLog = prop.getProperty("streaming.etl.name.t_contract_product_log");
			dbConfig.streamingEtlNameTImage= prop.getProperty("streaming.etl.name.t_image");
			dbConfig.streamingEtlNameJbpmVariableinstance = prop.getProperty("streaming.etl.name.jbpm_variableinstance");
			dbConfig.streamingEtlNameTPolicyChange = prop.getProperty("streaming.etl.name.t_policy_change");
			dbConfig.streamingEtlNameTPolicyPrintJob = prop.getProperty("streaming.etl.name.t_policy_print_job");
			dbConfig.streamingEtlNameTProductCommision = prop.getProperty("streaming.etl.name.t_product_commision");
			dbConfig.streamingEtlNameTProductDetail = prop.getProperty("streaming.etl.name.t_production_detail");
				
			dbConfig.sinkTableCreateFileKPolicyPrintJob = prop.getProperty("sink.table.create.file.k_policy_print_job");
			
			dbConfig.sinkTableIndexesFileKPolicyPrintJob = prop.getProperty("sink.table.indexes.file.k_policy_print_job");
					
			
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
