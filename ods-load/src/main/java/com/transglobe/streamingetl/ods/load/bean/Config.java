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
	
	public String sinkTableCreateFileKCommisionFee;
	public String sinkTableCreateFileKContractExtendCx;
	public String sinkTableCreateFileKContractExtendLog;
	public String sinkTableCreateFileKContractProductLog;
	public String sinkTableCreateFileKImage;
	public String sinkTableCreateFileJbpmVariableinstance;
	public String sinkTableCreateFileKPolicyChange;
	public String sinkTableCreateFileKPolicyPrintJob;
	public String sinkTableCreateFileKProductCommision;
	public String sinkTableCreateFileKProductionDetail;
	public String sinkTableCreateFileSupplLogSync;
	
	public String sinkTableIndexesFileKCommisionFee;
	public String sinkTableIndexesFileKContractExtendCx;
	public String sinkTableIndexesFileKContractExtendLog;
	public String sinkTableIndexesFileKContractProductLog;
	public String sinkTableIndexesFileKImage;
	public String sinkTableIndexesFileJbpmVariableinstance;
	public String sinkTableIndexesFileKPolicyChange;
	public String sinkTableIndexesFileKPolicyPrintJob;
	public String sinkTableIndexesFileKProductCommision;
	public String sinkTableIndexesFileKProductionDetail;
	public String sinkTableIndexesFileSupplLogSync; 
	
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
	
	public static Config getConfig(String fileName) throws Exception {
		ClassLoader loader = Thread.currentThread().getContextClassLoader();

		Properties prop = new Properties();
		try (InputStream input = loader.getResourceAsStream(fileName)) {

			// load a properties file
			prop.load(input);

			Config config = new Config();
			config.sourceDbDriver = prop.getProperty("source.db.driver");
			config.sourceDbUrl = prop.getProperty("source.db.url");
			config.sourceDbUsername = prop.getProperty("source.db.username");
			config.sourceDbPassword = prop.getProperty("source.db.password");
			
			config.sourceTableTCommisionFee = prop.getProperty("source.table.t_commision_fee");
			config.sourceTableTContractExtendCx = prop.getProperty("source.table.t_contract_extend_cx");
			config.sourceTableTContractExtendLog = prop.getProperty("source.table.t_contract_extend_log");
			config.sourceTableTContractProductLog = prop.getProperty("source.table.t_c_ontract_product_log");
			config.sourceTableTImage = prop.getProperty("source.table.t_image=T_IMAGE");
			config.sourceTableJbpmVariableinstance = prop.getProperty("source.table.jbpm_variableinstance");
			config.sourceTableTPolicyChange = prop.getProperty("source.table.t_policy_change");
			config.sourceTableTPolicyPrintJob = prop.getProperty("source.table.t_policy_print_job");
			config.sourceTableTProductCommision = prop.getProperty("source.table.t_product_commision");
			config.sourceTableTProductDetail = prop.getProperty("source.table.t_production_detail");
					
			config.sinkDbDriver = prop.getProperty("sink.db.driver");
			config.sinkDbUrl = prop.getProperty("sink.db.url");
			config.sinkDbUsername = prop.getProperty("sink.db.username");
			config.sinkDbPassword = prop.getProperty("sink.db.password");
		
			config.sinkTableTCommisionFee = prop.getProperty("sink.table.t_commision_fee");
			config.sinkTableTContractExtendCx = prop.getProperty("sink.table.t_contract_extend_cx");
			config.sinkTableTContractExtendLog = prop.getProperty("sink.table.t_contract_extend_log");
			config.sinkTableTContractProductLog = prop.getProperty("sink.table.t_contract_product_log");
			config.sinkTableTImage = prop.getProperty("sink.table.t_image=K_IMAGE");
			config.sinkTableJbpmVariableinstance = prop.getProperty("sink.table.jbpm_variableinstance");
			config.sinkTableTPolicyChange = prop.getProperty("sink.table.t_policy_change");
			config.sinkTableTPolicyPrintJob = prop.getProperty("sink.table.t_policy_print_job");
			config.sinkTableTProductCommision = prop.getProperty("sink.table.t_product_commision");
			config.sinkTableTProductDetail = prop.getProperty("sink.table.t_production_detail");
			config.sinkTableSupplLogSync = prop.getProperty("sink.table.suppl_log_sync");
			
			config.sinkTableCreateFileKCommisionFee = prop.getProperty("sink.table.create.file.k_commision_fee");
			config.sinkTableCreateFileKContractExtendCx  = prop.getProperty("sink.table.create.file.k_contract_extend_cx");
			config.sinkTableCreateFileKContractExtendLog = prop.getProperty("sink.table.create.file.k_contract_extend_log");
			config.sinkTableCreateFileKContractProductLog = prop.getProperty("sink.table.create.file.k_contract_product_log");
			config.sinkTableCreateFileKImage = prop.getProperty("sink.table.create.file.k_image");
			config.sinkTableCreateFileJbpmVariableinstance = prop.getProperty("sink.table.create.file.k_jbpm_variableinstance");
			config.sinkTableCreateFileKPolicyChange = prop.getProperty("sink.table.create.file.k_policy_change");
			config.sinkTableCreateFileKPolicyPrintJob = prop.getProperty("sink.table.create.file.k_policy_print_job");
			config.sinkTableCreateFileKProductCommision = prop.getProperty("sink.table.create.file.k_product_commision");
			config.sinkTableCreateFileKProductionDetail = prop.getProperty("sink.table.create.file.k_production_detail");
			config.sinkTableCreateFileSupplLogSync = prop.getProperty("sink.table.create.file.suppl_log_sync");

			config.sinkTableIndexesFileKCommisionFee = prop.getProperty("sink.table.indexes.file.k_commision_fee");
			config.sinkTableIndexesFileKContractExtendCx = prop.getProperty("sink.table.indexes.file.k_contract_extend_cx");
			config.sinkTableIndexesFileKContractExtendLog = prop.getProperty("sink.table.indexes.file.k_contract_extend_log");
			config.sinkTableIndexesFileKContractProductLog = prop.getProperty("sink.table.indexes.file.k_contract_product_log");
			config.sinkTableIndexesFileKImage = prop.getProperty("sink.table.indexes.file.k_image");
			config.sinkTableIndexesFileJbpmVariableinstance = prop.getProperty("sink.table.indexes.file.k_jbpm_variableinstance");
			config.sinkTableIndexesFileKPolicyChange = prop.getProperty("sink.table.indexes.file.k_policy_change");
			config.sinkTableIndexesFileKPolicyPrintJob = prop.getProperty("sink.table.indexes.file.k_policy_print_job");
			config.sinkTableIndexesFileKProductCommision = prop.getProperty("sink.table.indexes.file.k_product_commision");
			config.sinkTableIndexesFileKProductionDetail = prop.getProperty("sink.table.indexes.file.k_production_detail");
			config.sinkTableIndexesFileSupplLogSync = prop.getProperty("sink.table.indexes.file.suppl_log_sync");
			
			config.logminerDbDriver = prop.getProperty("logminer.db.driver");
			config.logminerDbUrl = prop.getProperty("logminer.db.url");
			config.logminerDbUsername = prop.getProperty("logminer.db.username");
			config.logminerDbPassword = prop.getProperty("logminer.db.password");
			
			config.streamingEtlNameTCommisionFee = prop.getProperty("streaming.etl.name.t_commission_fee");

			config.streamingEtlNameTContractExtendCx = prop.getProperty("streaming.etl.name.t_contract_extend_cx");
			config.streamingEtlNameTContractExtendLog = prop.getProperty("streaming.etl.name.t_contract_extend_log");
			config.streamingEtlNameTContractProductLog = prop.getProperty("streaming.etl.name.t_contract_product_log");
			config.streamingEtlNameTImage= prop.getProperty("streaming.etl.name.t_image");
			config.streamingEtlNameJbpmVariableinstance = prop.getProperty("streaming.etl.name.jbpm_variableinstance");
			config.streamingEtlNameTPolicyChange = prop.getProperty("streaming.etl.name.t_policy_change");
			config.streamingEtlNameTPolicyPrintJob = prop.getProperty("streaming.etl.name.t_policy_print_job");
			config.streamingEtlNameTProductCommision = prop.getProperty("streaming.etl.name.t_product_commision");
			config.streamingEtlNameTProductDetail = prop.getProperty("streaming.etl.name.t_production_detail");
				
					
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
