package com.transglobe.streamingetl.ods.load;

import java.io.InputStream;
import java.util.Properties;
import org.apache.commons.lang3.builder.ToStringBuilder;


public class Config {
	public String sourceDbDriver;
	public String sourceDbUrl;
	public String sourceDbUsername;
	public String sourceDbPassword;
	
	public String sourceTableSchema;
	public String sourceTableTCommisionFee;
	public String sourceTableTContractExtendCx;
	public String sourceTableTContractExtendLog;
	public String sourceTableTContractProductLog;
	public String sourceTableTImage;
	public String sourceTableJbpmVariableinstance;
	public String sourceTableTPolicyChange;
	public String sourceTableTPolicyPrintJob;
	public String sourceTableTProductCommision;
	public String sourceTableTProductionDetail;
	
	public String sinkDbDriver;
	public String sinkDbUrl;
	public String sinkDbUsername;
	public String sinkDbPassword;
	
	public String sinkTableSchema;
	public String sinkTableKCommisionFee;
	public String sinkTableKContractExtendCx;
	public String sinkTableKContractExtendLog;
	public String sinkTableKContractProductLog;
	public String sinkTableKImage;
	public String sinkTableKJbpmVariableinstance;
	public String sinkTableKPolicyChange;
	public String sinkTableKPolicyPrintJob;
	public String sinkTableKProductCommision;
	public String sinkTableKProductionDetail;
	
	public String sinkTableCreateFileKCommisionFee;
	public String sinkTableCreateFileKContractExtendCx;
	public String sinkTableCreateFileKContractExtendLog;
	public String sinkTableCreateFileKContractProductLog;
	public String sinkTableCreateFileKImage;
	public String sinkTableCreateFileKJbpmVariableinstance;
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
	public String sinkTableIndexesFileKJbpmVariableinstance;
	public String sinkTableIndexesFileKPolicyChange;
	public String sinkTableIndexesFileKPolicyPrintJob;
	public String sinkTableIndexesFileKProductCommision;
	public String sinkTableIndexesFileKProductionDetail;
	
//	public String logminerDbDriver;
//	public String logminerDbUrl;
//	public String logminerDbUsername;
//	public String logminerDbPassword;
	
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
			config.sourceTableTImage = prop.getProperty("source.table.t_image");
			config.sourceTableJbpmVariableinstance = prop.getProperty("source.table.jbpm_variableinstance");
			config.sourceTableTPolicyChange = prop.getProperty("source.table.t_policy_change");
			config.sourceTableTPolicyPrintJob = prop.getProperty("source.table.t_policy_print_job");
			config.sourceTableTProductCommision = prop.getProperty("source.table.t_product_commision");
			config.sourceTableTProductionDetail = prop.getProperty("source.table.t_production_detail");
					
			config.sinkDbDriver = prop.getProperty("sink.db.driver");
			config.sinkDbUrl = prop.getProperty("sink.db.url");
			config.sinkDbUsername = prop.getProperty("sink.db.username");
			config.sinkDbPassword = prop.getProperty("sink.db.password");
		
			config.sinkTableSchema = prop.getProperty("sink.table.schema");
			config.sinkTableKCommisionFee = prop.getProperty("sink.table.k_commision_fee");
			config.sinkTableKContractExtendCx = prop.getProperty("sink.table.k_contract_extend_cx");
			config.sinkTableKContractExtendLog = prop.getProperty("sink.table.k_contract_extend_log");
			config.sinkTableKContractProductLog = prop.getProperty("sink.table.k_contract_product_log");
			config.sinkTableKImage = prop.getProperty("sink.table.k_image");
			config.sinkTableKJbpmVariableinstance = prop.getProperty("sink.table.k_jbpm_variableinstance");
			config.sinkTableKPolicyChange = prop.getProperty("sink.table.k_policy_change");
			config.sinkTableKPolicyPrintJob = prop.getProperty("sink.table.k_policy_print_job");
			config.sinkTableKProductCommision = prop.getProperty("sink.table.k_product_commision");
			config.sinkTableKProductionDetail = prop.getProperty("sink.table.k_production_detail");
			
			config.sinkTableCreateFileKCommisionFee = prop.getProperty("sink.table.create.file.k_commision_fee");
			config.sinkTableCreateFileKContractExtendCx  = prop.getProperty("sink.table.create.file.k_contract_extend_cx");
			config.sinkTableCreateFileKContractExtendLog = prop.getProperty("sink.table.create.file.k_contract_extend_log");
			config.sinkTableCreateFileKContractProductLog = prop.getProperty("sink.table.create.file.k_contract_product_log");
			config.sinkTableCreateFileKImage = prop.getProperty("sink.table.create.file.k_image");
			config.sinkTableCreateFileKJbpmVariableinstance = prop.getProperty("sink.table.create.file.k_jbpm_variableinstance");
			config.sinkTableCreateFileKPolicyChange = prop.getProperty("sink.table.create.file.k_policy_change");
			config.sinkTableCreateFileKPolicyPrintJob = prop.getProperty("sink.table.create.file.k_policy_print_job");
			config.sinkTableCreateFileKProductCommision = prop.getProperty("sink.table.create.file.k_product_commision");
			config.sinkTableCreateFileKProductionDetail = prop.getProperty("sink.table.create.file.k_production_detail");
			
			config.sinkTableIndexesFileKCommisionFee = prop.getProperty("sink.table.indexes.file.k_commision_fee");
			config.sinkTableIndexesFileKContractExtendCx = prop.getProperty("sink.table.indexes.file.k_contract_extend_cx");
			config.sinkTableIndexesFileKContractExtendLog = prop.getProperty("sink.table.indexes.file.k_contract_extend_log");
			config.sinkTableIndexesFileKContractProductLog = prop.getProperty("sink.table.indexes.file.k_contract_product_log");
			config.sinkTableIndexesFileKImage = prop.getProperty("sink.table.indexes.file.k_image");
			config.sinkTableIndexesFileKJbpmVariableinstance = prop.getProperty("sink.table.indexes.file.k_jbpm_variableinstance");
			config.sinkTableIndexesFileKPolicyChange = prop.getProperty("sink.table.indexes.file.k_policy_change");
			config.sinkTableIndexesFileKPolicyPrintJob = prop.getProperty("sink.table.indexes.file.k_policy_print_job");
			config.sinkTableIndexesFileKProductCommision = prop.getProperty("sink.table.indexes.file.k_product_commision");
			config.sinkTableIndexesFileKProductionDetail = prop.getProperty("sink.table.indexes.file.k_production_detail");
			
//			config.logminerDbDriver = prop.getProperty("logminer.db.driver");
//			config.logminerDbUrl = prop.getProperty("logminer.db.url");
//			config.logminerDbUsername = prop.getProperty("logminer.db.username");
//			config.logminerDbPassword = prop.getProperty("logminer.db.password");
					
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
