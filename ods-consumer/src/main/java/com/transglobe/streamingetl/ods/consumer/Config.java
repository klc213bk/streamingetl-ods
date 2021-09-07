package com.transglobe.streamingetl.ods.consumer;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
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
	public String sourceTableTProductionDetail;
	
	public String sinkDbDriver;
	public String sinkDbUrl;
	public String sinkDbUsername;
	public String sinkDbPassword;
	
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
	public String streamingEtlNameTProductionDetail;
	
	public String bootstrapServers;
	
	public String groupIdTCommisionFee;
	public String groupIdTContractExtendCx;
	public String groupIdTContractExtendLog;
	public String groupIdTContractProductLog;
	public String groupIdTImage;
	public String groupIdJbpmVariableinstance;
	public String groupIdTPolicyChange;
	public String groupIdTPolicyPrintJob;
	public String groupIdTProductCommision;
	public String groupIdTProductionDetail;
	
	public List<String> topicsTCommisionFee;
	public List<String> topicsTContractExtendCx;
	public List<String> topicsTContractExtendLog;
	public List<String> topicsTContractProductLog;
	public List<String> topicsTImage;
	public List<String> topicsJbpmVariableinstance;
	public List<String> topicsTPolicyChange;
	public List<String> topicsTPolicyPrintJob;
	public List<String> topicsTProductCommision;
	public List<String> topicsTProductionDetail;
	
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
			config.streamingEtlNameTProductionDetail = prop.getProperty("streaming.etl.name.t_production_detail");

			config.bootstrapServers = prop.getProperty("bootstrap.servers");
			
			config.groupIdTCommisionFee = prop.getProperty("group.id.t_commision_fee");
			config.groupIdTContractExtendCx = prop.getProperty("group.id.t_contract_extend_cx");
			config.groupIdTContractExtendLog = prop.getProperty("group.id.t_contract_extend_log");
			config.groupIdTContractProductLog = prop.getProperty("group.id.t_contract_product_log");
			config.groupIdTImage = prop.getProperty("group.id.t_image");
			config.groupIdJbpmVariableinstance = prop.getProperty("group.id.jbpm_variableinstance");
			config.groupIdTPolicyChange = prop.getProperty("group.id.t_policy_change");
			config.groupIdTPolicyPrintJob = prop.getProperty("group.id.t_policy_print_job");
			config.groupIdTProductCommision = prop.getProperty("group.id.t_product_commision");
			config.groupIdTProductionDetail = prop.getProperty("group.id.t_production_detail");
			
			config.topicsTCommisionFee = Arrays.asList(prop.getProperty("topics.t_commision_fee").split(","));
			config.topicsTContractExtendCx = Arrays.asList(prop.getProperty("topics.t_contract_extend_cx").split(","));
			config.topicsTContractExtendLog = Arrays.asList(prop.getProperty("topics.t_contract_extend_log").split(","));
			config.topicsTContractProductLog = Arrays.asList(prop.getProperty("topics.t_contract_product_log").split(","));
			config.topicsTImage = Arrays.asList(prop.getProperty("topics.t_image").split(","));
			config.topicsJbpmVariableinstance = Arrays.asList(prop.getProperty("topics.jbpm_variableinstance").split(","));
			config.topicsTPolicyChange = Arrays.asList(prop.getProperty("topics.t_policy_change").split(","));
			config.topicsTPolicyPrintJob = Arrays.asList(prop.getProperty("topics.t_policy_print_job").split(","));
			config.topicsTProductCommision = Arrays.asList(prop.getProperty("topics.t_product_commision").split(","));
			config.topicsTProductionDetail = Arrays.asList(prop.getProperty("topics.t_production_detail").split(","));
								
			
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
