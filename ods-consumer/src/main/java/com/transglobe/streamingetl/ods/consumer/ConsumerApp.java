package com.transglobe.streamingetl.ods.consumer;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerApp {
	static final Logger logger = LoggerFactory.getLogger(ConsumerApp.class);

	private static final String CONFIG_FILE_NAME = "config.properties";
	
	private static final int NUM_CONSUMERS = 1;

	private static BasicDataSource sinkConnPool;
	private static BasicDataSource sourceConnPool;

	public static void main(String[] args) { 
		
		String profileActive = System.getProperty("profile.active", "");
		
		String configFile = StringUtils.isBlank(profileActive)? CONFIG_FILE_NAME : profileActive + "/" + CONFIG_FILE_NAME;

		Config config = null;
		try {
			config = Config.getConfig(configFile);
		} catch (Exception e1) {
			logger.error(">>>message={}, stack trace={}", e1.getMessage(), ExceptionUtils.getStackTrace(e1));
			e1.printStackTrace();
		}
		
		
		String groupId = null;
		List<String> topics = null;
		Date dataDate = null;
		if (args.length == 2) {
			dataDate = Date.valueOf(args[1]);
			if (StringUtils.equals(config.sourceTableTCommisionFee, args[0])) {
				groupId = config.groupIdTCommisionFee;
				topics = config.topicsTCommisionFee;
//				ConsumerLoop consumer = new ConsumerLoop((i + 1), groupId, topics, config.bootstrapServers, 
//						sourceConnPool, sinkConnPool);
			} else if (StringUtils.equals(config.sourceTableTContractExtendCx, args[0])) {
				groupId = config.groupIdTContractExtendCx;
				topics = config.topicsTContractExtendCx;
			} else if (StringUtils.equals(config.sourceTableTContractExtendLog, args[0])) {
				groupId = config.groupIdTContractExtendLog;
				topics = config.topicsTContractExtendLog;
			} else if (StringUtils.equals(config.sourceTableTContractProductLog, args[0])) {
				groupId = config.groupIdTContractProductLog;
				topics = config.topicsTContractProductLog;
			} else if (StringUtils.equals(config.sourceTableTImage, args[0])) {
				groupId = config.groupIdTImage;
				topics = config.topicsTImage;
			} else if (StringUtils.equals(config.sourceTableJbpmVariableinstance, args[0])) {
				groupId = config.groupIdJbpmVariableinstance;
				topics = config.topicsJbpmVariableinstance;
			} else if (StringUtils.equals(config.sourceTableTPolicyChange, args[0])) {
				groupId = config.groupIdTPolicyChange;
				topics = config.topicsTPolicyChange;
			} else if (StringUtils.equals(config.sourceTableTPolicyPrintJob, args[0])) {
				groupId = config.groupIdTPolicyPrintJob;
				topics = config.topicsTPolicyPrintJob;
			} else if (StringUtils.equals(config.sourceTableTProductCommision, args[0])) {
				groupId = config.groupIdTProductCommision;
				topics = config.topicsTProductCommision;
			} else if (StringUtils.equals(config.sourceTableTProductionDetail, args[0])) {
				groupId = config.groupIdTProductionDetail;
				topics = config.topicsTProductionDetail;
			} else {
				throw new RuntimeException ("Invalid input");
			}
		} else {
			throw new RuntimeException ("Please provide input ");
		}

		sourceConnPool = new BasicDataSource();
		sourceConnPool.setUrl(config.sourceDbUrl);
		sourceConnPool.setUsername(config.sourceDbUsername);
		sourceConnPool.setPassword(config.sourceDbPassword);
		sourceConnPool.setDriverClassName(config.sourceDbDriver);
		sourceConnPool.setMaxTotal(3);

		sinkConnPool = new BasicDataSource();
		sinkConnPool.setUrl(config.sinkDbUrl);
		sinkConnPool.setDriverClassName(config.sinkDbDriver);
		sinkConnPool.setMaxTotal(3);

		ExecutorService executor = Executors.newFixedThreadPool(NUM_CONSUMERS);

		final List<ConsumerLoop> consumers = new ArrayList<>();
		//		String groupId1 = config.groupId1;
		for (int i = 1; i <= NUM_CONSUMERS; i++) {
			ConsumerLoop consumer = new PolicyPrintJobConsumerLoop(i, config, 
					sourceConnPool, sinkConnPool, dataDate);
			consumers.add(consumer);
			executor.submit(consumer);
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				for (ConsumerLoop consumer : consumers) {
					consumer.shutdown();
				} 

				try {
					if (sourceConnPool != null) sourceConnPool.close();
					if (sinkConnPool != null) sinkConnPool.close();
				} catch (Exception e) {
					logger.error(">>>message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
				}

				executor.shutdown();
				try {
					executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					e.printStackTrace();
					logger.error(">>>message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));

				}
			}
		});
	}

}