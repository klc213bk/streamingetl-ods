package com.transglobe.streamingetl.ods.consumer;

import java.util.ArrayList;
import java.util.Arrays;
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
		logger.info(">>> start run ConsumerApp");
		
		String tableName = null; // 
		if (args.length != 0) {
			tableName = args[0];
		} else {
			logger.error("Should provide an Table Name argument");
			System.exit(1);
		}
		
		String profileActive = System.getProperty("profile.active", "");
		String configFile = StringUtils.isBlank(profileActive)? CONFIG_FILE_NAME : profileActive + "/" + CONFIG_FILE_NAME;

		Config config = null;
		try {
			config = Config.getConfig(configFile);
		} catch (Exception e1) {
			logger.error(">>>message={}, stack trace={}", e1.getMessage(), ExceptionUtils.getStackTrace(e1));
			e1.printStackTrace();
			throw new RuntimeException(e1);
		}

		//String groupId = config.groupId;

		sourceConnPool = new BasicDataSource();
		sourceConnPool.setUrl(config.sourceDbUrl);
		sourceConnPool.setUsername(config.sourceDbUsername);
		sourceConnPool.setPassword(config.sourceDbPassword);
		sourceConnPool.setDriverClassName(config.sourceDbDriver);
		sourceConnPool.setMaxTotal(3);

		sinkConnPool = new BasicDataSource();
		sinkConnPool.setUrl(config.sinkDbUrl);
		sinkConnPool.setUsername(null);
		sinkConnPool.setPassword(null);
		sinkConnPool.setDriverClassName(config.sinkDbDriver);
		sinkConnPool.setMaxTotal(1);

		ExecutorService executor = Executors.newFixedThreadPool(NUM_CONSUMERS);

		final List<ConsumerLoop> consumers = new ArrayList<>();
		ConsumerLoop consumerLoop = null;
		for (int id = 0; id < NUM_CONSUMERS; id++) {
			
			if (StringUtils.equals(config.sourceTableTPolicyPrintJob, tableName)) {
				consumerLoop = new PolicyPrintJobConsumerLoop(id, config, sourceConnPool, sinkConnPool);
			} else {
				logger.error(">>>message=No match table Name:", tableName);
				throw new RuntimeException("No match table Name:"+ tableName);
			}
			consumers.add(consumerLoop);
			
			logger.info(">>>executor:{}", executor);
			executor.submit(consumerLoop);
		}
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				for (ConsumerLoop consumerLoop : consumers) {
					consumerLoop.shutdown();
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
