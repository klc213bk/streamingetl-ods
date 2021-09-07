package com.transglobe.streamingetl.ods.consumer;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.transglobe.streamingetl.ods.consumer.model.ProductionDetail;

public abstract class ConsumerLoop implements Runnable {
	static final Logger logger = LoggerFactory.getLogger(ConsumerLoop.class);

	protected final KafkaConsumer<String, String> consumer;
	
	protected Config config;

	protected BasicDataSource sourceConnPool;
	protected BasicDataSource sinkConnPool;
	
	protected Date dataDate;

	public ConsumerLoop(int id,
			String groupId,  
			Config config,
			BasicDataSource sourceConnPool,
			BasicDataSource sinkConnPool) {
		this.config = config;
		this.sourceConnPool = sourceConnPool;
		this.sinkConnPool = sinkConnPool;
		Properties props = new Properties();
		props.put("bootstrap.servers", config.bootstrapServers);
		props.put("group.id", groupId);
		props.put("client.id", groupId + "-" + id );
		props.put("group.instance.id", groupId + "-mygid" );
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		props.put("session.timeout.ms", 60000 ); // 60 seconds
		props.put("max.poll.records", 50 );
		this.consumer = new KafkaConsumer<>(props);
		

	}

	
	public void shutdown() {
		consumer.wakeup();
	}

	public void close() {
		consumer.close();

		if (sourceConnPool != null) {
			try {
				sourceConnPool.close();
			} catch (SQLException e) {
				logger.error(">>>sourceConnPool close error, finally message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			}
		}
		if (sinkConnPool != null) {
			try {
				sinkConnPool.close();
			} catch (SQLException e) {
				logger.error(">>>sinkConnPool error, finally message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			}
		}
	}
	
	protected void checkConnection() {
		int tries = 0;
		while (sourceConnPool.isClosed()) {
			tries++;
			try {
				sourceConnPool.restart();

				logger.info("   >>> Source Connection Pool restart, try {} times", tries);

				Thread.sleep(10000);
			} catch (Exception e) {
				logger.error(">>> message={}, stack trace={}, record str={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			}
		}
		tries = 0;
		while (sinkConnPool.isClosed()) {
			tries++;
			try {
				sinkConnPool.restart();

				logger.info("   >>> Connection Pool restart, try {} times", tries);

				Thread.sleep(10000);
			} catch (Exception e) {
				logger.error(">>> message={}, stack trace={}, record str={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			}

		}
	}
	abstract protected void consume(ConsumerRecord<String, String> record) throws Exception;
	
	@Override
	public void run() {

		try {
			//consumer.subscribe(config.topicList);

			logger.info("   >>>>>>>>>>>>>>>>>>>>>>>> run ..........");

			while (true) {
				
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

				if (records.count() > 0) {
					checkConnection();
					
					for (ConsumerRecord<String, String> record : records) {
						
						
						Connection sourceConn = null;
						Connection sinkConn = null;
						try {	
							dataDate = new Date(System.currentTimeMillis());

							// do business logic
							consume(record);
							
						} catch(Exception e) {
							logger.error(">>>record error, message={}, stack trace={}, record str={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
						} finally {
							if (sinkConn != null) sinkConn.close();
							if (sourceConn != null) sourceConn.close();
						}
					}
					
				}


			}
		} catch (Exception e) {
			// ignore for shutdown 
			logger.error(">>>Consumer error, message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));

		} finally {
			close();
		}
	}
	


}