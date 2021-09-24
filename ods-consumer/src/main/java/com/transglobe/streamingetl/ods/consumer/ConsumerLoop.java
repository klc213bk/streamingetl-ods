package com.transglobe.streamingetl.ods.consumer;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.transglobe.streamingetl.ods.consumer.model.TProductionDetail;

public abstract class ConsumerLoop implements Runnable {
	static final Logger logger = LoggerFactory.getLogger(ConsumerLoop.class);

	private final AtomicBoolean closed = new AtomicBoolean(false);
	
	private final KafkaConsumer<String, String> consumer;
//	private final int id;

	//private Config config;

	private List<String> topics;
	
	protected String sinkTableName;
	
	private BasicDataSource sourceConnPool;
	private BasicDataSource sinkConnPool;

	protected Date dataDate;

	public ConsumerLoop(int id,
			String groupId, 
			List<String> topics,
			String bootstrapServers,
			String sinkTableName,
			BasicDataSource sourceConnPool,
			BasicDataSource sinkConnPool,
			Date dataDate) {
		
	//	this.config = config;
		this.topics = topics;
		this.sinkTableName = sinkTableName;
		this.sourceConnPool = sourceConnPool;
		this.sinkConnPool = sinkConnPool;
		this.dataDate = dataDate;
		
		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrapServers);
		props.put("group.id", groupId);
		props.put("client.id", groupId + "-" + id );
		props.put("group.instance.id", groupId + "-mygid" );
		props.put("enable.auto.commit", "false");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		props.put("session.timeout.ms", 60000 ); // 60 seconds
		props.put("max.poll.records", 50 );
		props.put("auto.offset.reset", "earliest" );
		this.consumer = new KafkaConsumer<>(props);
	}
	@Override
	public void run() {
		try {
			logger.info("   >>>>>>>>>>>>>>>>>>>>>>>> run ..........");			
			consumer.subscribe(topics);
			List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
			while (!closed.get()) {

				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
				for (ConsumerRecord<String, String> record : records) {
					buffer.add(record);
				}

				if (buffer.size() > 0) {

					//Connection sinkConn = null;
					//Connection sourceConn = null;
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

					process(buffer);

					consumer.commitSync();

					buffer.clear();
				}
			}
		} catch (WakeupException e) {
			// ignore excepton if closing 
			if (!closed.get()) throw e;

			logger.info(">>>ignore excepton if closing, message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));

		} finally {
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
	}
	public void process(List<ConsumerRecord<String, String>> buffer) {

		Connection sourceConn = null;
		Connection sinkConn = null;
		PreparedStatement sinkPstmt = null;
		ResultSet sinkRs = null;
		ConsumerRecord<String, String> recordEx = null;
		try {
			sourceConn = sourceConnPool.getConnection();
			sinkConn = sinkConnPool.getConnection();
			sinkConn.setAutoCommit(false);

			for (ConsumerRecord<String, String> record : buffer) {
				recordEx = record;

				ObjectMapper objectMapper = new ObjectMapper();
				objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

				JsonNode jsonNode = objectMapper.readTree(record.value());
				JsonNode payload = jsonNode.get("payload");
				//	payloadStr = payload.toString();

				String operation = payload.get("OPERATION").asText();
				String tableName = payload.get("TABLE_NAME").asText();
				BigDecimal scn = new BigDecimal(payload.get("SCN").asText());
				BigDecimal commitScn = new BigDecimal(payload.get("COMMIT_SCN").asText());
				String rowId = payload.get("ROW_ID").asText();
				//String sqlRedo = payload.get("SQL_REDO").toString();
				logger.info("   >>>offset={},operation={}, TableName={}, scn={}, commitScn={}, rowId={}", record.offset(), operation, tableName, scn, commitScn, rowId);

				String dataString = payload.get("data").toString();
				String beforeString = payload.get("before").toString();
				
				if (StringUtils.equals("INSERT", operation)) {
					insert(sinkConn, objectMapper, dataString, beforeString, scn, commitScn, rowId);
				} else if (StringUtils.equals("UPDATE", operation)) {
					update(sinkConn, objectMapper, dataString, beforeString, scn, commitScn, rowId);
				} else if (StringUtils.equals("DELETE", operation)) {
					delete(sinkConn, objectMapper, dataString, beforeString, scn, commitScn, rowId);
				}
				sinkConn.commit();
				logger.info("   >>>Done !!!");
			}
		}  catch(Exception e) {
			if (recordEx != null) {
				Map<String, Object> data = new HashMap<>();
				data.put("topic", recordEx.topic());
				data.put("partition", recordEx.partition());
				data.put("offset", recordEx.offset());
				data.put("value", recordEx.value());
				logger.error(">>>record error, message={}, stack trace={}, record str={}", e.getMessage(), ExceptionUtils.getStackTrace(e), data);
			} else {
				logger.error(">>>record error, message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			}
		} finally {
			if (sinkRs != null) {
				try {
					sinkRs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (sinkPstmt != null) {
				try {
					sinkPstmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (sinkConn != null) {
				try {
					sinkConn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (sourceConn != null) {
				try {
					sourceConn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public void shutdown() {
		closed.set(true);
		consumer.wakeup();
	}
	abstract void insert(Connection sinkConn, ObjectMapper objectMapper, String dataString, String beforeString, BigDecimal scn, BigDecimal commitScn, String rowId) throws Exception;
	abstract void update(Connection sinkConn, ObjectMapper objectMapper, String dataString, String beforeString, BigDecimal scn, BigDecimal commitScn, String rowId) throws Exception;
	abstract void delete(Connection sinkConn, ObjectMapper objectMapper, String dataString, String beforeString, BigDecimal scn, BigDecimal commitScn, String rowId) throws Exception;

}