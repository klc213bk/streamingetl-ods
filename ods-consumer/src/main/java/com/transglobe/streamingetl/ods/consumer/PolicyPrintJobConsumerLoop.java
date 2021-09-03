package com.transglobe.streamingetl.ods.consumer;

import java.sql.Connection;
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

public class PolicyPrintJobConsumerLoop implements Runnable {
	static final Logger logger = LoggerFactory.getLogger(PolicyPrintJobConsumerLoop.class);

	private final KafkaConsumer<String, String> consumer;
	
	private Config config;

	private BasicDataSource sourceConnPool;
	private BasicDataSource sinkConnPool;

	public PolicyPrintJobConsumerLoop(int id,
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

	@Override
	public void run() {

		try {
			consumer.subscribe(config.topicList);

			logger.info("   >>>>>>>>>>>>>>>>>>>>>>>> run ..........");

			while (true) {
				
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

				if (records.count() > 0) {
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
					

					for (ConsumerRecord<String, String> record : records) {
						Map<String, Object> map = new HashMap<>();
						
						Connection sourceConn = null;
						Connection sinkConn = null;
						try {	
							sourceConn = sourceConnPool.getConnection();
							sinkConn = sinkConnPool.getConnection();
							sinkConn.setAutoCommit(false);
							map.put("partition", record.partition());
							map.put("offset", record.offset());
							map.put("value", record.value());
							//					System.out.println(this.id + ": " + data);

							ObjectMapper objectMapper = new ObjectMapper();
							objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

							JsonNode jsonNode = objectMapper.readTree(record.value());
							JsonNode payload = jsonNode.get("payload");
							//	payloadStr = payload.toString();

							String operation = payload.get("OPERATION").asText();

							String tableName = payload.get("TABLE_NAME").asText();
							
							logger.info("   >>>offset={},operation={}, TableName={}", record.offset(), operation, tableName);
							logger.info("   >>>payload={}", payload.toPrettyString());
							
							String data = payload.get("data").toString();
							String befor = payload.get("before").toString();
							
							
							
//					
							
							
							sinkConn.commit();
						} catch(Exception e) {
							logger.error(">>>record error, message={}, stack trace={}, record str={}", e.getMessage(), ExceptionUtils.getStackTrace(e), map);
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

	public void shutdown() {
		consumer.wakeup();
	}

	
//
//	private void insertPartyContact(Connection sourceConn, Connection sinkConn, ProductionDetail productionDetail) throws Exception  {
//		PreparedStatement pstmt = null;
//		try {
//			String sql = "select count(*) AS COUNT from " + config.sinkTableProductionDetail
//					+ " where role_type = " + partyContact.getRoleType() + " and list_id = " + partyContact.getListId();
//			int count = 0;
//			if (count == 0) {
//				if (partyContact.getAddressId() == null) {
//					sql = "insert into " + config.sinkTablePartyContact + " (ROLE_TYPE,LIST_ID,POLICY_ID,NAME,CERTI_CODE,MOBILE_TEL,EMAIL) " 
//							+ " values (?,?,?,?,?,?,?)";
//					pstmt = sinkConn.prepareStatement(sql);
//					pstmt.setInt(1, partyContact.getRoleType());
//					pstmt.setLong(2, partyContact.getListId());
//					pstmt.setLong(3, partyContact.getPolicyId());
//					pstmt.setString(4, partyContact.getName());
//					pstmt.setString(5, partyContact.getCertiCode());
//					pstmt.setString(6, partyContact.getMobileTel());
//					if (partyContact.getRoleType() == CONTRACT_BENE_ROLE_TYPE) {
//						pstmt.setNull(7, Types.VARCHAR);
//					} else {
//						pstmt.setString(7, partyContact.getEmail());
//					}
//
//					pstmt.executeUpdate();
//					pstmt.close();
//				} else {
//					String address1 = getSourceAddress1(sourceConn, partyContact.getAddressId());
//					partyContact.setAddress1(address1);
//
//					sql = "insert into " + config.sinkTablePartyContact + " (ROLE_TYPE,LIST_ID,POLICY_ID,NAME,CERTI_CODE,MOBILE_TEL,EMAIL,ADDRESS_ID,ADDRESS_1) " 
//							+ " values (?,?,?,?,?,?,?,?,?)";
//					pstmt = sinkConn.prepareStatement(sql);
//					pstmt.setInt(1, partyContact.getRoleType());
//					pstmt.setLong(2, partyContact.getListId());
//					pstmt.setLong(3, partyContact.getPolicyId());
//					pstmt.setString(4, partyContact.getName());
//					pstmt.setString(5, partyContact.getCertiCode());
//					pstmt.setString(6, partyContact.getMobileTel());
//					pstmt.setString(7, partyContact.getEmail());
//					pstmt.setLong(8, partyContact.getAddressId());
//					if (address1 == null) {
//						pstmt.setNull(9, Types.VARCHAR);
//					} else {
//						pstmt.setString(9, partyContact.getAddress1());
//					}
//					pstmt.executeUpdate();
//					pstmt.close();
//				}
//
//			} else {
//				// record exists, error
//				String error = String.format("table=%s record already exists, therefore cannot insert, role_type=%d, list_id=%d", config.sinkTablePartyContact, partyContact.getRoleType(), partyContact.getListId());
//				throw new Exception(error);
//			}
//		} catch (Exception e) {
//			throw e;
//		} finally {
//			if (pstmt != null) pstmt.close();
//		}
//	}
//	private void updatePartyContact(Connection sourceConn, Connection sinkConn, ProductionDetail partyContact, ProductionDetail beforePartyContact) throws Exception  {
//		PreparedStatement pstmt = null;
//		ResultSet rs = null;
//		try {
//			String sql = "select *  from " + config.sinkTablePartyContact 
//					+ " where role_type = ? and list_id = ?";
//			pstmt = sinkConn.prepareStatement(sql);
//			pstmt.setInt(1, partyContact.getRoleType());
//			pstmt.setLong(2, partyContact.getListId());
//			rs = pstmt.executeQuery();
//			//Long partyAddressId = null;
//			int count = 0;
//			while (rs.next()) {
//				//	partyAddressId = rs.getLong("ADDRESS_ID");
//				count++;
//			}
//			rs.close();
//			pstmt.close();
//			logger.info(">>>count={}", count);
//
//			if (count > 0) {
//				if (partyContact.getAddressId() == null) {
//					sql = "update " + config.sinkTablePartyContact 
//							+ " set POLICY_ID=?,NAME=?,CERTI_CODE=?,MOBILE_TEL=?,EMAIL=?,ADDRESS_ID=null,ADDRESS_1=null"
//							+ " where ROLE_TYPE=? and LIST_ID=?";
//					pstmt = sinkConn.prepareStatement(sql);
//					pstmt.setLong(1, partyContact.getPolicyId());
//					pstmt.setString(2, partyContact.getName());
//					pstmt.setString(3, partyContact.getCertiCode());
//					pstmt.setString(4, partyContact.getMobileTel());
//					pstmt.setString(5, partyContact.getEmail());
//
//					pstmt.setInt(6, partyContact.getRoleType());
//					pstmt.setLong(7, partyContact.getListId());
//
//					pstmt.executeUpdate();
//					pstmt.close();
//				} else {
//
//					// update without address
//					if (beforePartyContact.getAddressId() != null 
//							&& partyContact.getAddressId() != null
//							&& beforePartyContact.getAddressId().longValue() == partyContact.getAddressId().longValue()) {
//						sql = "update " + config.sinkTablePartyContact 
//								+ " set POLICY_ID=?,NAME=?,CERTI_CODE=?,MOBILE_TEL=?,EMAIL=?"
//								+ " where ROLE_TYPE=? and LIST_ID=?";
//						pstmt = sinkConn.prepareStatement(sql);
//						pstmt.setLong(1, partyContact.getPolicyId());
//						pstmt.setString(2, partyContact.getName());
//						pstmt.setString(3, partyContact.getCertiCode());
//						pstmt.setString(4, partyContact.getMobileTel());
//						pstmt.setString(5, partyContact.getEmail());
//
//						pstmt.setInt(6, partyContact.getRoleType());
//						pstmt.setLong(7, partyContact.getListId());
//
//						pstmt.executeUpdate();
//						pstmt.close();
//					} // update with address
//					else {
//						// get address1
//						String address1 = getSourceAddress1(sourceConn, partyContact.getAddressId());	
//						partyContact.setAddress1(address1);
//
//						// update 
//						sql = "update " + config.sinkTablePartyContact 
//								+ " set POLICY_ID=?,NAME=?,CERTI_CODE=?,MOBILE_TEL=?,EMAIL=?,ADDRESS_ID=?,ADDRESS_1=?"
//								+ " where ROLE_TYPE=? and LIST_ID=?";
//						pstmt = sinkConn.prepareStatement(sql);
//						pstmt.setLong(1, partyContact.getPolicyId());
//						pstmt.setString(2, partyContact.getName());
//						pstmt.setString(3, partyContact.getCertiCode());
//						pstmt.setString(4, partyContact.getMobileTel());
//						pstmt.setString(5, partyContact.getEmail());
//						pstmt.setLong(6, partyContact.getAddressId());
//						pstmt.setString(7, partyContact.getAddress1());
//
//						pstmt.setInt(8, partyContact.getRoleType());
//						pstmt.setLong(9, partyContact.getListId());
//
//						pstmt.executeUpdate();
//						pstmt.close();
//					}
//
//				}
//
//			} else {
//				// record exists, error
//				String error = String.format("table=%s record does not exists, therefore cannot update, role_type=%d, list_id=%d", config.sinkTablePartyContact, partyContact.getRoleType(), partyContact.getListId());
//				throw new Exception(error);
//			}
//		} catch (Exception e) {
//			throw e;
//		} finally {
//			if (rs != null) rs.close();
//			if (pstmt != null) pstmt.close();
//		}
//	}
//	private void deletePartyContact(Connection sinkConn, ProductionDetail beforePartyContact) throws Exception  {
//		PreparedStatement pstmt = null;
//		try {
//			String sql = "select count(*) AS COUNT from " + config.sinkTablePartyContact 
//					+ " where role_type = " + beforePartyContact.getRoleType() + " and list_id = " + beforePartyContact.getListId();
//			int count = getCount(sinkConn, sql);
//			if (count > 0) {
//				sql = "delete from " + config.sinkTablePartyContact + " where role_type = ? and list_id = ?";
//
//				pstmt = sinkConn.prepareStatement(sql);
//				pstmt.setInt(1, beforePartyContact.getRoleType());
//				pstmt.setLong(2, beforePartyContact.getListId());
//
//				pstmt.executeUpdate();
//				pstmt.close();
//
//			} else {
//				// record exists, error
//				String error = String.format("table=%s record does not exist, therefore cannot delete, role_type=%d, list_id=%d", config.sinkTablePartyContact, beforePartyContact.getRoleType(), beforePartyContact.getListId());
//				throw new Exception(error);
//			}
//		} catch (Exception e) {
//			throw e;
//		} finally {
//			if (pstmt != null) pstmt.close();
//		}
//	}

}