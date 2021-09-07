package com.transglobe.streamingetl.ods.consumer;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Types;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.transglobe.streamingetl.ods.consumer.model.PolicyPrintJob;

public class PolicyPrintJobConsumerLoop extends ConsumerLoop {
	static final Logger logger = LoggerFactory.getLogger(PolicyPrintJobConsumerLoop.class);

	public PolicyPrintJobConsumerLoop(int id,
			Config config,
			BasicDataSource sourceConnPool,
			BasicDataSource sinkConnPool) {
		
		super(id, config.groupIdTPolicyPrintJob, config, sourceConnPool, sinkConnPool);
		
	}

	@Override
	protected void consume(ConsumerRecord<String, String> record) throws Exception {
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

		JsonNode jsonNode = objectMapper.readTree(record.value());
		JsonNode payload = jsonNode.get("payload");
		//	payloadStr = payload.toString();

		String operation = payload.get("OPERATION").asText();
		Long scn = Long.valueOf(payload.get("OPERATION").toString());

		String tableName = payload.get("TABLE_NAME").asText();
		logger.info("   >>>offset={},operation={}, TableName={}, scn={}", record.offset(), operation, tableName, scn);

		String payLoadData = payload.get("data").toString();
		String beforePayLoadData = payload.get("before").toString();
		
		PolicyPrintJob policyPrintJob = (payLoadData == null)? null : objectMapper.readValue(payLoadData, PolicyPrintJob.class);;
		
		Connection sinkConn = sinkConnPool.getConnection();
		
		if (StringUtils.equals("INSERT", operation)) {
			insert(sinkConn, policyPrintJob, scn);
		}
	}

	private void insert(Connection sinkConn, PolicyPrintJob obj, Long scn) throws Exception  {
		PreparedStatement sinkPstmt = null;
		try {
			String sql = "insert into " + config.sinkTableKPolicyPrintJob
					+ " (JOB_ID"                                  
					+ ",POLICY_ID"                             
					+ ",PAYCARD_INDI "                           
					+ ",LETTER_INDI   "                          
					+ ",ACKLETTER_INDI "                         
					+ ",PIS_INDI"                           
					+ ",HEALTH_CARD_INDI"                        
					+ ",COVER_INDI"                              
					+ ",SCHEDULE_INDI"                           
					+ ",CLAUSE_INDI "                            
					+ ",ANNEXURE_INDI "                          
					+ ",ENDORSE_INDI   "                         
					+ ",EXCLUSION_INDI "                         
					+ ",PRINT_CATEGORY "                         
					+ ",PRINT_TYPE     "                         
					+ ",PRINT_COPYSET  "                         
					+ ",PRINT_REASON   "                         
					+ ",VALID_STATUS   "                         
					+ ",PRINT_DATE     "                         
					+ ",OPERATOR_ID    "                         
					+ ",INSERT_TIME    "                         
					+ ",INSERTED_BY    "                         
					+ ",INSERT_TIMESTAMP"                        
					+ ",UPDATED_BY      "                        
					+ ",UPDATE_TIMESTAMP"                        
					+ ",PREMVOUCHER_INDI"                        
					+ ",ARCHIVE_ID      "                        
					+ ",UPDATE_TIME     "                        
					+ ",JOB_TYPE_DESC   "                        
					+ ",JOB_READY_DATE  "                        
					+ ",CONTENT         "                        
					+ ",COPY            "                        
					+ ",ERROR_CODE      "                        
					+ ",LANG_ID         "                        
					+ ",CHANGE_ID       "                        
					+ ",PRINT_COMP_INDI "                        
					+ ",DATA_DATE       "		// ods add column 	        
					+ ",TBL_UPD_TIME  "			// ods add column
					+ ",TBL_UPD_SCN)"	// new column
					+ " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
					+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_DATE,?)";
		
			sinkPstmt = sinkConn.prepareStatement(sql);
			
			sinkPstmt.setBigDecimal(1, obj.getJobId());
			sinkPstmt.setBigDecimal(2, obj.getPolicyId());
			sinkPstmt.setString(3,  obj.getPaycardIndi());
			sinkPstmt.setString(4,  obj.getLetterIndi());
			sinkPstmt.setString(5,  obj.getAckletterIndi());
			sinkPstmt.setString(6,  obj.getPisIndi());
			sinkPstmt.setString(7,  obj.getHealthCardIndi());
			sinkPstmt.setString(8,  obj.getCoverIndi());
			sinkPstmt.setString(9,  obj.getScheduleIndi());
			sinkPstmt.setString(10,  obj.getClauseIndi());
			sinkPstmt.setString(11,  obj.getAnnexureIndi());
			sinkPstmt.setString(12,  obj.getEndorseIndi());
			sinkPstmt.setString(13,  obj.getExclusionIndi());
			sinkPstmt.setBigDecimal(14,  obj.getPrintCategory());
			sinkPstmt.setBigDecimal(15,  obj.getPrintType());
			sinkPstmt.setString(16,  obj.getPrintCopyset());
			sinkPstmt.setString(17,  obj.getPrintReason());
			sinkPstmt.setString(18,  obj.getValidStatus());
			sinkPstmt.setDate(19, new Date(obj.getPrintDateMilli()));
			sinkPstmt.setBigDecimal(20,  obj.getOperatorId());
			sinkPstmt.setDate(21, new Date(obj.getInsertTimeMillis()));
			sinkPstmt.setBigDecimal(22,  obj.getInsertedBy());
			sinkPstmt.setDate(23, new Date(obj.getInsertTimestampMillis()));
			sinkPstmt.setBigDecimal(24,  obj.getUpdatedBy());
			sinkPstmt.setDate(25, new Date(obj.getUpateTimestampMillis()));
			sinkPstmt.setString(26,  obj.getPreMvoucherIndi());
			sinkPstmt.setBigDecimal(27,  obj.getArchiveId());
			sinkPstmt.setDate(28, new Date(obj.getUpdateTimeMillis()));
			sinkPstmt.setString(29,  obj.getJobTypeDesc());
			sinkPstmt.setDate(30, new Date(obj.getJobReadyDateMillis()));
			
			Clob clob = sinkConn.createClob();
			clob.setString(1, obj.getContent());
			sinkPstmt.setClob(31, clob);
			
			sinkPstmt.setBigDecimal(32,  obj.getCopy());
			sinkPstmt.setBigDecimal(33,  obj.getErrorCode());
			sinkPstmt.setString(34,  obj.getLangId());
			sinkPstmt.setBigDecimal(35,  obj.getChangeId());
			sinkPstmt.setBigDecimal(36,  obj.getPrintCompIndi());
			
			sinkPstmt.setDate(37, this.dataDate);
			
			// db current_time for tbl_upd_time 
			
			sinkPstmt.setLong(38, scn);				// new column
	
			sinkPstmt.executeUpdate();
			sinkPstmt.close();

		} catch (Exception e) {
			throw e;
		} finally {
			if (sinkPstmt != null) sinkPstmt.close();
		}
	}
	
}