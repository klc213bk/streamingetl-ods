package com.transglobe.streamingetl.ods.consumer;

import java.math.BigDecimal;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transglobe.streamingetl.ods.consumer.model.PolicyPrintJob;

public class PolicyPrintJobConsumerLoop extends ConsumerLoop {
	static final Logger logger = LoggerFactory.getLogger(PolicyPrintJobConsumerLoop.class);

	public PolicyPrintJobConsumerLoop(int id,
			Config config,
			BasicDataSource sourceConnPool,
			BasicDataSource sinkConnPool,
			Date dataDate)
	{
		
		super(id, config.groupIdTPolicyPrintJob, config.topicsTPolicyPrintJob, config.bootstrapServers, config.sinkTableKPolicyPrintJob, sourceConnPool, sinkConnPool, dataDate);
		
	}

//	@Override
//	protected void consume() throws Exception {
//	
//		Connection sourceConn = null;
//		Connection sinkConn = null;
//		try {	
//			sinkConn = sinkConnPool.getConnection();
//			
//			if (StringUtils.equals("INSERT", operation)) {
//				insert(sinkConn);
//			} else if (StringUtils.equals("UPDATE", operation)) {
//				update(sinkConn);
//			} else if (StringUtils.equals("DELETE", operation)) {
//				delete(sinkConn);
//			}
//		} finally {
//			if (sinkConn != null) sinkConn.close();
//			if (sourceConn != null) sourceConn.close();
//		}
//		
//	}
	@Override
	void delete(Connection sinkConn, ObjectMapper objectMapper, String dataString, String beforeString, BigDecimal scn,
			BigDecimal commitScn, String rowId) throws Exception {
		PreparedStatement sinkPstmt = null;
		try {
			PolicyPrintJob obj = (beforeString == null)? null : objectMapper.readValue(beforeString, PolicyPrintJob.class);;
			
			String sql = "delete " + sinkTableName
					+ " where JOB_ID=?";
		
			sinkPstmt = sinkConn.prepareStatement(sql);		
			sinkPstmt.setBigDecimal(1, obj.getJobId());	
	
			sinkPstmt.executeUpdate();
			sinkPstmt.close();

		} catch (Exception e) {
			throw e;
		} finally {
			if (sinkPstmt != null) sinkPstmt.close();
		}
	}
	@Override
	void update(Connection sinkConn, ObjectMapper objectMapper, String dataString, String beforeString, BigDecimal scn,
			BigDecimal commitScn, String rowId) throws Exception {
		PreparedStatement sinkPstmt = null;
		try {
			PolicyPrintJob obj = (dataString == null)? null : objectMapper.readValue(dataString, PolicyPrintJob.class);;
			
			String sql = "update " + this.sinkTableName
					+ " set JOB_ID=?"                                  
					+ ",POLICY_ID=?"                             
					+ ",PAYCARD_INDI=?"                           
					+ ",LETTER_INDI=?"                          
					+ ",ACKLETTER_INDI=?"                         
					+ ",PIS_INDI=?"                           
					+ ",HEALTH_CARD_INDI=?"                        
					+ ",COVER_INDI=?"                              
					+ ",SCHEDULE_INDI=?"                           
					+ ",CLAUSE_INDI=?"                            
					+ ",ANNEXURE_INDI=?"                          
					+ ",ENDORSE_INDI=?"                         
					+ ",EXCLUSION_INDI=?"                         
					+ ",PRINT_CATEGORY=?"                         
					+ ",PRINT_TYPE=?"                         
					+ ",PRINT_COPYSET=?"                         
					+ ",PRINT_REASON=?"                         
					+ ",VALID_STATUS=?"                         
					+ ",PRINT_DATE=?"                         
					+ ",OPERATOR_ID=?"                         
					+ ",INSERT_TIME=?"                         
					+ ",INSERTED_BY=?"                         
					+ ",INSERT_TIMESTAMP"                        
					+ ",UPDATED_BY=?"                        
					+ ",UPDATE_TIMESTAMP"                        
					+ ",PREMVOUCHER_INDI"                        
					+ ",ARCHIVE_ID=?"                        
					+ ",UPDATE_TIME=?"                        
					+ ",JOB_TYPE_DESC=?"                        
					+ ",JOB_READY_DATE=?"                        
					+ ",CONTENT=? "                        
					+ ",COPY=?"                        
					+ ",ERROR_CODE=?"                        
					+ ",LANG_ID=?"                        
					+ ",CHANGE_ID=?"                        
					+ ",PRINT_COMP_INDI=? "                        
					+ ",DATA_DATE=?"		// ods add column 	        
					+ ",TBL_UPD_TIME=CURRENT_DATE"			// ods add column
					+ ",SCN=?"	// new column
					+ ",COMMIT_SCN=?"	// new column
					+ ",ROW_ID=?"	// new column
					+ " where JOB_ID=?";
		
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
			
			sinkPstmt.setBigDecimal(38, scn);				// new column
			sinkPstmt.setBigDecimal(39, commitScn);				// new column
			sinkPstmt.setString(40, rowId);				// new column
			
			sinkPstmt.setBigDecimal(41, obj.getJobId());
	
			sinkPstmt.executeUpdate();
			sinkPstmt.close();

		} catch (Exception e) {
			throw e;
		} finally {
			if (sinkPstmt != null) sinkPstmt.close();
		}
	}
	@Override
	void insert(Connection sinkConn, ObjectMapper objectMapper, String dataString, String beforeString, BigDecimal scn,
			BigDecimal commitScn, String rowId) throws Exception {
		PreparedStatement sinkPstmt = null;
		try {
			PolicyPrintJob obj = (dataString == null)? null : objectMapper.readValue(dataString, PolicyPrintJob.class);
			
			String sql = "insert into " + this.sinkTableName
					+ " (JOB_ID"                                  
					+ ",POLICY_ID"                             
					+ ",PAYCARD_INDI"                           
					+ ",LETTER_INDI"                          
					+ ",ACKLETTER_INDI"                         
					+ ",PIS_INDI"                           
					+ ",HEALTH_CARD_INDI"                        
					+ ",COVER_INDI"                              
					+ ",SCHEDULE_INDI"                           
					+ ",CLAUSE_INDI"                            
					+ ",ANNEXURE_INDI"                          
					+ ",ENDORSE_INDI"                         
					+ ",EXCLUSION_INDI"                         
					+ ",PRINT_CATEGORY"                         
					+ ",PRINT_TYPE"                         
					+ ",PRINT_COPYSET"                         
					+ ",PRINT_REASON"                         
					+ ",VALID_STATUS"                         
					+ ",PRINT_DATE"                         
					+ ",OPERATOR_ID"                         
					+ ",INSERT_TIME"                         
					+ ",INSERTED_BY"                         
					+ ",INSERT_TIMESTAMP"                        
					+ ",UPDATED_BY"                        
					+ ",UPDATE_TIMESTAMP"                        
					+ ",PREMVOUCHER_INDI"                        
					+ ",ARCHIVE_ID"                        
					+ ",UPDATE_TIME"                        
					+ ",JOB_TYPE_DESC"                        
					+ ",JOB_READY_DATE"                        
					+ ",CONTENT"                        
					+ ",COPY"                        
					+ ",ERROR_CODE"                        
					+ ",LANG_ID"                        
					+ ",CHANGE_ID"                        
					+ ",PRINT_COMP_INDI"                        
					+ ",DATA_DATE"		// ods add column 	        
					+ ",TBL_UPD_TIME "			// ods add column
					+ ",SCN"		// new column
					+ ",COMMIT_SCN"	// new column
					+ ",ROW_ID)"	// new column
					+ " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
					+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_DATE,?,?,?)";
		
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
			
			sinkPstmt.setBigDecimal(38, scn);				// new column
			sinkPstmt.setBigDecimal(39, commitScn);				// new column
			sinkPstmt.setString(40, rowId);				// new column
			
			sinkPstmt.executeUpdate();
			sinkPstmt.close();

		} catch (Exception e) {
			throw e;
		} finally {
			if (sinkPstmt != null) sinkPstmt.close();
		}
	}


	
}