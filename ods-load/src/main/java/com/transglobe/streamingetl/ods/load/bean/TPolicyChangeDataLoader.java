package com.transglobe.streamingetl.ods.load.bean;

import java.io.Console;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TPolicyChangeDataLoader extends DataLoader {
	private static final Logger logger = LoggerFactory.getLogger(TPolicyChangeDataLoader.class);

	private String sourceTableName ;
	
	private String sinkTableName;
	
	private String streamingEtlName;
	
	private String sinkTableCreateFile;
	
	private String sinkTableIndexesFile;
	
	public TPolicyChangeDataLoader(Config config, Date dataDate) throws Exception {
		
		super(DataLoader.DEFAULT_THREADS, DataLoader.DEFAULT_BATCH_COMMIT_SIZE, config, dataDate);
		
		this.sourceTableName = config.sourceTableTPolicyChange;
		
		this.sinkTableName = config.sinkTableKPolicyChange;
		
		this.streamingEtlName = config.streamingEtlNameTPolicyChange;
		
		this.sinkTableCreateFile = config.sinkTableCreateFileKPolicyChange;
		
		this.sinkTableIndexesFile = config.sinkTableIndexesFileKPolicyChange;
	}	
	
	@Override
	public String getSourceTableName() {
		return this.sourceTableName;
	}

	@Override
	public String getStreamingEtlName() {
		return this.streamingEtlName;
	}

	@Override
	protected String getSinkTableName() {
		return this.sinkTableName;
	}

	@Override
	protected String getSinkTableCreateFileName() {
		return sinkTableCreateFile;
	}

	@Override
	protected String getSinkTableIndexesCreateFileName() {
		return sinkTableIndexesFile;
	}

	@Override
	protected String getSelectMinIdSql() {
		return "select min(POLICY_CHG_ID) as MIN_ID from " + this.sourceTableName;
	}

	@Override
	protected String getSelectMaxIdSql() {
		return "select max(POLICY_CHG_ID) as MAX_ID from " + this.sourceTableName;
	}

	@Override
	protected String getCountSql() {
		return "select count(*) as CNT from " + this.sourceTableName + " a where ? <= a.POLICY_CHG_ID and a.POLICY_CHG_ID < ?";
	}

	@Override
	protected String getSelectSql() {
		return "select"
				+ " POLICY_CHG_ID"
				+ ",POLICY_ID"
				+ ",SERVICE_ID"
				+ ",CHANGE_RECORD"
				+ ",INSERT_TIME"
				+ ",VALIDATE_TIME"
				+ ",NEED_UNDERWRITE"
				+ ",APPLY_TIME"
				+ ",CHANGE_CAUSE"
				+ ",CANCEL_ID"
				+ ",CANCEL_TIME"
				+ ",REJECTER_ID"
				+ ",REJECT_TIME"
				+ ",REJECT_NOTE"
				+ ",UPDATE_TIME"
				+ ",MASTER_CHG_ID"
				+ ",CANCEL_CAUSE"
				+ ",CANCEL_NOTE"
				+ ",REJECT_CAUSE"
				+ ",LAST_HANDLER_ID"
				+ ",LAST_ENTRY_TIME"
				+ ",LAST_UW_DISP_ID"
				+ ",LAST_UW_DISP_TIME"
				+ ",ORDER_ID"
				+ ",POLICY_CHG_STATUS"
				+ ",DISPATCH_EMP"
				+ ",LETTER_EFFECT_TYPE"
				+ ",DISPATCH_TYPE"
				+ ",DISPATCH_LETTER"
				+ ",SUB_SERVICE_ID"
				+ ",CHANGE_NOTE"
				+ ",INSERTED_BY"
				+ ",INSERT_TIMESTAMP"
				+ ",UPDATED_BY"
				+ ",UPDATE_TIMESTAMP"
				+ ",PRE_POLICY_CHG"
				+ ",CHANGE_SEQ"
				+ ",FINISH_TIME"
				+ ",ORG_ID"
				+ ",REQUEST_EFFECT_DATE"
				+ ",TASK_DUE_DATE"
				+ ",ALTERATION_INFO_ID"
				+ ",POLICY_CHG_ORDER_SEQ" 
				+ " from " + this.sourceTableName
				+ " a where ? <= a.POLICY_CHG_ID and a.POLICY_CHG_ID < ?";
	}

	@Override
	protected String getInsertSql() {
		return "insert into " + this.sinkTableName
				+ " (POLICY_CHG_ID"
				+ ",POLICY_ID"
				+ ",SERVICE_ID"
				+ ",CHANGE_RECORD"
				+ ",INSERT_TIME"
				+ ",VALIDATE_TIME"
				+ ",NEED_UNDERWRITE"
				+ ",APPLY_TIME"
				+ ",CHANGE_CAUSE"
				+ ",CANCEL_ID"
				+ ",CANCEL_TIME"
				+ ",REJECTER_ID"
				+ ",REJECT_TIME"
				+ ",REJECT_NOTE"
				+ ",UPDATE_TIME"
				+ ",MASTER_CHG_ID"
				+ ",CANCEL_CAUSE"
				+ ",CANCEL_NOTE"
				+ ",REJECT_CAUSE"
				+ ",LAST_HANDLER_ID"
				+ ",LAST_ENTRY_TIME"
				+ ",LAST_UW_DISP_ID"
				+ ",LAST_UW_DISP_TIME"
				+ ",ORDER_ID"
				+ ",POLICY_CHG_STATUS"
				+ ",DISPATCH_EMP"
				+ ",LETTER_EFFECT_TYPE"
				+ ",DISPATCH_TYPE"
				+ ",DISPATCH_LETTER"
				+ ",SUB_SERVICE_ID"
				+ ",CHANGE_NOTE"
				+ ",INSERTED_BY"
				+ ",INSERT_TIMESTAMP"
				+ ",UPDATED_BY"
				+ ",UPDATE_TIMESTAMP"
				+ ",PRE_POLICY_CHG"
				+ ",CHANGE_SEQ"
				+ ",FINISH_TIME"
				+ ",ORG_ID"
				+ ",REQUEST_EFFECT_DATE"
				+ ",TASK_DUE_DATE"
				+ ",ALTERATION_INFO_ID"
				+ ",POLICY_CHG_ORDER_SEQ"                      
				+ ",DATA_DATE"				// ods add column 	        
				+ ",TBL_UPD_TIME"			// ods add column
				+ ",SCN"		// new column
				+ ",COMMIT_SCN"	// new column
				+ ",ROW_ID)"	// new column
				+ " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,CURRENT_DATE,?,?,NULL)";
			
	}

	@Override
	public void transferData(LoadBean loadBean, BasicDataSource sourceConnectionPool, BasicDataSource sinkConnectionPool,
			BasicDataSource logminerConnectionPool) throws Exception {
		Console cnsl = null;
		
		try (
				Connection sourceConn = sourceConnectionPool.getConnection();
				Connection sinkConn = sinkConnectionPool.getConnection();
				Connection minerConn = logminerConnectionPool.getConnection();
				final PreparedStatement sourcePstmt = sourceConn.prepareStatement(getSelectSql());
				final PreparedStatement sinkPstmt = 
						sinkConn.prepareStatement(getInsertSql());  
				)
		{
			long t0 = System.currentTimeMillis();
			sourceConn.setAutoCommit(false);
			sinkConn.setAutoCommit(false); 

			sourcePstmt.setLong(1, loadBean.startSeq);
			sourcePstmt.setLong(2, loadBean.endSeq);

			try (final ResultSet rs =
					sourcePstmt.executeQuery())
			{
				Long count = 0L;
				Long longValue;
				while (rs.next())
				{
					count++;

					sinkPstmt.clearParameters();

					sinkPstmt.setBigDecimal(1, rs.getBigDecimal("POLICY_CHG_ID"));
					sinkPstmt.setBigDecimal(2, rs.getBigDecimal("POLICY_ID"));
					sinkPstmt.setBigDecimal(3, rs.getBigDecimal("SERVICE_ID"));
					sinkPstmt.setString(4, rs.getString("CHANGE_RECORD"));
					sinkPstmt.setDate(5, rs.getDate("INSERT_TIME"));
					sinkPstmt.setDate(6, rs.getDate("VALIDATE_TIME"));
					sinkPstmt.setString(7, rs.getString("NEED_UNDERWRITE"));
					sinkPstmt.setDate(8, rs.getDate("APPLY_TIME"));
					sinkPstmt.setString(9, rs.getString("CHANGE_CAUSE"));
					sinkPstmt.setBigDecimal(10, rs.getBigDecimal("CANCEL_ID"));
					sinkPstmt.setDate(11, rs.getDate("CANCEL_TIME"));
					sinkPstmt.setBigDecimal(12, rs.getBigDecimal("REJECTER_ID"));
					sinkPstmt.setDate(13, rs.getDate("REJECT_TIME"));
					sinkPstmt.setString(14, rs.getString("REJECT_NOTE"));
					sinkPstmt.setDate(15, rs.getDate("UPDATE_TIME"));
					sinkPstmt.setBigDecimal(16, rs.getBigDecimal("MASTER_CHG_ID"));
					sinkPstmt.setString(17, rs.getString("CANCEL_CAUSE"));
					sinkPstmt.setString(18, rs.getString("CANCEL_NOTE"));
					sinkPstmt.setString(19, rs.getString("REJECT_CAUSE"));
					sinkPstmt.setBigDecimal(20, rs.getBigDecimal("LAST_HANDLER_ID"));
					sinkPstmt.setDate(21, rs.getDate("LAST_ENTRY_TIME"));
					sinkPstmt.setBigDecimal(22, rs.getBigDecimal("LAST_UW_DISP_ID"));
					sinkPstmt.setDate(23, rs.getDate("LAST_UW_DISP_TIME"));
					sinkPstmt.setBigDecimal(24, rs.getBigDecimal("ORDER_ID"));
					sinkPstmt.setBigDecimal(25, rs.getBigDecimal("POLICY_CHG_STATUS"));
					sinkPstmt.setBigDecimal(26, rs.getBigDecimal("DISPATCH_EMP"));
					sinkPstmt.setString(27, rs.getString("LETTER_EFFECT_TYPE"));
					sinkPstmt.setString(28, rs.getString("DISPATCH_TYPE"));
					sinkPstmt.setString(29, rs.getString("DISPATCH_LETTER"));
					sinkPstmt.setBigDecimal(30, rs.getBigDecimal("SUB_SERVICE_ID"));
					sinkPstmt.setString(31, rs.getString("CHANGE_NOTE"));
					sinkPstmt.setBigDecimal(32, rs.getBigDecimal("INSERTED_BY"));
					sinkPstmt.setDate(33, rs.getDate("INSERT_TIMESTAMP"));
					sinkPstmt.setBigDecimal(34, rs.getBigDecimal("UPDATED_BY"));
					sinkPstmt.setDate(35, rs.getDate("UPDATE_TIMESTAMP"));
					sinkPstmt.setBigDecimal(36, rs.getBigDecimal("PRE_POLICY_CHG"));
					sinkPstmt.setBigDecimal(37, rs.getBigDecimal("CHANGE_SEQ"));
					sinkPstmt.setDate(38, rs.getDate("FINISH_TIME"));
					sinkPstmt.setBigDecimal(39, rs.getBigDecimal("ORG_ID"));
					sinkPstmt.setDate(40, rs.getDate("REQUEST_EFFECT_DATE"));
					sinkPstmt.setDate(41, rs.getDate("TASK_DUE_DATE"));
					sinkPstmt.setBigDecimal(42, rs.getBigDecimal("ALTERATION_INFO_ID"));
					sinkPstmt.setBigDecimal(43, rs.getBigDecimal("POLICY_CHG_ORDER_SEQ"));
					sinkPstmt.setDate(44, dataDate);
					
					// db current_time for tbl_upd_time 
					
					sinkPstmt.setLong(45, loadBean.currentScn);				// new column
					sinkPstmt.setLong(46, loadBean.currentScn);				// new column
					
					sinkPstmt.addBatch();

					if (count % this.batchCommitSize == 0) {
						sinkPstmt.executeBatch();//executing the batch  
						sinkConn.commit(); 
						sinkPstmt.clearBatch();
					}
				}

				if (count > 0) {
					sinkPstmt.executeBatch();
					sinkConn.commit(); 
					
					long span = System.currentTimeMillis() - t0;
					loadBean.span = span;
					loadBean.count = count;
					
					cnsl = System.console();
					
					cnsl.printf("   >>>insert into %s count=%d, loadbeanseq=%d, loadBeanSize=%d, startSeq=%d, endSeq=%d, span=%d\n", 
							sinkTableName, loadBean.count, loadBean.seq, loadBean.loadBeanSize, loadBean.startSeq, loadBean.endSeq, span);
					cnsl.flush();
				}
			} catch (Exception e) {
				sinkConn.rollback();
				throw e;
			}
			
		} 
	}
	
	
}
