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

import com.transglobe.streamingetl.ods.load.Config;

public class TPolicyPrintJobDataLoader extends DataLoader {
	private static final Logger logger = LoggerFactory.getLogger(TPolicyPrintJobDataLoader.class);

	private String sourceTableName ;
	
	private String sinkTableName;
	
	private String sinkTableCreateFile;
	
	private String sinkTableIndexesFile;
	
	public TPolicyPrintJobDataLoader(Config config, Date dataDate) throws Exception {
		
		super(DataLoader.DEFAULT_THREADS, DataLoader.DEFAULT_BATCH_COMMIT_SIZE, config, dataDate);
		
		this.sourceTableName = config.sourceTableTPolicyPrintJob;
		
		this.sinkTableName = config.sinkTableKPolicyPrintJob;
			
		this.sinkTableCreateFile = config.sinkTableCreateFileKPolicyPrintJob;
		
		this.sinkTableIndexesFile = config.sinkTableIndexesFileKPolicyPrintJob;
	}	
	
	@Override
	public String getSourceTableName() {
		return this.sourceTableName;
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
		return "select min(JOB_ID) as MIN_ID from " + this.sourceTableName;
	}

	@Override
	protected String getSelectMaxIdSql() {
		return "select max(JOB_ID) as MAX_ID from " + this.sourceTableName;
	}

	@Override
	protected String getCountSql() {
		return "select count(*) as CNT from " + this.sourceTableName + " a where ? <= a.JOB_ID and a.JOB_ID < ?";
	}

	@Override
	protected String getSelectSql() {
		return "select"
				+ " JOB_ID"
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
				+ ",ORA_ROWSCN"
				+ ",ROWID"
				+ " from " + this.sourceTableName
				+ " a where ? <= a.JOB_ID and a.JOB_ID < ?";
	}

	@Override
	protected String getInsertSql() {
		return "insert into " + this.sinkTableName
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
				+ ",SCN"		// new column
				+ ",COMMIT_SCN"	// new column
				+ ",ROW_ID)"	// new column
				+ " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_DATE,?,?,?)";
			
	}

	@Override
	public void transferData(LoadBean loadBean, BasicDataSource sourceConnectionPool, BasicDataSource sinkConnectionPool) throws Exception {
		Console cnsl = null;
		
		try (
				Connection sourceConn = sourceConnectionPool.getConnection();
				Connection sinkConn = sinkConnectionPool.getConnection();
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

					sinkPstmt.setLong(1, rs.getLong("JOB_ID"));
					
					longValue = rs.getLong("POLICY_ID");
					if (rs.wasNull()) {
						sinkPstmt.setNull(2, Types.BIGINT);
					} else {
						sinkPstmt.setLong(2, longValue);
					}
					
					sinkPstmt.setString(3, rs.getString("PAYCARD_INDI"));
					sinkPstmt.setString(4, rs.getString("LETTER_INDI"));
					sinkPstmt.setString(5, rs.getString("ACKLETTER_INDI"));
					sinkPstmt.setString(6, rs.getString("PIS_INDI"));
					sinkPstmt.setString(7, rs.getString("HEALTH_CARD_INDI"));
					sinkPstmt.setString(8, rs.getString("COVER_INDI"));
					sinkPstmt.setString(9, rs.getString("SCHEDULE_INDI"));
					sinkPstmt.setString(10, rs.getString("CLAUSE_INDI"));
					sinkPstmt.setString(11, rs.getString("ANNEXURE_INDI"));
					sinkPstmt.setString(12, rs.getString("ENDORSE_INDI"));
					sinkPstmt.setString(13, rs.getString("EXCLUSION_INDI"));
					
					longValue = rs.getLong("PRINT_CATEGORY");
					if (rs.wasNull()) {
						sinkPstmt.setNull(14, Types.BIGINT);
					} else {
						sinkPstmt.setLong(14, longValue);
					}
					
					longValue = rs.getLong("PRINT_TYPE");
					if (rs.wasNull()) {
						sinkPstmt.setNull(15, Types.BIGINT);
					} else {
						sinkPstmt.setLong(15, longValue);
					}
					
					sinkPstmt.setString(16, rs.getString("PRINT_COPYSET"));
					sinkPstmt.setString(17, rs.getString("PRINT_REASON"));
					sinkPstmt.setString(18, rs.getString("VALID_STATUS"));
					sinkPstmt.setDate(19, rs.getDate("PRINT_DATE"));
					
					longValue = rs.getLong("OPERATOR_ID");
					if (rs.wasNull()) {
						sinkPstmt.setNull(20, Types.BIGINT);
					} else {
						sinkPstmt.setLong(20, longValue);
					}
					
					sinkPstmt.setDate(21, rs.getDate("INSERT_TIME"));
					
					longValue = rs.getLong("INSERTED_BY");
					if (rs.wasNull()) {
						sinkPstmt.setNull(22, Types.BIGINT);
					} else {
						sinkPstmt.setLong(22, longValue);
					}
					
					sinkPstmt.setDate(23, rs.getDate("INSERT_TIMESTAMP"));
					
					longValue = rs.getLong("UPDATED_BY");
					if (rs.wasNull()) {
						sinkPstmt.setNull(24, Types.BIGINT);
					} else {
						sinkPstmt.setLong(24, longValue);
					}
					
					sinkPstmt.setDate(25, rs.getDate("UPDATE_TIMESTAMP"));
					sinkPstmt.setString(26, rs.getString("PREMVOUCHER_INDI"));
					
					longValue = rs.getLong("ARCHIVE_ID");
					if (rs.wasNull()) {
						sinkPstmt.setNull(27, Types.BIGINT);
					} else {
						sinkPstmt.setLong(27, longValue);
					}
					sinkPstmt.setDate(28, rs.getDate("UPDATE_TIME"));
					sinkPstmt.setString(29, rs.getString("JOB_TYPE_DESC"));
					sinkPstmt.setDate(30, rs.getDate("JOB_READY_DATE"));
					
					Clob clob = rs.getClob("CONTENT");
					if (rs.wasNull()) {
						sinkPstmt.setNull(31, Types.CLOB);
					} else {
						sinkPstmt.setCharacterStream(31, clob.getCharacterStream());
					}
				
					longValue = rs.getLong("COPY");
					if (rs.wasNull()) {
						sinkPstmt.setNull(32, Types.BIGINT);
					} else {
						sinkPstmt.setLong(32, longValue);
					}
					
					longValue = rs.getLong("ERROR_CODE");
					if (rs.wasNull()) {
						sinkPstmt.setNull(33, Types.BIGINT);
					} else {
						sinkPstmt.setLong(33, longValue);
					}
					
					sinkPstmt.setString(34, rs.getString("LANG_ID"));
					
					longValue = rs.getLong("CHANGE_ID");
					if (rs.wasNull()) {
						sinkPstmt.setNull(35, Types.BIGINT);
					} else {
						sinkPstmt.setLong(35, longValue);
					}
					
					longValue = rs.getLong("PRINT_COMP_INDI");
					if (rs.wasNull()) {
						sinkPstmt.setNull(36, Types.BIGINT);
					} else {
						sinkPstmt.setLong(36, longValue);
					}
					
					sinkPstmt.setDate(37, dataDate);
					
					// db current_time for tbl_upd_time 
					
					sinkPstmt.setLong(38, rs.getLong("ORA_ROWSCN"));				// new column
					sinkPstmt.setLong(39, rs.getLong("ORA_ROWSCN"));				// new column
					sinkPstmt.setString(40,  rs.getString("ROWID"));		// new column
					
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
