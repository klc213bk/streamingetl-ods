package com.transglobe.streamingetl.ods.load.bean;

import java.io.Console;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TPolicyPrintJobDataLoader extends DataLoader {
	private static final Logger logger = LoggerFactory.getLogger(TPolicyPrintJobDataLoader.class);
	
	protected static final int THREADS = 15;
	
	public static final int BATCH_COMMIT_SIZE = 1000;

	public static final String STREAMING_ETL_NAME = "ODS-T_POLICY_PRINT_JOB";
	
	public static final String SOURCE_TABLE_NAME = "T_POLICY_PRINT_JOB";
	
	private static final String SINK_TABLE_NAME = "K_POLICY_PRINT_JOB";
	
	private static final String SINK_TABLE_CREATE_FILE_NAME = "createtable-K_POLICY_PRINT_JOB.sql";
	private static final String SINK_TABLE_INDEX_CREATE_FILE_NAME = "createindexes-K_POLICY_PRINT_JOB.sql";
	
	public static final String SELECT_MIN_ID_SQL = 
			"select min(JOB_ID) as MIN_ID from " + SOURCE_TABLE_NAME;

	public static final String SELECT_MAX_ID_SQL = 
			"select max(JOB_ID) as MAX_ID from " + SOURCE_TABLE_NAME;

	public static final String COUNT_SQL = 
			"select count(*) as CNT from " + SOURCE_TABLE_NAME + " a where ? <= a.JOB_ID and a.JOB_ID < ?";
			
	public static final String SELECT_SQL = "select "
			+ "JOB_ID"
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
			+ ",ROWID"
			+ " from " + SOURCE_TABLE_NAME
			+ " a where ? <= a.JOB_ID and a.JOB_ID < ? for update";

	public static String INSERT_SQL = 
			"insert into " + SINK_TABLE_NAME 
			+ "(JOB_ID"                                  
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
			+ ",DATA_DATE       "                        
			+ ",TBL_UPD_TIME  "
			+ ",SRC_ROWID"
			+ ",INSERT_CURRENT_SCN)"
			+ " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_DATE,?,?)";
		

	
	public TPolicyPrintJobDataLoader(String fileName, String dataDateStr) throws Exception {
		super(THREADS, BATCH_COMMIT_SIZE, fileName, dataDateStr);
		
		
	}
	
	@Override
	public String getStreamingEtlName() {
		return STREAMING_ETL_NAME;
	}
	@Override
	public String getSourceTableName() {
		return SOURCE_TABLE_NAME;
	}
	@Override
	public String getSinkTableName() {
		return SINK_TABLE_NAME;
	}
	@Override
	String getSinkTableCreateFileName() {
		return SINK_TABLE_CREATE_FILE_NAME;
	}
	@Override
	String getSinkTableIndexCreateFileName() {
		return SINK_TABLE_INDEX_CREATE_FILE_NAME;
	}
	@Override
	String getSelectMinIdSql() {
		return SELECT_MIN_ID_SQL;
	}
	@Override
	String getSelectMaxIdSql() {
		return SELECT_MAX_ID_SQL;
	}
	@Override
	String getCountSql() {
		return COUNT_SQL;
	}

	@Override
	LoadBean transferData(LoadBean loadBean, BasicDataSource sourceConnectionPool, BasicDataSource sinkConnectionPool,
			BasicDataSource logminerConnectionPool, Date dataDate) {
		Console cnsl = null;
		long t0 = System.currentTimeMillis();
		
		try (
				Connection sourceConn = sourceConnectionPool.getConnection();
				Connection sinkConn = sinkConnectionPool.getConnection();
				Connection minerConn = logminerConnectionPool.getConnection();
				final PreparedStatement sourcePstmt = sourceConn.prepareStatement(SELECT_SQL);
				final PreparedStatement sinkPstmt = 
						sinkConn.prepareStatement(INSERT_SQL);
				final PreparedStatement minerPstmt = minerConn.prepareStatement("SELECT CURRENT_SCN FROM v$database")     
				)
		{
			sinkConn.setAutoCommit(false); 

			sourcePstmt.setLong(1, loadBean.startSeq);
			sourcePstmt.setLong(2, loadBean.endSeq);

			Long currentScn = 0L;
			try (final ResultSet rs =
					minerPstmt.executeQuery())
			{
				while (rs.next())
				{
					currentScn = rs.getLong("CURRENT_SCN");
					break;
				}
			} catch (Exception e) {
				logger.error("error message={},stack trace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e) );
			}

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
					sinkPstmt.setString(38, rs.getString("ROWID"));
					
					sinkPstmt.setLong(39, currentScn);

					sinkPstmt.addBatch();

					if (count % BATCH_COMMIT_SIZE == 0) {
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
					long expectedCompleteTime = (Long)((loadBean.loadBeanSize * span / THREADS)/60000);
					cnsl.printf("   >>>insert into %s count=%d, loadbeanseq=%d, loadBeanSize=%d, startSeq=%d, endSeq=%d, span=%d, expectedCompleteTime(min)=%d\n", 
							SINK_TABLE_NAME, loadBean.count, loadBean.seq, loadBean.loadBeanSize, loadBean.startSeq, loadBean.endSeq, span, expectedCompleteTime);
					cnsl.flush();
				}
			} catch (Exception e) {
				logger.error("error message={},stack trace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e) );
			}
		} catch (Exception e) {
			logger.error("error message={},stack trace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e) );
		}
		return loadBean;
	}

}
