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

public class TImageDataLoader extends DataLoader {

	private static final Logger logger = LoggerFactory.getLogger(TImageDataLoader.class);

	private String sourceTableName ;

	private String sinkTableName;

	private String streamingEtlName;

	private String sinkTableCreateFile;

	private String sinkTableIndexesFile;

	public TImageDataLoader(Config config, Date dataDate) throws Exception {

		super(config, dataDate);

		this.sourceTableName = config.sourceTableTPolicyPrintJob;

		this.sinkTableName = config.sinkTableTPolicyPrintJob;

		this.streamingEtlName = config.streamingEtlNameTPolicyPrintJob;

		this.sinkTableCreateFile = config.sinkTableCreateFileKPolicyPrintJob;

		this.sinkTableIndexesFile = config.sinkTableIndexesFileKPolicyPrintJob;
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
		return "select min(IMAGE_ID) as MIN_ID from " + this.sourceTableName;
	}

	@Override
	protected String getSelectMaxIdSql() {
		return "select max(IMAGE_ID) as MAX_ID from " + this.sourceTableName;
	}

	@Override
	protected String getCountSql() {
		return "select count(*) as CNT from " + this.sourceTableName + " a where ? <= a.IMAGE_ID and a.IMAGE_ID < ?";
	}

	@Override
	protected String getSelectSql() {
		// TODO Auto-generated method stub
		return "select"
		+ " IMAGE_ID"
		+ ",POLICY_ID"
		+ ",IMAGE_TYPE_ID"
		+ ",SEQ_NUMBER"
		+ ",IMAGE_FORMAT"
		+ ",IMAGE_DATA"
		+ ",SCAN_TIME"
		+ ",EMP_ID"
		+ ",HEAD_ID"
		+ ",FILE_CODE"
		+ ",PROCESS_STATUS"
		+ ",GROUP_POLICY_ID"
		+ ",CASE_ID"
		+ ",CHANGE_ID"
		+ ",IMAGE_LOCATION"
		+ ",IMAGE_FILE_NAME"
		+ ",AUTH_CODE"
		+ ",ORGAN_ID"
		+ ",SUB_FILE_CODE"
		+ ",BUSINESS_ORGAN"
		+ ",IS_PRIORITY"
		+ ",ZIP_DATE"
		+ ",IS_CLEAR"
		+ ",LIST_ID"
		+ ",INSERT_TIME"
		+ ",INSERTED_BY"
		+ ",UPDATE_TIME"
		+ ",UPDATED_BY"
		+ ",INSERT_TIMESTAMP"
		+ ",UPDATE_TIMESTAMP"
		+ ",DEPT_ID"
		+ ",COMPANY_CODE"
		+ ",PERSONAL_CODE"
		+ ",BATCH_DEPT_TYPE"
		+ ",BATCH_DATE"
		+ ",BATCH_AREA"
		+ ",BATCH_DOC_TYPE"
		+ ",BOX_NO"
		+ ",REMARK"
		+ ",SIGNATURE"
		+ ",REAL_WIDTH"
		+ ",SIG_SEQ_NUMBER"
		+ ",SCAN_ORDER"
		+ " from " + this.sourceTableName
		+ " a where ? <= a.IMAGE_ID and a.IMAGE_ID < ?";
	}

	@Override
	protected String getInsertSql() {
		return "insert into " + this.sinkTableName
				+ "IMAGE_ID"
				+ ",POLICY_ID"
				+ ",IMAGE_TYPE_ID"
				+ ",SEQ_NUMBER"
				+ ",IMAGE_FORMAT"
				+ ",IMAGE_DATA"
				+ ",SCAN_TIME"
				+ ",EMP_ID"
				+ ",HEAD_ID"
				+ ",FILE_CODE"
				+ ",PROCESS_STATUS"
				+ ",GROUP_POLICY_ID"
				+ ",CASE_ID"
				+ ",CHANGE_ID"
				+ ",IMAGE_LOCATION"
				+ ",IMAGE_FILE_NAME"
				+ ",AUTH_CODE"
				+ ",ORGAN_ID"
				+ ",SUB_FILE_CODE"
				+ ",BUSINESS_ORGAN"
				+ ",IS_PRIORITY"
				+ ",ZIP_DATE"
				+ ",IS_CLEAR"
				+ ",LIST_ID"
				+ ",INSERT_TIME"
				+ ",INSERTED_BY"
				+ ",UPDATE_TIME"
				+ ",UPDATED_BY"
				+ ",INSERT_TIMESTAMP"
				+ ",UPDATE_TIMESTAMP"
				+ ",DEPT_ID"
				+ ",COMPANY_CODE"
				+ ",PERSONAL_CODE"
				+ ",BATCH_DEPT_TYPE"
				+ ",BATCH_DATE"
				+ ",BATCH_AREA"
				+ ",BATCH_DOC_TYPE"
				+ ",BOX_NO"
				+ ",REMARK"
				+ ",SIGNATURE"
				+ ",REAL_WIDTH"
				+ ",SIG_SEQ_NUMBER"
				+ ",SCAN_ORDER"
				+ ",DATA_DATE"				// ods add column
				+ ",TBL_UPD_TIME"			// ods add column
				+ ",SRC_ROWID" 				// new column
				+ ",TBL_UPD_SCN)"	// new column
				+ " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,CURRENT_DATE,?)";
			
	}

	@Override
	protected LoadBean transferData(LoadBean loadBean, BasicDataSource sourceConnectionPool,
			BasicDataSource sinkConnectionPool, BasicDataSource logminerConnectionPool) throws Exception {
Console cnsl = null;
		
		try (
				Connection sourceConn = sourceConnectionPool.getConnection();
				Connection sinkConn = sinkConnectionPool.getConnection();
				Connection minerConn = logminerConnectionPool.getConnection();
				final PreparedStatement sourcePstmt = sourceConn.prepareStatement(getSelectSql());
				final PreparedStatement sinkPstmt = 
						sinkConn.prepareStatement(getInsertSql());
				final PreparedStatement minerPstmt = minerConn.prepareStatement("SELECT CURRENT_SCN FROM v$database")     
				)
		{
			long t0 = System.currentTimeMillis();
			sourceConn.setAutoCommit(false);
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

					sinkPstmt.setLong(1, rs.getLong("IMAGE_ID"));
					
					longValue = rs.getLong("POLICY_ID");
					if (rs.wasNull()) {
						sinkPstmt.setNull(2, Types.BIGINT);
					} else {
						sinkPstmt.setLong(2, longValue);
					}
					
					longValue = rs.getLong("IMAGE_TYPE_ID");
					if (rs.wasNull()) {
						sinkPstmt.setNull(3, Types.BIGINT);
					} else {
						sinkPstmt.setLong(3, longValue);
					}
					
					longValue = rs.getLong("SEQ_NUMBER");
					if (rs.wasNull()) {
						sinkPstmt.setNull(4, Types.BIGINT);
					} else {
						sinkPstmt.setLong(4, longValue);
					}
				
					sinkPstmt.setString(5, rs.getString("IMAGE_FORMAT"));
					sinkPstmt.setString(6, null);
					sinkPstmt.setDate(7, rs.getDate("SCAN_TIME"));
					
					longValue = rs.getLong("EMP_ID");
					if (rs.wasNull()) {
						sinkPstmt.setNull(8, Types.BIGINT);
					} else {
						sinkPstmt.setLong(8, longValue);
					}
					
					longValue = rs.getLong("HEAD_ID");
					if (rs.wasNull()) {
						sinkPstmt.setNull(9, Types.BIGINT);
					} else {
						sinkPstmt.setLong(9, longValue);
					}
					
					sinkPstmt.setString(10, rs.getString("FILE_CODE"));
					sinkPstmt.setString(11, rs.getString("PROCESS_STATUS"));
					
					longValue = rs.getLong("GROUP_POLICY_ID");
					if (rs.wasNull()) {
						sinkPstmt.setNull(12, Types.BIGINT);
					} else {
						sinkPstmt.setLong(12, longValue);
					}
					
					longValue = rs.getLong("CASE_ID");
					if (rs.wasNull()) {
						sinkPstmt.setNull(13, Types.BIGINT);
					} else {
						sinkPstmt.setLong(13, longValue);
					}
					
					longValue = rs.getLong("CHANGE_ID");
					if (rs.wasNull()) {
						sinkPstmt.setNull(14, Types.BIGINT);
					} else {
						sinkPstmt.setLong(14, longValue);
					}
					
					longValue = rs.getLong("IMAGE_LOCATION");
					if (rs.wasNull()) {
						sinkPstmt.setNull(15, Types.BIGINT);
					} else {
						sinkPstmt.setLong(15, longValue);
					}
					
					sinkPstmt.setString(16, rs.getString("IMAGE_FILE_NAME"));
					sinkPstmt.setString(17, rs.getString("AUTH_CODE"));
					sinkPstmt.setString(18, rs.getString("ORGAN_ID"));
					sinkPstmt.setString(19, rs.getString("SUB_FILE_CODE"));
					sinkPstmt.setString(20, rs.getString("BUSINESS_ORGAN"));
					sinkPstmt.setString(21, rs.getString("IS_PRIORITY"));
					sinkPstmt.setDate(22, rs.getDate("ZIP_DATE"));
					sinkPstmt.setString(23, rs.getString("IS_CLEAR"));
					
					longValue = rs.getLong("LIST_ID");
					if (rs.wasNull()) {
						sinkPstmt.setNull(24, Types.BIGINT);
					} else {
						sinkPstmt.setLong(24, longValue);
					}
					
					sinkPstmt.setDate(25, rs.getDate("INSERT_TIME"));
					
					longValue = rs.getLong("INSERTED_BY");
					if (rs.wasNull()) {
						sinkPstmt.setNull(26, Types.BIGINT);
					} else {
						sinkPstmt.setLong(26, longValue);
					}
					
					sinkPstmt.setDate(27, rs.getDate("UPDATE_TIME"));
					
					longValue = rs.getLong("UPDATED_BY");
					if (rs.wasNull()) {
						sinkPstmt.setNull(28, Types.BIGINT);
					} else {
						sinkPstmt.setLong(28, longValue);
					}
					
					sinkPstmt.setDate(29, rs.getDate("INSERT_TIMESTAMP"));
					sinkPstmt.setDate(30, rs.getDate("UPDATE_TIMESTAMP"));
					
					longValue = rs.getLong("DEPT_ID");
					if (rs.wasNull()) {
						sinkPstmt.setNull(31, Types.BIGINT);
					} else {
						sinkPstmt.setLong(31, longValue);
					}
			
					sinkPstmt.setString(32, rs.getString("COMPANY_CODE"));
					sinkPstmt.setString(33, rs.getString("PERSONAL_CODE"));
					sinkPstmt.setString(34, rs.getString("BATCH_DEPT_TYPE"));
					sinkPstmt.setString(35, rs.getString("BATCH_DATE"));
					sinkPstmt.setString(36, rs.getString("BATCH_AREA"));
					sinkPstmt.setString(37, rs.getString("BATCH_DOC_TYPE"));
					sinkPstmt.setString(38, rs.getString("BOX_NO"));
					sinkPstmt.setString(39, rs.getString("REMARK"));
					sinkPstmt.setString(40, rs.getString("SIGNATURE"));
					
					longValue = rs.getLong("REAL_WIDTH");
					if (rs.wasNull()) {
						sinkPstmt.setNull(41, Types.BIGINT);
					} else {
						sinkPstmt.setLong(41, longValue);
					}
					
					longValue = rs.getLong("SIG_SEQ_NUMBER");
					if (rs.wasNull()) {
						sinkPstmt.setNull(42, Types.BIGINT);
					} else {
						sinkPstmt.setLong(42, longValue);
					}
					
					longValue = rs.getLong("SCAN_ORDER");
					if (rs.wasNull()) {
						sinkPstmt.setNull(43, Types.BIGINT);
					} else {
						sinkPstmt.setLong(43, longValue);
					}
					
					sinkPstmt.setDate(44, dataDate);
					
					// db current_time for tbl_upd_time 
					
					sinkPstmt.setLong(45, currentScn); 				// new column
					
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
		return loadBean;
	}	

}
