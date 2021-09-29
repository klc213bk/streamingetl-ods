package com.transglobe.streamingetl.ods.update.bean;

import java.io.Console;
import java.math.BigDecimal;
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
import com.transglobe.streamingetl.ods.load.bean.LoadBean;

public class TPolicyPrintJobUpdateDataLoader extends UpdateDataLoader {

	private static final Logger logger = LoggerFactory.getLogger(TPolicyPrintJobUpdateDataLoader.class);

	private static final String UPDATE_TABLE_NAME = "T_POLICY_PRINT_JOB_UPDATE";

	private static final String UPDATE_TABLE_CREATE_FILE_NAME = "update/createtable-T_POLICY_PRINT_JOB_UPDATE.sql";

	private String sourceTableName ;

	private String fromUpdateTime;

	private String toUpdateTime;

	public TPolicyPrintJobUpdateDataLoader(Config config, String fromUpdateTime, String toUpdateTime) throws Exception {

		super(config, fromUpdateTime, toUpdateTime);

		this.sourceTableName = config.sourceTableTPolicyPrintJob;
		this.fromUpdateTime = fromUpdateTime;
		this.toUpdateTime = toUpdateTime;
	}

	@Override
	public String getSourceTableName() {
		return this.sourceTableName;
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
				+ ",REMAKE_ID"
				+ ",CHECK_ID"
				+ ",EVA_LIST_ID"
				+ ",URGENT_INDI"
				+ ",ORA_ROWSCN"
				+ ",ROWID"
				+ " from " + this.sourceTableName
				+ " a where ? <= a.JOB_ID and a.JOB_ID < ? and to_date(?, 'YYYY-MM-DD') <= UPDATE_TIME and UPDATE_TIME < to_date(?, 'YYYY-MM-DD')";
	}

	@Override
	protected String getInsertSql() {
		return "insert into " + UPDATE_TABLE_NAME
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
				+ ",REMAKE_ID"
				+ ",CHECK_ID"
				+ ",EVA_LIST_ID"
				+ ",URGENT_INDI"
				+ ",SCN"		// new column
				+ ",ROW_ID)"	// new column
				+ " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?)";

	}

	@Override
	protected void transferData(LoadBean loadBean, BasicDataSource sourceConnectionPool,
			BasicDataSource sinkConnectionPool) throws Exception {
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
			sourcePstmt.setString(3, fromUpdateTime);
			sourcePstmt.setString(4, toUpdateTime);
			try (final ResultSet rs =
					sourcePstmt.executeQuery())
			{
				Long count = 0L;
				while (rs.next())
				{
					count++;

					sinkPstmt.clearParameters();

					sinkPstmt.setBigDecimal(1, rs.getBigDecimal("JOB_ID"));
					sinkPstmt.setBigDecimal(2, rs.getBigDecimal("POLICY_ID"));
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
					sinkPstmt.setBigDecimal(14, rs.getBigDecimal("PRINT_CATEGORY"));
					sinkPstmt.setBigDecimal(15, rs.getBigDecimal("PRINT_TYPE"));			
					sinkPstmt.setString(16, rs.getString("PRINT_COPYSET"));
					sinkPstmt.setString(17, rs.getString("PRINT_REASON"));
					sinkPstmt.setString(18, rs.getString("VALID_STATUS"));
					sinkPstmt.setDate(19, rs.getDate("PRINT_DATE"));
					sinkPstmt.setBigDecimal(20, rs.getBigDecimal("OPERATOR_ID"));
					sinkPstmt.setDate(21, rs.getDate("INSERT_TIME"));
					sinkPstmt.setBigDecimal(22, rs.getBigDecimal("INSERTED_BY"));
					sinkPstmt.setDate(23, rs.getDate("INSERT_TIMESTAMP"));

					sinkPstmt.setBigDecimal(24, rs.getBigDecimal("UPDATED_BY"));
					
					sinkPstmt.setDate(25, rs.getDate("UPDATE_TIMESTAMP"));
					sinkPstmt.setString(26, rs.getString("PREMVOUCHER_INDI"));

					sinkPstmt.setBigDecimal(27, rs.getBigDecimal("ARCHIVE_ID"));
				
					sinkPstmt.setDate(28, rs.getDate("UPDATE_TIME"));
					sinkPstmt.setString(29, rs.getString("JOB_TYPE_DESC"));
					sinkPstmt.setDate(30, rs.getDate("JOB_READY_DATE"));

					Clob clob = rs.getClob("CONTENT");
					if (rs.wasNull()) {
						sinkPstmt.setNull(31, Types.CLOB);
					} else {
						sinkPstmt.setCharacterStream(31, clob.getCharacterStream());
					}

					sinkPstmt.setBigDecimal(32, rs.getBigDecimal("COPY"));
					sinkPstmt.setBigDecimal(33, rs.getBigDecimal("ERROR_CODE"));		
					sinkPstmt.setString(34, rs.getString("LANG_ID"));
					sinkPstmt.setBigDecimal(35, rs.getBigDecimal("CHANGE_ID"));
					sinkPstmt.setBigDecimal(36, rs.getBigDecimal("PRINT_COMP_INDI"));		
					
					sinkPstmt.setBigDecimal(37, rs.getBigDecimal("REMAKE_ID"));
					sinkPstmt.setBigDecimal(38, rs.getBigDecimal("CHECK_ID"));
					sinkPstmt.setBigDecimal(39, rs.getBigDecimal("EVA_LIST_ID"));
					sinkPstmt.setString(40, rs.getString("URGENT_INDI"));
					
					sinkPstmt.setLong(41, rs.getLong("ORA_ROWSCN"));				// new column
					sinkPstmt.setString(42,  rs.getString("ROWID"));		// new column

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
							UPDATE_TABLE_NAME, loadBean.count, loadBean.seq, loadBean.loadBeanSize, loadBean.startSeq, loadBean.endSeq, span);
					cnsl.flush();
				} else {
					long span = System.currentTimeMillis() - t0;
					cnsl = System.console();
					cnsl.printf("### %s count=%d, loadbeanseq=%d, startSeq=%d, endSeq=%d, span=%d\n", 
							UPDATE_TABLE_NAME, loadBean.count, loadBean.seq, loadBean.startSeq, loadBean.endSeq, span);
					cnsl.flush();
				}
			} catch (Exception e) {
				sinkConn.rollback();
				throw e;
			}

		} 
	}

	@Override
	protected String getUpdateTableCreateFileName() {
		return UPDATE_TABLE_CREATE_FILE_NAME;
	}

	@Override
	protected String getUpdateTableName() {
		return UPDATE_TABLE_NAME;
	}	

}
