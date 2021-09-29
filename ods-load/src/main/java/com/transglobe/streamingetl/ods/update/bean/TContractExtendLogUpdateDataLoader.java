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

public class TContractExtendLogUpdateDataLoader extends UpdateDataLoader {

	private static final Logger logger = LoggerFactory.getLogger(TContractExtendLogUpdateDataLoader.class);

	private static final String UPDATE_TABLE_NAME = "T_CONTRACT_EXTEND_LOG_UPDATE";

	private static final String UPDATE_TABLE_CREATE_FILE_NAME = "update/createtable-T_CONTRACT_EXTEND_LOG_UPDATE.sql";

	private String sourceTableName ;

	private String fromUpdateTime;

	private String toUpdateTime;

	public TContractExtendLogUpdateDataLoader(Config config, String fromUpdateTime, String toUpdateTime) throws Exception {

		super(config, fromUpdateTime, toUpdateTime);

		this.sourceTableName = config.sourceTableTContractExtendLog;
		this.fromUpdateTime = fromUpdateTime;
		this.toUpdateTime = toUpdateTime;
	}

	@Override
	public String getSourceTableName() {
		return this.sourceTableName;
	}

	@Override
	protected String getSelectMinIdSql() {
		return "select min(LOG_ID) as MIN_ID from " + this.sourceTableName;
	}

	@Override
	protected String getSelectMaxIdSql() {
		return "select max(LOG_ID) as MAX_ID from " + this.sourceTableName;
	}

	@Override
	protected String getCountSql() {
		return "select count(*) as CNT from " + this.sourceTableName + " a where ? <= a.LOG_ID and a.LOG_ID < ?";
	}

	@Override
	protected String getSelectSql() {
		return "select"
				+ " CHANGE_ID"
				+ ",LOG_TYPE"
				+ ",POLICY_CHG_ID"
				+ ",ITEM_ID"
				+ ",DUE_DATE"
				+ ",POLICY_YEAR"
				+ ",POLICY_PERIOD"
				+ ",STRGY_DUE_DATE"
				+ ",PREM_STATUS"
				+ ",LOG_ID"
				+ ",SA_DUE_DATE"
				+ ",LAST_CMT_FLG"
				+ ",EMS_VERSION"
				+ ",INSERTED_BY"
				+ ",UPDATED_BY"
				+ ",INSERT_TIME"
				+ ",UPDATE_TIME"
				+ ",INSERT_TIMESTAMP"
				+ ",UPDATE_TIMESTAMP"
				+ ",POLICY_ID"
				+ ",BILLING_DATE"
				+ ",REMINDER_DATE"
				+ ",INDX_DUE_DATE"
				+ ",INDX_REJECT"
				+ ",INSURABILITY_DUE_DATE"
				+ ",INSURABILITY_REJECT_COUNT"
				+ ",INSURABILITY_REJECT_REASON"
				+ ",BILL_TO_DATE"
				+ ",BUCKET_FILLING_DUE_DATE"
				+ ",ILP_DUE_DATE"
				+ ",WAIVER_SOURCE"
				+ ",ORA_ROWSCN"
				+ ",ROWID"
				+ " from " + this.sourceTableName
				+ " a where ? <= a.LOG_ID and a.LOG_ID < ? and to_date(?, 'YYYY-MM-DD') <= UPDATE_TIME and UPDATE_TIME < to_date(?, 'YYYY-MM-DD')";
	}

	@Override
	protected String getInsertSql() {
		return "insert into " + UPDATE_TABLE_NAME
				+ " (CHANGE_ID"
				+ ",LOG_TYPE"
				+ ",POLICY_CHG_ID"
				+ ",ITEM_ID"
				+ ",DUE_DATE"
				+ ",POLICY_YEAR"
				+ ",POLICY_PERIOD"
				+ ",STRGY_DUE_DATE"
				+ ",PREM_STATUS"
				+ ",LOG_ID"
				+ ",SA_DUE_DATE"
				+ ",LAST_CMT_FLG"
				+ ",EMS_VERSION"
				+ ",INSERTED_BY"
				+ ",UPDATED_BY"
				+ ",INSERT_TIME"
				+ ",UPDATE_TIME"
				+ ",INSERT_TIMESTAMP"
				+ ",UPDATE_TIMESTAMP"
				+ ",POLICY_ID"
				+ ",BILLING_DATE"
				+ ",REMINDER_DATE"
				+ ",INDX_DUE_DATE"
				+ ",INDX_REJECT"
				+ ",INSURABILITY_DUE_DATE"
				+ ",INSURABILITY_REJECT_COUNT"
				+ ",INSURABILITY_REJECT_REASON"
				+ ",BILL_TO_DATE"
				+ ",BUCKET_FILLING_DUE_DATE"
				+ ",ILP_DUE_DATE"
				+ ",WAIVER_SOURCE"
				+ ",SCN"		// new column
				+ ",ROW_ID)"	// new column
				+ " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,?,?,?,?,?,?,?,?,?)";

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

					sinkPstmt.setBigDecimal(1, rs.getBigDecimal("CHANGE_ID"));
					sinkPstmt.setString(2, rs.getString("LOG_TYPE"));
					sinkPstmt.setBigDecimal(3, rs.getBigDecimal("POLICY_CHG_ID"));
					sinkPstmt.setBigDecimal(4, rs.getBigDecimal("ITEM_ID"));
					sinkPstmt.setDate(5, rs.getDate("DUE_DATE"));
					sinkPstmt.setBigDecimal(6, rs.getBigDecimal("POLICY_YEAR"));
					sinkPstmt.setBigDecimal(7, rs.getBigDecimal("POLICY_PERIOD"));
					sinkPstmt.setDate(8, rs.getDate("STRGY_DUE_DATE"));
					sinkPstmt.setBigDecimal(9, rs.getBigDecimal("PREM_STATUS"));
					sinkPstmt.setBigDecimal(10, rs.getBigDecimal("LOG_ID"));
					sinkPstmt.setDate(11, rs.getDate("SA_DUE_DATE"));
					sinkPstmt.setString(12, rs.getString("LAST_CMT_FLG"));
					sinkPstmt.setBigDecimal(13, rs.getBigDecimal("EMS_VERSION"));
					sinkPstmt.setBigDecimal(14, rs.getBigDecimal("INSERTED_BY"));
					sinkPstmt.setBigDecimal(15, rs.getBigDecimal("UPDATED_BY"));
					sinkPstmt.setDate(16, rs.getDate("INSERT_TIME"));
					sinkPstmt.setDate(17, rs.getDate("UPDATE_TIME"));
					sinkPstmt.setDate(18, rs.getDate("INSERT_TIMESTAMP"));
					sinkPstmt.setDate(19, rs.getDate("UPDATE_TIMESTAMP"));
					sinkPstmt.setBigDecimal(20, rs.getBigDecimal("POLICY_ID"));
					sinkPstmt.setDate(21, rs.getDate("BILLING_DATE"));
					sinkPstmt.setDate(22, rs.getDate("REMINDER_DATE"));
					sinkPstmt.setDate(23, rs.getDate("INDX_DUE_DATE"));
					sinkPstmt.setBigDecimal(24, rs.getBigDecimal("INDX_REJECT"));
					sinkPstmt.setDate(25, rs.getDate("INSURABILITY_DUE_DATE"));
					sinkPstmt.setBigDecimal(26, rs.getBigDecimal("INSURABILITY_REJECT_COUNT"));
					sinkPstmt.setString(27, rs.getString("INSURABILITY_REJECT_REASON"));
					sinkPstmt.setDate(28, rs.getDate("BILL_TO_DATE"));
					sinkPstmt.setDate(29, rs.getDate("BUCKET_FILLING_DUE_DATE"));
					sinkPstmt.setDate(30, rs.getDate("ILP_DUE_DATE"));
					sinkPstmt.setBigDecimal(31, rs.getBigDecimal("WAIVER_SOURCE"));

					sinkPstmt.setLong(32, rs.getLong("ORA_ROWSCN"));		// new column
					sinkPstmt.setString(33,  rs.getString("ROWID"));		// new column
					
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
