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

public class TContractExtendCxUpdateDataLoader extends UpdateDataLoader {

	private static final Logger logger = LoggerFactory.getLogger(TContractExtendCxUpdateDataLoader.class);

	private static final String UPDATE_TABLE_NAME = "T_CONTRACT_EXTEND_CX_UPDATE";

	private static final String UPDATE_TABLE_CREATE_FILE_NAME = "update/createtable-T_CONTRACT_EXTEND_CX_UPDATE.sql";

	private String sourceTableName ;

	private String fromUpdateTime;

	private String toUpdateTime;

	public TContractExtendCxUpdateDataLoader(Config config, String fromUpdateTime, String toUpdateTime) throws Exception {

		super(config, fromUpdateTime, toUpdateTime);

		this.sourceTableName = config.sourceTableTCommisionFee;
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
				+ " POLICY_CHG_ID"
				+ ",ITEM_ID"
				+ ",POLICY_ID"
				+ ",CHANGE_ID"
				+ ",LOG_ID"
				+ ",PRE_LOG_ID"
				+ ",OPER_TYPE"
				+ ",LOG_TYPE"
				+ ",CHANGE_SEQ"
				+ ",ORA_ROWSCN"
				+ ",ROWID"
				+ " from " + this.sourceTableName
				+ " a where ? <= a.LOG_ID and a.LOG_ID < ? and to_date(?, 'YYYY-MM-DD') <= UPDATE_TIME and UPDATE_TIME < to_date(?, 'YYYY-MM-DD')";
	}

	@Override
	protected String getInsertSql() {
		return "insert into " + UPDATE_TABLE_NAME
				+ " (POLICY_CHG_ID"
				+ ",ITEM_ID"
				+ ",POLICY_ID"
				+ ",CHANGE_ID"
				+ ",LOG_ID"
				+ ",PRE_LOG_ID"
				+ ",OPER_TYPE"
				+ ",LOG_TYPE"
				+ ",CHANGE_SEQ"
				+ ",SCN"		// new column
				+ ",ROW_ID)"	// new column
				+ " values (?,?,?,?,?,?,?,?,?,?,?)";

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

					sinkPstmt.setBigDecimal(1, rs.getBigDecimal("POLICY_CHG_ID"));
					sinkPstmt.setBigDecimal(2, rs.getBigDecimal("ITEM_ID"));
					sinkPstmt.setBigDecimal(3, rs.getBigDecimal("POLICY_ID"));
					sinkPstmt.setBigDecimal(4, rs.getBigDecimal("CHANGE_ID"));
					sinkPstmt.setBigDecimal(5, rs.getBigDecimal("LOG_ID"));
					sinkPstmt.setBigDecimal(6, rs.getBigDecimal("PRE_LOG_ID"));
					sinkPstmt.setString(7, rs.getString("OPER_TYPE"));
					sinkPstmt.setString(8, rs.getString("LOG_TYPE"));
					sinkPstmt.setBigDecimal(9, rs.getBigDecimal("CHANGE_SEQ"));
					sinkPstmt.setLong(10, rs.getLong("ORA_ROWSCN"));		// new column
					sinkPstmt.setString(11,  rs.getString("ROWID"));		// new column

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
