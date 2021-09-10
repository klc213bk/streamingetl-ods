package com.transglobe.streamingetl.ods.load.bean;

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

public class TContractExtendCxDataLoader extends DataLoader {

	private static final Logger logger = LoggerFactory.getLogger(TContractExtendCxDataLoader.class);

	private String sourceTableName ;

	private String sinkTableName;

	private String streamingEtlName;

	private String sinkTableCreateFile;

	private String sinkTableIndexesFile;

	public TContractExtendCxDataLoader(Config config, Date dataDate) throws Exception {

		super(DataLoader.DEFAULT_THREADS, DataLoader.DEFAULT_BATCH_COMMIT_SIZE, config, dataDate);

		this.sourceTableName = config.sourceTableTContractExtendCx;

		this.sinkTableName = config.sinkTableKContractExtendCx;

		this.streamingEtlName = config.streamingEtlNameTContractExtendCx;

		this.sinkTableCreateFile = config.sinkTableCreateFileKContractExtendCx;

		this.sinkTableIndexesFile = config.sinkTableIndexesFileKContractExtendCx;
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
		// TODO Auto-generated method stub
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
		+ " from " + this.sourceTableName
		+ " a where ? <= a.LOG_ID and a.LOG_ID < ?";
	}

	@Override
	protected String getInsertSql() {
		return "insert into " + this.sinkTableName
				+ " (POLICY_CHG_ID" 
				+ ",ITEM_ID"
				+ ",POLICY_ID" 
				+ ",CHANGE_ID" 
				+ ",LOG_ID"
				+ ",PRE_LOG_ID" 
				+ ",OPER_TYPE"
				+ ",LOG_TYPE" 
				+ ",CHANGE_SEQ" 
				+ ",DATA_DATE" 		// ods add column 
				+ ",TBL_UPD_TIME"	// ods add column
				+ ",SCN"		// new column
				+ ",COMMIT_SCN"	// new column
				+ ",ROW_ID)"	// new column
				+ " values (?,?,?,?,?,?,?,?,?,?,CURRENT_DATE,?,?,NULL)";

	}

	@Override
	protected void transferData(LoadBean loadBean, BasicDataSource sourceConnectionPool,
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
					sinkPstmt.setDate(10, dataDate);
					//TBL_UPD_TIME
					sinkPstmt.setLong(11, loadBean.currentScn);				// new column				// new column
					sinkPstmt.setLong(12, loadBean.currentScn);				// new column
					
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
