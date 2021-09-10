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

public class JbpmVariableinstanceDataLoader extends DataLoader {

	private static final Logger logger = LoggerFactory.getLogger(JbpmVariableinstanceDataLoader.class);

	private String sourceTableName ;

	private String sinkTableName;

	private String streamingEtlName;

	private String sinkTableCreateFile;

	private String sinkTableIndexesFile;

	public JbpmVariableinstanceDataLoader(Config config, Date dataDate) throws Exception {

		super(DataLoader.DEFAULT_THREADS, DataLoader.DEFAULT_BATCH_COMMIT_SIZE, config, dataDate);

		this.sourceTableName = config.sourceTableJbpmVariableinstance;

		this.sinkTableName = config.sinkTableKJbpmVariableinstance;

		this.streamingEtlName = config.streamingEtlNameJbpmVariableinstance;

		this.sinkTableCreateFile = config.sinkTableCreateFileKJbpmVariableinstance;

		this.sinkTableIndexesFile = config.sinkTableIndexesFileKJbpmVariableinstance;
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
		return "select min(ID_) as MIN_ID from " + this.sourceTableName;
	}

	@Override
	protected String getSelectMaxIdSql() {
		return "select max(ID_) as MAX_ID from " + this.sourceTableName;
	}

	@Override
	protected String getCountSql() {
		return "select count(*) as CNT from " + this.sourceTableName + " a where ? <= a.ID_ and a.ID_ < ?";
	}

	@Override
	protected String getSelectSql() {
		// TODO Auto-generated method stub
		return "select"
		+ " ID_"
		+ ",CLASS_"
		+ ",VERSION_"
		+ ",NAME_"
		+ ",CONVERTER_"
		+ ",TOKEN_"
		+ ",TOKENVARIABLEMAP_"
		+ ",PROCESSINSTANCE_"
		+ ",BYTEARRAYVALUE_"
		+ ",DATEVALUE_"
		+ ",DOUBLEVALUE_"
		+ ",LONGIDCLASS_"
		+ ",LONGVALUE_"
		+ ",STRINGIDCLASS_"
		+ ",STRINGVALUE_"
		+ ",TASKINSTANCE_"
		+ " from " + this.sourceTableName
		+ " a where ? <= a.ID_ and a.ID_ < ?";
	}

	@Override
	protected String getInsertSql() {
		return "insert into " + this.sinkTableName
				+ " (ID_"
				+ ",CLASS_"
				+ ",VERSION_"
				+ ",NAME_"
				+ ",CONVERTER_"
				+ ",TOKEN_"
				+ ",TOKENVARIABLEMAP_"
				+ ",PROCESSINSTANCE_"
				+ ",BYTEARRAYVALUE_"
				+ ",DATEVALUE_"
				+ ",DOUBLEVALUE_"
				+ ",LONGIDCLASS_"
				+ ",LONGVALUE_"
				+ ",STRINGIDCLASS_"
				+ ",STRINGVALUE_"
				+ ",TASKINSTANCE_"
				+ ",DATA_DATE"				// ods add column
				+ ",TBL_UPD_TIME"			// ods add column
				+ ",SCN"		// new column
				+ ",COMMIT_SCN"	// new column
				+ ",ROW_ID)"	// new column
				+ " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_DATE,?,?,NULL)";
			
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
				Long longValue;
				while (rs.next())
				{
					count++;

					sinkPstmt.clearParameters();
					
					sinkPstmt.setBigDecimal(1, rs.getBigDecimal("ID_"));
					sinkPstmt.setString(2, rs.getString("CLASS_"));
					sinkPstmt.setBigDecimal(3, rs.getBigDecimal("VERSION_"));
					sinkPstmt.setString(4, rs.getString("NAME_"));
					sinkPstmt.setString(5, rs.getString("CONVERTER_"));
					sinkPstmt.setBigDecimal(6, rs.getBigDecimal("TOKEN_"));
					sinkPstmt.setBigDecimal(7, rs.getBigDecimal("TOKENVARIABLEMAP_"));
					sinkPstmt.setBigDecimal(8, rs.getBigDecimal("PROCESSINSTANCE_"));
					sinkPstmt.setBigDecimal(9, rs.getBigDecimal("BYTEARRAYVALUE_"));
					sinkPstmt.setTimestamp(10, rs.getTimestamp("DATEVALUE_"));
					sinkPstmt.setFloat(11, rs.getFloat("DOUBLEVALUE_"));
					sinkPstmt.setString(12, rs.getString("LONGIDCLASS_"));
					sinkPstmt.setBigDecimal(13, rs.getBigDecimal("LONGVALUE_"));
					sinkPstmt.setString(14, rs.getString("STRINGIDCLASS_"));
					sinkPstmt.setString(15, rs.getString("STRINGVALUE_"));
					sinkPstmt.setBigDecimal(16, rs.getBigDecimal("TASKINSTANCE_"));
					sinkPstmt.setDate(17, dataDate);
					
					// db current_time for tbl_upd_time 
					
					sinkPstmt.setLong(18, loadBean.currentScn);				// new column
					sinkPstmt.setLong(19, loadBean.currentScn);				// new column// new column
					
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
