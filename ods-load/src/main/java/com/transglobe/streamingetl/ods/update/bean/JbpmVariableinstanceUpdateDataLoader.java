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

public class JbpmVariableinstanceUpdateDataLoader extends UpdateDataLoader {

	private static final Logger logger = LoggerFactory.getLogger(JbpmVariableinstanceUpdateDataLoader.class);

	private static final String UPDATE_TABLE_NAME = "JBPM_VARIABLEINSTANCE_UPDATE";

	private static final String UPDATE_TABLE_CREATE_FILE_NAME = "update/createtable-JBPM_VARIABLEINSTANCE_UPDATE.sql";

	private String sourceTableName ;

	private String fromUpdateTime;

	private String toUpdateTime;

	public JbpmVariableinstanceUpdateDataLoader(Config config, String fromUpdateTime, String toUpdateTime) throws Exception {

		super(config, fromUpdateTime, toUpdateTime);

		this.sourceTableName = config.sourceTableJbpmVariableinstance;
		this.fromUpdateTime = fromUpdateTime;
		this.toUpdateTime = toUpdateTime;
	}

	@Override
	public String getSourceTableName() {
		return this.sourceTableName;
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
				+ ",ORA_ROWSCN"
				+ ",ROWID"
				+ " from " + this.sourceTableName
				+ " a where ? <= a.ID_ and a.ID_ < ? and to_date(?, 'YYYY-MM-DD') <= UPDATE_TIME and UPDATE_TIME < to_date(?, 'YYYY-MM-DD')";
	}

	@Override
	protected String getInsertSql() {
		return "insert into " + UPDATE_TABLE_NAME
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
				+ ",SCN"		// new column
				+ ",ROW_ID)"	// new column
				+ " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

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
					
					sinkPstmt.setLong(17, rs.getLong("ORA_ROWSCN"));				// new column// new column
					sinkPstmt.setString(18, rs.getString("ROWID"));				// new column// new column
							
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
