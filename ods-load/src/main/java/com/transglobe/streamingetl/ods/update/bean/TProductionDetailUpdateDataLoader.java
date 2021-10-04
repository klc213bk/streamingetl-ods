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

public class TProductionDetailUpdateDataLoader extends UpdateDataLoader {

	private static final Logger logger = LoggerFactory.getLogger(TProductionDetailUpdateDataLoader.class);

	private static final String UPDATE_TABLE_NAME = "T_PRODUCTION_DETAIL_UPDATE";

	private static final String UPDATE_TABLE_CREATE_FILE_NAME = "update/createtable-T_PRODUCTION_DETAIL_UPDATE.sql";

	private String sourceTableName ;

	private String fromUpdateTime;

	private String toUpdateTime;

	public TProductionDetailUpdateDataLoader(Config config, String fromUpdateTime, String toUpdateTime) throws Exception {

		super(config, fromUpdateTime, toUpdateTime);

		this.sourceTableName = config.sourceTableTProductionDetail;
		this.fromUpdateTime = fromUpdateTime;
		this.toUpdateTime = toUpdateTime;
	}

	@Override
	public String getSourceTableName() {
		return this.sourceTableName;
	}

	@Override
	protected String getSelectMinIdSql() {
		return "select min(DETAIL_ID) as MIN_ID from " + this.sourceTableName;
	}

	@Override
	protected String getSelectMaxIdSql() {
		return "select max(DETAIL_ID) as MAX_ID from " + this.sourceTableName;
	}

	@Override
	protected String getCountSql() {
		return "select count(*) as CNT from " + this.sourceTableName + " a where ? <= a.DETAIL_ID and a.DETAIL_ID < ?";
	}

	@Override
	protected String getSelectSql() {
		return "select"
				+ " DETAIL_ID"
				+ ",PRODUCTION_ID"
				+ ",POLICY_ID"
				+ ",ITEM_ID"
				+ ",PRODUCT_ID"
				+ ",POLICY_YEAR"
				+ ",PRODUCTION_VALUE"
				+ ",EFFECTIVE_DATE"
				+ ",HIERARCHY_DATE"
				+ ",PRODUCER_ID"
				+ ",PRODUCER_POSITION"
				+ ",BENEFIT_TYPE"
				+ ",FEE_TYPE"
				+ ",CHARGE_MODE"
				+ ",PREM_LIST_ID"
				+ ",COMM_ITEM_ID"
				+ ",POLICY_CHG_ID"
				+ ",EXCHANGE_RATE"
				+ ",RELATED_ID"
				+ ",INSURED_ID"
				+ ",POL_PRODUCTION_VALUE"
				+ ",POL_CURRENCY_ID"
				+ ",HIERARCHY_EXIST_INDI"
				+ ",AGGREGATION_ID"
				+ ",PRODUCT_VERSION"
				+ ",SOURCE_TABLE"
				+ ",SOURCE_ID"
				+ ",RESULT_LIST_ID"
				+ ",FINISH_TIME"
				+ ",INSERTED_BY"
				+ ",UPDATED_BY"
				+ ",INSERT_TIME"
				+ ",UPDATE_TIME"
				+ ",INSERT_TIMESTAMP"
				+ ",UPDATE_TIMESTAMP"
				+ ",COMMISSION_RATE"
				+ ",CHEQUE_INDI"
				+ ",PREM_ALLOCATE_YEAR"
				+ ",RECALCULATED_INDI"
				+ ",EXCLUDE_POLICY_INDI"
				+ ",CHANNEL_ORG_ID"
				+ ",AGENT_CATE"
				+ ",YEAR_MONTH"
				+ ",CONVERSION_CATE"
				+ ",ORDER_ID"
				+ ",ASSIGN_RATE"
				+ ",ORA_ROWSCN"
				+ ",ROWID"
				+ " from " + this.sourceTableName
				+ " a where ? <= a.DETAIL_ID and a.DETAIL_ID < ? and to_date(?, 'YYYY-MM-DD') <= UPDATE_TIME and UPDATE_TIME < to_date(?, 'YYYY-MM-DD')";
	}

	@Override
	protected String getInsertSql() {
		return "insert into " + UPDATE_TABLE_NAME
				+ " (DETAIL_ID"
				+ ",PRODUCTION_ID"
				+ ",POLICY_ID"
				+ ",ITEM_ID"
				+ ",PRODUCT_ID"
				+ ",POLICY_YEAR"
				+ ",PRODUCTION_VALUE"
				+ ",EFFECTIVE_DATE"
				+ ",HIERARCHY_DATE"
				+ ",PRODUCER_ID"
				+ ",PRODUCER_POSITION"
				+ ",BENEFIT_TYPE"
				+ ",FEE_TYPE"
				+ ",CHARGE_MODE"
				+ ",PREM_LIST_ID"
				+ ",COMM_ITEM_ID"
				+ ",POLICY_CHG_ID"
				+ ",EXCHANGE_RATE"
				+ ",RELATED_ID"
				+ ",INSURED_ID"
				+ ",POL_PRODUCTION_VALUE"
				+ ",POL_CURRENCY_ID"
				+ ",HIERARCHY_EXIST_INDI"
				+ ",AGGREGATION_ID"
				+ ",PRODUCT_VERSION"
				+ ",SOURCE_TABLE"
				+ ",SOURCE_ID"
				+ ",RESULT_LIST_ID"
				+ ",FINISH_TIME"
				+ ",INSERTED_BY"
				+ ",UPDATED_BY"
				+ ",INSERT_TIME"
				+ ",UPDATE_TIME"
				+ ",INSERT_TIMESTAMP"
				+ ",UPDATE_TIMESTAMP"
				+ ",COMMISSION_RATE"
				+ ",CHEQUE_INDI"
				+ ",PREM_ALLOCATE_YEAR"
				+ ",RECALCULATED_INDI"
				+ ",EXCLUDE_POLICY_INDI"
				+ ",CHANNEL_ORG_ID"
				+ ",AGENT_CATE"
				+ ",YEAR_MONTH"
				+ ",CONVERSION_CATE"
				+ ",ORDER_ID"
				+ ",ASSIGN_RATE"
				+ ",SCN"		// new column
				+ ",ROW_ID)"	// new column
				+ " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,?,?,?,?)";

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

sinkPstmt.setLong(1, rs.getLong("DETAIL_ID"));
					
					sinkPstmt.setBigDecimal(2, rs.getBigDecimal("PRODUCTION_ID"));
					sinkPstmt.setBigDecimal(3, rs.getBigDecimal("POLICY_ID"));
					sinkPstmt.setBigDecimal(4, rs.getBigDecimal("ITEM_ID"));
					sinkPstmt.setBigDecimal(5, rs.getBigDecimal("PRODUCT_ID"));
					sinkPstmt.setBigDecimal(6, rs.getBigDecimal("POLICY_YEAR"));
					sinkPstmt.setBigDecimal(7, rs.getBigDecimal("PRODUCTION_VALUE"));
				
					sinkPstmt.setDate(8, rs.getDate("EFFECTIVE_DATE"));
					sinkPstmt.setDate(9, rs.getDate("HIERARCHY_DATE"));
					sinkPstmt.setBigDecimal(10, rs.getBigDecimal("PRODUCER_ID"));			
					sinkPstmt.setString(11, rs.getString("PRODUCER_POSITION"));
					sinkPstmt.setString(12, rs.getString("BENEFIT_TYPE"));
					sinkPstmt.setBigDecimal(13, rs.getBigDecimal("FEE_TYPE"));
					sinkPstmt.setString(14, rs.getString("CHARGE_MODE"));
					sinkPstmt.setBigDecimal(15, rs.getBigDecimal("PREM_LIST_ID"));
					sinkPstmt.setBigDecimal(16, rs.getBigDecimal("COMM_ITEM_ID"));
					sinkPstmt.setBigDecimal(17, rs.getBigDecimal("POLICY_CHG_ID"));
					sinkPstmt.setBigDecimal(18, rs.getBigDecimal("EXCHANGE_RATE"));
					sinkPstmt.setBigDecimal(19, rs.getBigDecimal("RELATED_ID"));
					sinkPstmt.setBigDecimal(20, rs.getBigDecimal("INSURED_ID"));
					sinkPstmt.setBigDecimal(21, rs.getBigDecimal("POL_PRODUCTION_VALUE"));
					sinkPstmt.setBigDecimal(22, rs.getBigDecimal("POL_CURRENCY_ID"));
					sinkPstmt.setString(23, rs.getString("HIERARCHY_EXIST_INDI"));
					sinkPstmt.setBigDecimal(24, rs.getBigDecimal("AGGREGATION_ID"));
					sinkPstmt.setBigDecimal(25, rs.getBigDecimal("PRODUCT_VERSION"));
					sinkPstmt.setString(26, rs.getString("SOURCE_TABLE"));
					sinkPstmt.setBigDecimal(27, rs.getBigDecimal("SOURCE_ID"));
					sinkPstmt.setBigDecimal(28, rs.getBigDecimal("RESULT_LIST_ID"));
					sinkPstmt.setDate(29, rs.getDate("FINISH_TIME"));
					sinkPstmt.setBigDecimal(30, rs.getBigDecimal("INSERTED_BY"));
					sinkPstmt.setBigDecimal(31, rs.getBigDecimal("UPDATED_BY"));
					sinkPstmt.setDate(32, rs.getDate("INSERT_TIME"));
					sinkPstmt.setDate(33, rs.getDate("UPDATE_TIME"));
					sinkPstmt.setDate(34, rs.getDate("INSERT_TIMESTAMP"));
					sinkPstmt.setDate(35, rs.getDate("UPDATE_TIMESTAMP"));
					sinkPstmt.setBigDecimal(36, rs.getBigDecimal("COMMISSION_RATE"));
					sinkPstmt.setString(37, rs.getString("CHEQUE_INDI"));
					sinkPstmt.setBigDecimal(38, rs.getBigDecimal("PREM_ALLOCATE_YEAR"));
					sinkPstmt.setString(39, rs.getString("RECALCULATED_INDI"));
					sinkPstmt.setString(40, rs.getString("EXCLUDE_POLICY_INDI"));
					sinkPstmt.setBigDecimal(41, rs.getBigDecimal("CHANNEL_ORG_ID"));
					sinkPstmt.setString(42, rs.getString("AGENT_CATE"));
					sinkPstmt.setString(43, rs.getString("YEAR_MONTH"));
					sinkPstmt.setBigDecimal(44, rs.getBigDecimal("CONVERSION_CATE"));
					sinkPstmt.setBigDecimal(45, rs.getBigDecimal("ORDER_ID"));
					sinkPstmt.setBigDecimal(46, rs.getBigDecimal("ASSIGN_RATE"));
					
					sinkPstmt.setLong(47, rs.getLong("ORA_ROWSCN"));				// new column
					sinkPstmt.setString(48,  rs.getString("ROWID"));		// new column
					
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
