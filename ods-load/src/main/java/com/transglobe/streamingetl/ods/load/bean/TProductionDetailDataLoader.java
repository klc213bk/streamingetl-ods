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

public class TProductionDetailDataLoader extends DataLoader {

	private static final Logger logger = LoggerFactory.getLogger(TProductionDetailDataLoader.class);

	private String sourceTableName ;

	private String sinkTableName;

	private String streamingEtlName;

	private String sinkTableCreateFile;

	private String sinkTableIndexesFile;

	public TProductionDetailDataLoader(Config config, Date dataDate) throws Exception {

		super(DataLoader.DEFAULT_THREADS, DataLoader.DEFAULT_BATCH_COMMIT_SIZE, config, dataDate);

		this.sourceTableName = config.sourceTableTProductionDetail;

		this.sinkTableName = config.sinkTableKProductionDetail;

		this.streamingEtlName = config.streamingEtlNameTProductionDetail;

		this.sinkTableCreateFile = config.sinkTableCreateFileKProductionDetail;

		this.sinkTableIndexesFile = config.sinkTableIndexesFileKProductionDetail;
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
		// TODO Auto-generated method stub
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
		+ ",ACCEPT_ID"
		+ " from " + this.sourceTableName
		+ " a where ? <= a.DETAIL_ID and a.DETAIL_ID < ?";
	}

	@Override
	protected String getInsertSql() {
		return "insert into " + this.sinkTableName
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
				+ ",DATA_DATE"				// ods add column
				+ ",TBL_UPD_TIME"			// ods add column
				+ ",TBL_UPD_SCN)"	// new column
				+ " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,?,?,?,CURRENT_DATE,?)";
			
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
					
					sinkPstmt.setDate(47, dataDate);
					
					// db current_time for tbl_upd_time 
					
					sinkPstmt.setLong(48, loadBean.currentScn);				// new column				// new column
					
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
