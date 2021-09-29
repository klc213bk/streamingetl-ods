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

import com.transglobe.streamingetl.ods.load.Config;

public class TCommisionFeeDataLoader extends DataLoader {

	private static final Logger logger = LoggerFactory.getLogger(TCommisionFeeDataLoader.class);

	private String sourceTableName ;

	private String sinkTableName;

	private String sinkTableCreateFile;

	private String sinkTableIndexesFile;

	public TCommisionFeeDataLoader(Config config, Date dataDate) throws Exception {

		super(config, dataDate);

		this.sourceTableName = config.sourceTableTCommisionFee;

		this.sinkTableName = config.sinkTableKCommisionFee;

		this.sinkTableCreateFile = config.sinkTableCreateFileKCommisionFee;

		this.sinkTableIndexesFile = config.sinkTableIndexesFileKCommisionFee;
	}

	@Override
	public String getSourceTableName() {
		return this.sourceTableName;
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
		return "select min(FEE_ID) as MIN_ID from " + this.sourceTableName;
	}

	@Override
	protected String getSelectMaxIdSql() {
		return "select max(FEE_ID) as MAX_ID from " + this.sourceTableName;
	}

	@Override
	protected String getCountSql() {
		return "select count(*) as CNT from " + this.sourceTableName + " a where ? <= a.FEE_ID and a.FEE_ID < ?";
	}

	@Override
	protected String getSelectSql() {
		return "select"
		+ " FEE_ID"
		+ ",SOURCE_ID"
		+ ",SOURCE_TABLE"
		+ ",COMMISION_TYPE_ID"
		+ ",COMMISION"
		+ ",AGENT_ID"
		+ ",AGENT_CATE"
		+ ",AGENT_GRADE"
		+ ",CHANNEL_ORG_ID"
		+ ",HEAD_ID"
		+ ",BRANCH_ID"
		+ ",ORGAN_ID"
		+ ",POLICY_ID"
		+ ",POLICY_CHG_ID"
		+ ",POLICY_TYPE"
		+ ",BENEFIT_ITEM_ID"
		+ ",PRODUCT_ID"
		+ ",POLICY_YEAR"
		+ ",FEE_TYPE"
		+ ",HAPPEN_TIME"
		+ ",DUE_TIME"
		+ ",CONFIRM_DATE"
		+ ",ACCOUNTING_DATE"
		+ ",JE_CREATOR_ID"
		+ ",JE_POSTING_ID"
		+ ",POSTED"
		+ ",DR_SEG1"
		+ ",DR_SEG2"
		+ ",DR_SEG3"
		+ ",DR_SEG4"
		+ ",DR_SEG5"
		+ ",DR_SEG6"
		+ ",DR_SEG7"
		+ ",DR_SEG8"
		+ ",DR_SEG9"
		+ ",DR_SEG10"
		+ ",DR_SEG11"
		+ ",DR_SEG12"
		+ ",DR_SEG13"
		+ ",DR_SEG14"
		+ ",DR_SEG15"
		+ ",DR_SEG16"
		+ ",DR_SEG17"
		+ ",DR_SEG18"
		+ ",DR_SEG19"
		+ ",DR_SEG20"
		+ ",CR_SEG1"
		+ ",CR_SEG2"
		+ ",CR_SEG3"
		+ ",CR_SEG4"
		+ ",CR_SEG5"
		+ ",CR_SEG6"
		+ ",CR_SEG7"
		+ ",CR_SEG8"
		+ ",CR_SEG9"
		+ ",CR_SEG10"
		+ ",CR_SEG11"
		+ ",CR_SEG12"
		+ ",CR_SEG13"
		+ ",CR_SEG14"
		+ ",CR_SEG15"
		+ ",CR_SEG16"
		+ ",CR_SEG17"
		+ ",CR_SEG18"
		+ ",CR_SEG19"
		+ ",CR_SEG20"
		+ ",ADJUST_ITEM_ID"
		+ ",MONEY_ID"
		+ ",AGGREGATION_ID"
		+ ",PRODUCT_VERSION"
		+ ",COMMISSION_VERSION"
		+ ",POL_CURRENCY_ID"
		+ ",POL_COMMISION"
		+ ",CHEQUE_ID"
		+ ",DIVISION_INDI"
		+ ",REPLY_CODE"
		+ ",TRIGGER_CODE"
		+ ",PREM_YEAR"
		+ ",COMM_STATUS"
		+ ",INSERTED_BY"
		+ ",UPDATED_BY"
		+ ",INSERT_TIME"
		+ ",UPDATE_TIME"
		+ ",INSERT_TIMESTAMP"
		+ ",UPDATE_TIMESTAMP"
		+ ",EXCHANGE_DATE"
		+ ",EXCHANGE_RATE"
		+ ",HEAD_AGENT_ID"
		+ ",ORA_ROWSCN"
		+ ",ROWID"
		+ " from " + this.sourceTableName
		+ " a where ? <= a.FEE_ID and a.FEE_ID < ?";
	}

	@Override
	protected String getInsertSql() {
		return "insert into " + this.sinkTableName
				+ " (FEE_ID"
				+ ",SOURCE_ID"
				+ ",SOURCE_TABLE"
				+ ",COMMISION_TYPE_ID"
				+ ",COMMISION"
				+ ",AGENT_ID"
				+ ",AGENT_CATE"
				+ ",AGENT_GRADE"
				+ ",CHANNEL_ORG_ID"
				+ ",HEAD_ID"
				+ ",BRANCH_ID"
				+ ",ORGAN_ID"
				+ ",POLICY_ID"
				+ ",POLICY_CHG_ID"
				+ ",POLICY_TYPE"
				+ ",BENEFIT_ITEM_ID"
				+ ",PRODUCT_ID"
				+ ",POLICY_YEAR"
				+ ",FEE_TYPE"
				+ ",HAPPEN_TIME"
				+ ",DUE_TIME"
				+ ",CONFIRM_DATE"
				+ ",ACCOUNTING_DATE"
				+ ",JE_CREATOR_ID"
				+ ",JE_POSTING_ID"
				+ ",POSTED"
				+ ",DR_SEG1"
				+ ",DR_SEG2"
				+ ",DR_SEG3"
				+ ",DR_SEG4"
				+ ",DR_SEG5"
				+ ",DR_SEG6"
				+ ",DR_SEG7"
				+ ",DR_SEG8"
				+ ",DR_SEG9"
				+ ",DR_SEG10"
				+ ",DR_SEG11"
				+ ",DR_SEG12"
				+ ",DR_SEG13"
				+ ",DR_SEG14"
				+ ",DR_SEG15"
				+ ",DR_SEG16"
				+ ",DR_SEG17"
				+ ",DR_SEG18"
				+ ",DR_SEG19"
				+ ",DR_SEG20"
				+ ",CR_SEG1"
				+ ",CR_SEG2"
				+ ",CR_SEG3"
				+ ",CR_SEG4"
				+ ",CR_SEG5"
				+ ",CR_SEG6"
				+ ",CR_SEG7"
				+ ",CR_SEG8"
				+ ",CR_SEG9"
				+ ",CR_SEG10"
				+ ",CR_SEG11"
				+ ",CR_SEG12"
				+ ",CR_SEG13"
				+ ",CR_SEG14"
				+ ",CR_SEG15"
				+ ",CR_SEG16"
				+ ",CR_SEG17"
				+ ",CR_SEG18"
				+ ",CR_SEG19"
				+ ",CR_SEG20"
				+ ",ADJUST_ITEM_ID"
				+ ",MONEY_ID"
				+ ",AGGREGATION_ID"
				+ ",PRODUCT_VERSION"
				+ ",COMMISSION_VERSION"
				+ ",POL_CURRENCY_ID"
				+ ",POL_COMMISION"
				+ ",CHEQUE_ID"
				+ ",DIVISION_INDI"
				+ ",REPLY_CODE"
				+ ",TRIGGER_CODE"
				+ ",PREM_YEAR"
				+ ",COMM_STATUS"
				+ ",DATA_DATE"		// ods add column
				+ ",TBL_UPD_TIME"	// ods add column
				+ ",INSERTED_BY"
				+ ",UPDATED_BY"
				+ ",INSERT_TIME"
				+ ",UPDATE_TIME"
				+ ",INSERT_TIMESTAMP"
				+ ",UPDATE_TIMESTAMP"
				+ ",EXCHANGE_DATE"
				+ ",EXCHANGE_RATE"
				+ ",HEAD_AGENT_ID"
				+ ",SCN"		// new column
				+ ",COMMIT_SCN"	// new column
				+ ",ROW_ID)"	// new column
				+ " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",CURRENT_DATE,?,?,?,?,?,?,?,?,?,?,?,?)";
			
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

			try (final ResultSet rs =
					sourcePstmt.executeQuery())
			{
				Long count = 0L;
				while (rs.next())
				{
					count++;

					sinkPstmt.clearParameters();

					sinkPstmt.setLong(1, rs.getLong("FEE_ID"));
					sinkPstmt.setBigDecimal(2, rs.getBigDecimal("SOURCE_ID"));
					sinkPstmt.setBigDecimal(3, rs.getBigDecimal("SOURCE_TABLE"));
					sinkPstmt.setBigDecimal(4, rs.getBigDecimal("COMMISION_TYPE_ID"));
					sinkPstmt.setBigDecimal(5, rs.getBigDecimal("COMMISION"));
					sinkPstmt.setBigDecimal(6, rs.getBigDecimal("AGENT_ID"));
					sinkPstmt.setString(7, rs.getString("AGENT_CATE"));
					sinkPstmt.setString(8, rs.getString("AGENT_GRADE"));
					sinkPstmt.setBigDecimal(9, rs.getBigDecimal("CHANNEL_ORG_ID"));
					sinkPstmt.setBigDecimal(10, rs.getBigDecimal("HEAD_ID"));
					sinkPstmt.setBigDecimal(11, rs.getBigDecimal("BRANCH_ID"));
					sinkPstmt.setBigDecimal(12, rs.getBigDecimal("ORGAN_ID"));
					sinkPstmt.setBigDecimal(13, rs.getBigDecimal("POLICY_ID"));
					sinkPstmt.setBigDecimal(14, rs.getBigDecimal("POLICY_CHG_ID"));
					sinkPstmt.setString(15, rs.getString("POLICY_TYPE"));
					sinkPstmt.setBigDecimal(16, rs.getBigDecimal("BENEFIT_ITEM_ID"));
					sinkPstmt.setBigDecimal(17, rs.getBigDecimal("PRODUCT_ID"));
					sinkPstmt.setBigDecimal(18, rs.getBigDecimal("POLICY_YEAR"));
					sinkPstmt.setBigDecimal(19, rs.getBigDecimal("FEE_TYPE"));
					sinkPstmt.setDate(20, rs.getDate("HAPPEN_TIME"));
					sinkPstmt.setDate(21, rs.getDate("DUE_TIME"));
					sinkPstmt.setDate(22, rs.getDate("CONFIRM_DATE"));
					sinkPstmt.setDate(23, rs.getDate("ACCOUNTING_DATE"));
					sinkPstmt.setBigDecimal(24, rs.getBigDecimal("JE_CREATOR_ID"));
					sinkPstmt.setBigDecimal(25, rs.getBigDecimal("JE_POSTING_ID"));
					sinkPstmt.setString(26, rs.getString("POSTED"));
					sinkPstmt.setString(27, rs.getString("DR_SEG1"));
					sinkPstmt.setString(28, rs.getString("DR_SEG2"));
					sinkPstmt.setString(29, rs.getString("DR_SEG3"));
					sinkPstmt.setString(30, rs.getString("DR_SEG4"));
					sinkPstmt.setString(31, rs.getString("DR_SEG5"));
					sinkPstmt.setString(32, rs.getString("DR_SEG6"));
					sinkPstmt.setString(33, rs.getString("DR_SEG7"));
					sinkPstmt.setString(34, rs.getString("DR_SEG8"));
					sinkPstmt.setString(35, rs.getString("DR_SEG9"));
					sinkPstmt.setString(36, rs.getString("DR_SEG10"));
					sinkPstmt.setString(37, rs.getString("DR_SEG11"));
					sinkPstmt.setString(38, rs.getString("DR_SEG12"));
					sinkPstmt.setString(39, rs.getString("DR_SEG13"));
					sinkPstmt.setString(40, rs.getString("DR_SEG14"));
					sinkPstmt.setString(41, rs.getString("DR_SEG15"));
					sinkPstmt.setString(42, rs.getString("DR_SEG16"));
					sinkPstmt.setString(43, rs.getString("DR_SEG17"));
					sinkPstmt.setString(44, rs.getString("DR_SEG18"));
					sinkPstmt.setString(45, rs.getString("DR_SEG19"));
					sinkPstmt.setString(46, rs.getString("DR_SEG20"));
					sinkPstmt.setString(47, rs.getString("CR_SEG1"));
					sinkPstmt.setString(48, rs.getString("CR_SEG2"));
					sinkPstmt.setString(49, rs.getString("CR_SEG3"));
					sinkPstmt.setString(50, rs.getString("CR_SEG4"));
					sinkPstmt.setString(51, rs.getString("CR_SEG5"));
					sinkPstmt.setString(52, rs.getString("CR_SEG6"));
					sinkPstmt.setString(53, rs.getString("CR_SEG7"));
					sinkPstmt.setString(54, rs.getString("CR_SEG8"));
					sinkPstmt.setString(55, rs.getString("CR_SEG9"));
					sinkPstmt.setString(56, rs.getString("CR_SEG10"));
					sinkPstmt.setString(57, rs.getString("CR_SEG11"));
					sinkPstmt.setString(58, rs.getString("CR_SEG12"));
					sinkPstmt.setString(59, rs.getString("CR_SEG13"));
					sinkPstmt.setString(60, rs.getString("CR_SEG14"));
					sinkPstmt.setString(61, rs.getString("CR_SEG15"));
					sinkPstmt.setString(62, rs.getString("CR_SEG16"));
					sinkPstmt.setString(63, rs.getString("CR_SEG17"));
					sinkPstmt.setString(64, rs.getString("CR_SEG18"));
					sinkPstmt.setString(65, rs.getString("CR_SEG19"));
					sinkPstmt.setString(66, rs.getString("CR_SEG20"));
					sinkPstmt.setBigDecimal(67, rs.getBigDecimal("ADJUST_ITEM_ID"));
					sinkPstmt.setBigDecimal(68, rs.getBigDecimal("MONEY_ID"));
					sinkPstmt.setBigDecimal(69, rs.getBigDecimal("AGGREGATION_ID"));
					sinkPstmt.setBigDecimal(70, rs.getBigDecimal("PRODUCT_VERSION"));
					sinkPstmt.setBigDecimal(71, rs.getBigDecimal("COMMISSION_VERSION"));
					sinkPstmt.setBigDecimal(72, rs.getBigDecimal("POL_CURRENCY_ID"));
					sinkPstmt.setBigDecimal(73, rs.getBigDecimal("POL_COMMISION"));
					sinkPstmt.setBigDecimal(74, rs.getBigDecimal("CHEQUE_ID"));
					sinkPstmt.setString(75, rs.getString("DIVISION_INDI"));
					sinkPstmt.setString(76, rs.getString("REPLY_CODE"));
					sinkPstmt.setString(77, rs.getString("TRIGGER_CODE"));
					sinkPstmt.setString(78, rs.getString("PREM_YEAR"));
					sinkPstmt.setBigDecimal(79, rs.getBigDecimal("COMM_STATUS"));
					sinkPstmt.setDate(80, dataDate);
					//TBL_UPD_TIME
					sinkPstmt.setBigDecimal(81, rs.getBigDecimal("INSERTED_BY"));
					sinkPstmt.setBigDecimal(82, rs.getBigDecimal("UPDATED_BY"));
					sinkPstmt.setDate(83, rs.getDate("INSERT_TIME"));
					sinkPstmt.setDate(84, rs.getDate("UPDATE_TIME"));
					sinkPstmt.setDate(85, rs.getDate("INSERT_TIMESTAMP"));
					sinkPstmt.setDate(86, rs.getDate("UPDATE_TIMESTAMP"));
					sinkPstmt.setDate(87, rs.getDate("EXCHANGE_DATE"));
					sinkPstmt.setString(88, rs.getString("EXCHANGE_RATE"));
					sinkPstmt.setBigDecimal(89, rs.getBigDecimal("HEAD_AGENT_ID"));

					sinkPstmt.setLong(90, rs.getLong("ORA_ROWSCN"));				// new column				// new column
					sinkPstmt.setLong(91, rs.getLong("ORA_ROWSCN"));				// new column
					sinkPstmt.setString(92,  rs.getString("ROWID"));		// new column
					
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
