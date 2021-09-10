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


public class TProductCommisionDataLoader extends DataLoader {
	private static final Logger logger = LoggerFactory.getLogger(TProductCommisionDataLoader.class);

	private String sourceTableName ;
	
	private String sinkTableName;
	
	private String streamingEtlName;
	
	private String sinkTableCreateFile;
	
	private String sinkTableIndexesFile;
	
	public TProductCommisionDataLoader(Config config, Date dataDate) throws Exception {
		
		super(DataLoader.DEFAULT_THREADS, DataLoader.DEFAULT_BATCH_COMMIT_SIZE, config, dataDate);
		
		this.sourceTableName = config.sourceTableTProductCommision;
		
		this.sinkTableName = config.sinkTableKProductCommision;
		
		this.streamingEtlName = config.streamingEtlNameTProductCommision;
		
		this.sinkTableCreateFile = config.sinkTableCreateFileKProductCommision;
		
		this.sinkTableIndexesFile = config.sinkTableIndexesFileKProductCommision;
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
		return "select min(ITEM_ID) as MIN_ID from " + this.sourceTableName;
	}

	@Override
	protected String getSelectMaxIdSql() {
		return "select max(ITEM_ID) as MAX_ID from " + this.sourceTableName;
	}

	@Override
	protected String getCountSql() {
		return "select count(*) as CNT from " + this.sourceTableName + " a where ? <= a.ITEM_ID and a.ITEM_ID < ?";
	}

	@Override
	protected String getSelectSql() {
		return "select"
				+ " ITEM_ID"
				+ ",LIST_ID"
				+ ",HEAD_ID"
				+ ",BRANCH_ID"
				+ ",ORGAN_ID"
				+ ",POLICY_TYPE"
				+ ",HAPPEN_TIME"
				+ ",DUE_TIME"
				+ ",AGENT_ID"
				+ ",GRADE_ID"
				+ ",COMMISION_RATE"
				+ ",NORMAL_COMMISION"
				+ ",DISCOUNT_RATE"
				+ ",COMMISION"
				+ ",COMMISION_ID"
				+ ",IS_PAY"
				+ ",POLICY_YEAR"
				+ ",ASSIGN_RATE"
				+ ",SIGN_ID"
				+ ",MGR_RATE"
				+ ",AGENT_CATE"
				+ ",COMMISION_TYPE_ID"
				+ ",DERIVATION"
				+ ",FEE_TYPE"
				+ ",GST_COMMISION"
				+ ",SUSPEND_CAUSE"
				+ ",ISSUE_MODE"
				+ ",PAYMENT_ID"
				+ ",POSTED"
				+ ",CRED_ID"
				+ ",POST_ID"
				+ ",POLICY_ID"
				+ ",MONEY_ID"
				+ ",COMM_STATUS"
				+ ",AGGREGATION_ID"
				+ ",BENEFIT_ITEM_ID"
				+ ",PRODUCT_ID"
				+ ",RELATED_ID"
				+ ",REVERSAL_POLICY_CHG_ID"
				+ ",COMM_SOURCE"
				+ ",COMM_COMMENT"
				+ ",EXCHANGE_RATE"
				+ ",CONFIRM_DATE"
				+ ",ACCOUNTING_DATE"
				+ ",JE_POSTING_ID"
				+ ",JE_CREATOR_ID"
				+ ",DR_SEG1"
				+ ",DR_SEG2"
				+ ",DR_SEG3"
				+ ",DR_SEG4"
				+ ",DR_SEG5"
				+ ",DR_SEG6"
				+ ",DR_SEG7"
				+ ",DR_SEG8"
				+ ",CR_SEG1"
				+ ",CR_SEG2"
				+ ",CR_SEG3"
				+ ",CR_SEG4"
				+ ",CR_SEG5"
				+ ",CR_SEG6"
				+ ",CR_SEG7"
				+ ",CR_SEG8"
				+ ",CONFIRM_EMP"
				+ ",CHILD_LEVEL"
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
				+ ",CHANNEL_ORG_ID"
				+ ",STREAM_ID"
				+ ",PREM_TYPE"
				+ ",CHANGE_ID"
				+ ",POLICY_CHG_ID"
				+ ",INSERT_TIME"
				+ ",UPDATE_TIME"
				+ ",INSERTED_BY"
				+ ",UPDATED_BY"
				+ ",INSERT_TIMESTAMP"
				+ ",UPDATE_TIMESTAMP"
				+ ",COMMISION_RATE_EXTRA"
				+ ",SOURCE_TABLE"
				+ ",SOURCE_ID"
				+ ",PRODUCT_VERSION"
				+ ",POL_CURRENCY_ID"
				+ ",POL_COMMISION"
				+ ",RETAIN_INDI"
				+ ",MANG_TAKEON_INDI"
				+ ",COMMISION_VERSION"
				+ ",DIVISION_INDI"
				+ ",STD_PREM_AF"
				+ ",EXTRA_PREM_AF"
				+ ",EXCHANGE_DATE"
				+ ",RESULT_LIST_ID"
				+ ",CHEQUE_INDI"
				+ ",PREM_ALLOCATE_YEAR"
				+ ",COMMENTS"
				+ ",YEAR_MONTH"
				+ ",CALC_RST_ID"
				+ ",CONVERSION_CATE"
				+ ",CHEQUE_YEAR_MONTH"
				+ ",ENTRY_AGE_"
				+ ",PRODUCT_VERSION_"
				+ ",INTERNAL_ID_"
				+ ",INSURED_CATEGORY_"
				+ ",COVERAGE_YEAR_"
				+ ",COVERAGE_PERIOD_"
				+ ",CHARGE_YEAR_"
				+ ",CHARGE_PERIOD_"
				+ ",AMOUNT_"
				+ ",CHANNEL_CODE_"
				+ ",INITIAL_TYPE_"
				+ ",RPT_EXCLUDE_INDI"
				+ ",POLICY_PERIOD"
				+ ",CHECK_ENTER_TIME"
				+ ",SERVICE_ID"
				+ ",ORDER_ID" 
				+ ",ROWID"
				+ " from " + this.sourceTableName
				+ " a where ? <= a.ITEM_ID and a.ITEM_ID < ?";
	}

	@Override
	protected String getInsertSql() {
		return "insert into " + this.sinkTableName
				+ " (ITEM_ID"
				+ ",LIST_ID"
				+ ",HEAD_ID"
				+ ",BRANCH_ID"
				+ ",ORGAN_ID"
				+ ",POLICY_TYPE"
				+ ",HAPPEN_TIME"
				+ ",DUE_TIME"
				+ ",AGENT_ID"
				+ ",GRADE_ID"
				+ ",COMMISION_RATE"
				+ ",NORMAL_COMMISION"
				+ ",DISCOUNT_RATE"
				+ ",COMMISION"
				+ ",COMMISION_ID"
				+ ",IS_PAY"
				+ ",POLICY_YEAR"
				+ ",ASSIGN_RATE"
				+ ",SIGN_ID"
				+ ",MGR_RATE"
				+ ",AGENT_CATE"
				+ ",COMMISION_TYPE_ID"
				+ ",DERIVATION"
				+ ",FEE_TYPE"
				+ ",GST_COMMISION"
				+ ",SUSPEND_CAUSE"
				+ ",ISSUE_MODE"
				+ ",PAYMENT_ID"
				+ ",POSTED"
				+ ",CRED_ID"
				+ ",POST_ID"
				+ ",POLICY_ID"
				+ ",MONEY_ID"
				+ ",COMM_STATUS"
				+ ",AGGREGATION_ID"
				+ ",BENEFIT_ITEM_ID"
				+ ",PRODUCT_ID"
				+ ",RELATED_ID"
				+ ",REVERSAL_POLICY_CHG_ID"
				+ ",COMM_SOURCE"
				+ ",COMM_COMMENT"
				+ ",EXCHANGE_RATE"
				+ ",CONFIRM_DATE"
				+ ",ACCOUNTING_DATE"
				+ ",JE_POSTING_ID"
				+ ",JE_CREATOR_ID"
				+ ",DR_SEG1"
				+ ",DR_SEG2"
				+ ",DR_SEG3"
				+ ",DR_SEG4"
				+ ",DR_SEG5"
				+ ",DR_SEG6"
				+ ",DR_SEG7"
				+ ",DR_SEG8"
				+ ",CR_SEG1"
				+ ",CR_SEG2"
				+ ",CR_SEG3"
				+ ",CR_SEG4"
				+ ",CR_SEG5"
				+ ",CR_SEG6"
				+ ",CR_SEG7"
				+ ",CR_SEG8"
				+ ",CONFIRM_EMP"
				+ ",CHILD_LEVEL"
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
				+ ",CHANNEL_ORG_ID"
				+ ",STREAM_ID"
				+ ",PREM_TYPE"
				+ ",CHANGE_ID"
				+ ",POLICY_CHG_ID"
				+ ",INSERT_TIME"
				+ ",UPDATE_TIME"
				+ ",INSERTED_BY"
				+ ",UPDATED_BY"
				+ ",INSERT_TIMESTAMP"
				+ ",UPDATE_TIMESTAMP"
				+ ",COMMISION_RATE_EXTRA"
				+ ",SOURCE_TABLE"
				+ ",SOURCE_ID"
				+ ",PRODUCT_VERSION"
				+ ",POL_CURRENCY_ID"
				+ ",POL_COMMISION"
				+ ",RETAIN_INDI"
				+ ",MANG_TAKEON_INDI"
				+ ",COMMISION_VERSION"
				+ ",DIVISION_INDI"
				+ ",STD_PREM_AF"
				+ ",EXTRA_PREM_AF"
				+ ",EXCHANGE_DATE"
				+ ",RESULT_LIST_ID"
				+ ",CHEQUE_INDI"
				+ ",PREM_ALLOCATE_YEAR"
				+ ",DATA_DATE"				// ods add column
				+ ",TBL_UPD_TIME"			// ods add column
				+ ",COMMENTS"
				+ ",YEAR_MONTH"
				+ ",CALC_RST_ID"
				+ ",CONVERSION_CATE"
				+ ",CHEQUE_YEAR_MONTH"
				+ ",ENTRY_AGE_"
				+ ",PRODUCT_VERSION_"
				+ ",INTERNAL_ID_"
				+ ",INSURED_CATEGORY_"
				+ ",COVERAGE_YEAR_"
				+ ",COVERAGE_PERIOD_"
				+ ",CHARGE_YEAR_"
				+ ",CHARGE_PERIOD_"
				+ ",AMOUNT_"
				+ ",CHANNEL_CODE_"
				+ ",INITIAL_TYPE_"                 
				+ ",SCN"		// new column
				+ ",COMMIT_SCN"	// new column
				+ ",ROW_ID)"	// new column
				+ " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_DATE,?,?,?"
				+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			
	}

	@Override
	public void transferData(LoadBean loadBean, BasicDataSource sourceConnectionPool, BasicDataSource sinkConnectionPool,
			BasicDataSource logminerConnectionPool) throws Exception {
		Console cnsl = null;
		
		try (
				Connection sourceConn = sourceConnectionPool.getConnection();
				Connection sinkConn = sinkConnectionPool.getConnection();
				Connection minerConn = logminerConnectionPool.getConnection();
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
				Long longValue;
				while (rs.next())
				{
					count++;

					sinkPstmt.clearParameters();

					sinkPstmt.setBigDecimal(1, rs.getBigDecimal("ITEM_ID"));
					sinkPstmt.setBigDecimal(2, rs.getBigDecimal("LIST_ID"));
					sinkPstmt.setBigDecimal(3, rs.getBigDecimal("HEAD_ID"));
					sinkPstmt.setBigDecimal(4, rs.getBigDecimal("BRANCH_ID"));
					sinkPstmt.setBigDecimal(5, rs.getBigDecimal("ORGAN_ID"));
					sinkPstmt.setString(6, rs.getString("POLICY_TYPE"));
					sinkPstmt.setDate(7, rs.getDate("HAPPEN_TIME"));
					sinkPstmt.setDate(8, rs.getDate("DUE_TIME"));
					sinkPstmt.setBigDecimal(9, rs.getBigDecimal("AGENT_ID"));
					sinkPstmt.setString(10, rs.getString("GRADE_ID"));
					sinkPstmt.setBigDecimal(11, rs.getBigDecimal("COMMISION_RATE"));
					sinkPstmt.setBigDecimal(12, rs.getBigDecimal("NORMAL_COMMISION"));
					sinkPstmt.setBigDecimal(13, rs.getBigDecimal("DISCOUNT_RATE"));
					sinkPstmt.setBigDecimal(14, rs.getBigDecimal("COMMISION"));
					sinkPstmt.setBigDecimal(15, rs.getBigDecimal("COMMISION_ID"));
					sinkPstmt.setBigDecimal(16, rs.getBigDecimal("IS_PAY"));
					sinkPstmt.setBigDecimal(17, rs.getBigDecimal("POLICY_YEAR"));
					sinkPstmt.setBigDecimal(18, rs.getBigDecimal("ASSIGN_RATE"));
					sinkPstmt.setBigDecimal(19, rs.getBigDecimal("SIGN_ID"));
					sinkPstmt.setBigDecimal(20, rs.getBigDecimal("MGR_RATE"));
					sinkPstmt.setString(21, rs.getString("AGENT_CATE"));
					sinkPstmt.setBigDecimal(22, rs.getBigDecimal("COMMISION_TYPE_ID"));
					sinkPstmt.setString(23, rs.getString("DERIVATION"));
					sinkPstmt.setBigDecimal(24, rs.getBigDecimal("FEE_TYPE"));
					sinkPstmt.setBigDecimal(25, rs.getBigDecimal("GST_COMMISION"));
					sinkPstmt.setBigDecimal(26, rs.getBigDecimal("SUSPEND_CAUSE"));
					sinkPstmt.setBigDecimal(27, rs.getBigDecimal("ISSUE_MODE"));
					sinkPstmt.setBigDecimal(28, rs.getBigDecimal("PAYMENT_ID"));
					sinkPstmt.setString(29, rs.getString("POSTED"));
					sinkPstmt.setBigDecimal(30, rs.getBigDecimal("CRED_ID"));
					sinkPstmt.setBigDecimal(31, rs.getBigDecimal("POST_ID"));
					sinkPstmt.setBigDecimal(32, rs.getBigDecimal("POLICY_ID"));
					sinkPstmt.setBigDecimal(33, rs.getBigDecimal("MONEY_ID"));
					sinkPstmt.setBigDecimal(34, rs.getBigDecimal("COMM_STATUS"));
					sinkPstmt.setBigDecimal(35, rs.getBigDecimal("AGGREGATION_ID"));
					sinkPstmt.setBigDecimal(36, rs.getBigDecimal("BENEFIT_ITEM_ID"));
					sinkPstmt.setBigDecimal(37, rs.getBigDecimal("PRODUCT_ID"));
					sinkPstmt.setBigDecimal(38, rs.getBigDecimal("RELATED_ID"));
					sinkPstmt.setBigDecimal(39, rs.getBigDecimal("REVERSAL_POLICY_CHG_ID"));
					sinkPstmt.setBigDecimal(40, rs.getBigDecimal("COMM_SOURCE"));
					sinkPstmt.setString(41, rs.getString("COMM_COMMENT"));
					sinkPstmt.setBigDecimal(42, rs.getBigDecimal("EXCHANGE_RATE"));
					sinkPstmt.setDate(43, rs.getDate("CONFIRM_DATE"));
					sinkPstmt.setDate(44, rs.getDate("ACCOUNTING_DATE"));
					sinkPstmt.setBigDecimal(45, rs.getBigDecimal("JE_POSTING_ID"));
					sinkPstmt.setBigDecimal(46, rs.getBigDecimal("JE_CREATOR_ID"));
					sinkPstmt.setString(47, rs.getString("DR_SEG1"));
					sinkPstmt.setString(48, rs.getString("DR_SEG2"));
					sinkPstmt.setString(49, rs.getString("DR_SEG3"));
					sinkPstmt.setString(50, rs.getString("DR_SEG4"));
					sinkPstmt.setString(51, rs.getString("DR_SEG5"));
					sinkPstmt.setString(52, rs.getString("DR_SEG6"));
					sinkPstmt.setString(53, rs.getString("DR_SEG7"));
					sinkPstmt.setString(54, rs.getString("DR_SEG8"));
					sinkPstmt.setString(55, rs.getString("CR_SEG1"));
					sinkPstmt.setString(56, rs.getString("CR_SEG2"));
					sinkPstmt.setString(57, rs.getString("CR_SEG3"));
					sinkPstmt.setString(58, rs.getString("CR_SEG4"));
					sinkPstmt.setString(59, rs.getString("CR_SEG5"));
					sinkPstmt.setString(60, rs.getString("CR_SEG6"));
					sinkPstmt.setString(61, rs.getString("CR_SEG7"));
					sinkPstmt.setString(62, rs.getString("CR_SEG8"));
					sinkPstmt.setBigDecimal(63, rs.getBigDecimal("CONFIRM_EMP"));
					sinkPstmt.setBigDecimal(64, rs.getBigDecimal("CHILD_LEVEL"));
					sinkPstmt.setString(65, rs.getString("DR_SEG9"));
					sinkPstmt.setString(66, rs.getString("DR_SEG10"));
					sinkPstmt.setString(67, rs.getString("DR_SEG11"));
					sinkPstmt.setString(68, rs.getString("DR_SEG12"));
					sinkPstmt.setString(69, rs.getString("DR_SEG13"));
					sinkPstmt.setString(70, rs.getString("DR_SEG14"));
					sinkPstmt.setString(71, rs.getString("DR_SEG15"));
					sinkPstmt.setString(72, rs.getString("DR_SEG16"));
					sinkPstmt.setString(73, rs.getString("DR_SEG17"));
					sinkPstmt.setString(74, rs.getString("DR_SEG18"));
					sinkPstmt.setString(75, rs.getString("DR_SEG19"));
					sinkPstmt.setString(76, rs.getString("DR_SEG20"));
					sinkPstmt.setString(77, rs.getString("CR_SEG9"));
					sinkPstmt.setString(78, rs.getString("CR_SEG10"));
					sinkPstmt.setString(79, rs.getString("CR_SEG11"));
					sinkPstmt.setString(80, rs.getString("CR_SEG12"));
					sinkPstmt.setString(81, rs.getString("CR_SEG13"));
					sinkPstmt.setString(82, rs.getString("CR_SEG14"));
					sinkPstmt.setString(83, rs.getString("CR_SEG15"));
					sinkPstmt.setString(84, rs.getString("CR_SEG16"));
					sinkPstmt.setString(85, rs.getString("CR_SEG17"));
					sinkPstmt.setString(86, rs.getString("CR_SEG18"));
					sinkPstmt.setString(87, rs.getString("CR_SEG19"));
					sinkPstmt.setString(88, rs.getString("CR_SEG20"));
					sinkPstmt.setBigDecimal(89, rs.getBigDecimal("CHANNEL_ORG_ID"));
					sinkPstmt.setBigDecimal(90, rs.getBigDecimal("STREAM_ID"));
					sinkPstmt.setString(91, rs.getString("PREM_TYPE"));
					sinkPstmt.setBigDecimal(92, rs.getBigDecimal("CHANGE_ID"));
					sinkPstmt.setBigDecimal(93, rs.getBigDecimal("POLICY_CHG_ID"));
					sinkPstmt.setDate(94, rs.getDate("INSERT_TIME"));
					sinkPstmt.setDate(95, rs.getDate("UPDATE_TIME"));
					sinkPstmt.setBigDecimal(96, rs.getBigDecimal("INSERTED_BY"));
					sinkPstmt.setBigDecimal(97, rs.getBigDecimal("UPDATED_BY"));
					sinkPstmt.setDate(98, rs.getDate("INSERT_TIMESTAMP"));
					sinkPstmt.setDate(99, rs.getDate("UPDATE_TIMESTAMP"));
					sinkPstmt.setBigDecimal(100, rs.getBigDecimal("COMMISION_RATE_EXTRA"));
					sinkPstmt.setString(101, rs.getString("SOURCE_TABLE"));
					sinkPstmt.setBigDecimal(102, rs.getBigDecimal("SOURCE_ID"));
					sinkPstmt.setBigDecimal(103, rs.getBigDecimal("PRODUCT_VERSION"));
					sinkPstmt.setBigDecimal(104, rs.getBigDecimal("POL_CURRENCY_ID"));
					sinkPstmt.setString(105, rs.getString("POL_COMMISION"));
					sinkPstmt.setString(106, rs.getString("RETAIN_INDI"));
					sinkPstmt.setString(107, rs.getString("MANG_TAKEON_INDI"));
					sinkPstmt.setBigDecimal(108, rs.getBigDecimal("COMMISION_VERSION"));
					sinkPstmt.setString(109, rs.getString("DIVISION_INDI"));
					sinkPstmt.setBigDecimal(110, rs.getBigDecimal("STD_PREM_AF"));
					sinkPstmt.setBigDecimal(111, rs.getBigDecimal("EXTRA_PREM_AF"));
					sinkPstmt.setDate(112, rs.getDate("EXCHANGE_DATE"));
					sinkPstmt.setBigDecimal(113, rs.getBigDecimal("RESULT_LIST_ID"));
					sinkPstmt.setString(114, rs.getString("CHEQUE_INDI"));
					sinkPstmt.setBigDecimal(115, rs.getBigDecimal("PREM_ALLOCATE_YEAR"));
					sinkPstmt.setDate(116, dataDate);	// ods add column 
					// db current_time for tbl_upd_time 				// ods add column 
					sinkPstmt.setString(117, rs.getString("COMMENTS"));
					sinkPstmt.setString(118, rs.getString("YEAR_MONTH"));
					sinkPstmt.setBigDecimal(119, rs.getBigDecimal("CALC_RST_ID"));
					sinkPstmt.setBigDecimal(120, rs.getBigDecimal("CONVERSION_CATE"));
					sinkPstmt.setString(121, rs.getString("CHEQUE_YEAR_MONTH"));
					sinkPstmt.setBigDecimal(122, rs.getBigDecimal("ENTRY_AGE_"));
					sinkPstmt.setString(123, rs.getString("PRODUCT_VERSION_"));
					sinkPstmt.setString(124, rs.getString("INTERNAL_ID_"));
					sinkPstmt.setString(125, rs.getString("INSURED_CATEGORY_"));
					sinkPstmt.setBigDecimal(126, rs.getBigDecimal("COVERAGE_YEAR_"));
					sinkPstmt.setString(127, rs.getString("COVERAGE_PERIOD_"));
					sinkPstmt.setBigDecimal(128, rs.getBigDecimal("CHARGE_YEAR_"));
					sinkPstmt.setString(129, rs.getString("CHARGE_PERIOD_"));
					sinkPstmt.setBigDecimal(130, rs.getBigDecimal("AMOUNT_"));
					sinkPstmt.setString(131, rs.getString("CHANNEL_CODE_"));
					sinkPstmt.setString(132, rs.getString("INITIAL_TYPE_"));

					sinkPstmt.setLong(133, loadBean.currentScn);		// new column
					sinkPstmt.setLong(134, loadBean.currentScn);		// new column
					sinkPstmt.setString(135,  rs.getString("ROWID"));		// new column
					
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
