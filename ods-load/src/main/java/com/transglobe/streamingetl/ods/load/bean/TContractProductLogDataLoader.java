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

public class TContractProductLogDataLoader extends DataLoader {

	private static final Logger logger = LoggerFactory.getLogger(TContractProductLogDataLoader.class);

	private String sourceTableName ;

	private String sinkTableName;

	private String streamingEtlName;

	private String sinkTableCreateFile;

	private String sinkTableIndexesFile;

	public TContractProductLogDataLoader(Config config, Date dataDate) throws Exception {

		super(DataLoader.DEFAULT_THREADS, DataLoader.DEFAULT_BATCH_COMMIT_SIZE, config, dataDate);

		this.sourceTableName = config.sourceTableTContractProductLog;

		this.sinkTableName = config.sinkTableKContractProductLog;

		this.streamingEtlName = config.streamingEtlNameTContractProductLog;

		this.sinkTableCreateFile = config.sinkTableCreateFileKContractProductLog;

		this.sinkTableIndexesFile = config.sinkTableIndexesFileKContractProductLog;
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
		+ " CHANGE_ID"
		+ ",LOG_TYPE"
		+ ",POLICY_CHG_ID"
		+ ",ITEM_ID"
		+ ",MASTER_ID"
		+ ",POLICY_ID"
		+ ",PRODUCT_ID"
		+ ",AMOUNT"
		+ ",UNIT"
		+ ",APPLY_DATE"
		+ ",EXPIRY_DATE"
		+ ",VALIDATE_DATE"
		+ ",PAIDUP_DATE"
		+ ",LIABILITY_STATE"
		+ ",END_CAUSE"
		+ ",INITIAL_TYPE"
		+ ",RENEWAL_TYPE"
		+ ",CHARGE_PERIOD"
		+ ",CHARGE_YEAR"
		+ ",COVERAGE_PERIOD"
		+ ",COVERAGE_YEAR"
		+ ",SHORT_END_TIME"
		+ ",EXCEPT_VALUE"
		+ ",BENEFIT_LEVEL"
		+ ",INSURED_CATEGORY"
		+ ",SUSPEND"
		+ ",SUSPEND_CAUSE"
		+ ",DERIVATION"
		+ ",PAY_MODE"
		+ ",EXPIRY_CASH_VALUE"
		+ ",DECISION_ID"
		+ ",COUNT_WAY"
		+ ",RENEW_DECISION"
		+ ",BONUS_SA"
		+ ",PAY_NEXT"
		+ ",ANNI_BALANCE"
		+ ",FIX_INCREMENT"
		+ ",CPF_COST"
		+ ",CASH_COST"
		+ ",ORIGIN_SA"
		+ ",ORIGIN_BONUS_SA"
		+ ",RISK_CODE"
		+ ",EXPOSURE_RATE"
		+ ",REINS_RATE"
		+ ",SUSPEND_CHG_ID"
		+ ",NEXT_AMOUNT"
		+ ",WAIVER_START"
		+ ",WAIVER_END"
		+ ",AUTO_PERMNT_LAPSE"
		+ ",PERMNT_LAPSE_NOTICE_DATE"
		+ ",WAIVER"
		+ ",WAIVED_SA"
		+ ",ISSUE_AGENT"
		+ ",MASTER_PRODUCT"
		+ ",STRATEGY_CODE"
		+ ",LOAN_TYPE"
		+ ",BEN_PERIOD_TYPE"
		+ ",BENEFIT_PERIOD"
		+ ",GURNT_START_DATE"
		+ ",GURNT_PERD_TYPE"
		+ ",GURNT_PERIOD"
		+ ",INVEST_HORIZON"
		+ ",MANUAL_SA"
		+ ",DEFER_PERIOD"
		+ ",WAIT_PERIOD"
		+ ",NEXT_DISCNTED_PREM_AF"
		+ ",NEXT_POLICY_FEE_AF"
		+ ",NEXT_GROSS_PREM_AF"
		+ ",NEXT_EXTRA_PREM_AF"
		+ ",NEXT_TOTAL_PREM_AF"
		+ ",NEXT_STD_PREM_AN"
		+ ",NEXT_DISCNT_PREM_AN"
		+ ",NEXT_DISCNTED_PREM_AN"
		+ ",NEXT_POLICY_FEE_AN"
		+ ",NEXT_EXTRA_PREM_AN"
		+ ",WAIV_ANUL_BENEFIT"
		+ ",WAIV_ANUL_PREM"
		+ ",LAPSE_CAUSE"
		+ ",PREM_CHANGE_TIME"
		+ ",SUBMISSION_DATE"
		+ ",NEXT_DISCNTED_PREM_BF"
		+ ",NEXT_POLICY_FEE_BF"
		+ ",NEXT_EXTRA_PREM_BF"
		+ ",SA_FACTOR"
		+ ",NEXT_STD_PREM_AF"
		+ ",NEXT_DISCNT_PREM_AF"
		+ ",STD_PREM_BF"
		+ ",DISCNT_PREM_BF"
		+ ",DISCNTED_PREM_BF"
		+ ",POLICY_FEE_BF"
		+ ",EXTRA_PREM_BF"
		+ ",STD_PREM_AF"
		+ ",DISCNT_PREM_AF"
		+ ",POLICY_FEE_AF"
		+ ",GROSS_PREM_AF"
		+ ",EXTRA_PREM_AF"
		+ ",TOTAL_PREM_AF"
		+ ",STD_PREM_AN"
		+ ",DISCNT_PREM_AN"
		+ ",DISCNTED_PREM_AN"
		+ ",POLICY_FEE_AN"
		+ ",EXTRA_PREM_AN"
		+ ",NEXT_STD_PREM_BF"
		+ ",NEXT_DISCNT_PREM_BF"
		+ ",DISCNTED_PREM_AF"
		+ ",POLICY_PREM_SOURCE"
		+ ",ACTUAL_VALIDATE"
		+ ",ENTITY_FUND"
		+ ",CAR_REG_NO"
		+ ",MANU_SURR_VALUE"
		+ ",LOG_ID"
		+ ",ILP_CALC_SA"
		+ ",P_LAPSE_DATE"
		+ ",INITIAL_VALI_DATE"
		+ ",AGE_INCREASE_INDI"
		+ ",PAIDUP_OPTION"
		+ ",NEXT_COUNT_WAY"
		+ ",NEXT_UNIT"
		+ ",INSUR_PREM_AF"
		+ ",INSUR_PREM_AN"
		+ ",NEXT_INSUR_PREM_AF"
		+ ",NEXT_INSUR_PREM_AN"
		+ ",CAR_REG_NO_START"
		+ ",PHD_PERIOD"
		+ ",ORIGIN_PRODUCT_ID"
		+ ",LAST_STATEMENT_DATE"
		+ ",PRE_WAR_INDI"
		+ ",WAIVER_CLAIM_TYPE"
		+ ",ISSUE_DATE"
		+ ",ADVANTAGE_INDI"
		+ ",RISK_COMMENCE_DATE"
		+ ",LAST_CMT_FLG"
		+ ",EMS_VERSION"
		+ ",INSERTED_BY"
		+ ",UPDATED_BY"
		+ ",INSERT_TIME"
		+ ",UPDATE_TIME"
		+ ",INSERT_TIMESTAMP"
		+ ",UPDATE_TIMESTAMP"
		+ ",LIABILITY_STATE_CAUSE"
		+ ",LIABILITY_STATE_DATE"
		+ ",INVEST_SCHEME"
		+ ",TARIFF_TYPE"
		+ ",INDX_SUSPEND_CAUSE"
		+ ",INDX_TYPE"
		+ ",INDX_CALC_BASIS"
		+ ",INDX_INDI"
		+ ",COOLING_OFF_OPTION"
		+ ",POSTPONE_PERIOD"
		+ ",NEXT_BENEFIT_LEVEL"
		+ ",PRODUCT_VERSION_ID"
		+ ",RENEW_INDI"
		+ ",AGENCY_ORGAN_ID"
		+ ",RELATION_ORGAN_TO_PH"
		+ ",PH_ROLE"
		+ ",NHI_INSUR_INDI"
		+ ",VERSION_TYPE_ID"
		+ ",AGREE_READ_INDI"
		+ ",ITEM_ORDER"
		+ ",CUSTOMIZED_PREM"
		+ ",SAME_STD_PREM_INDI"
		+ ",PREMIUM_ADJUST_YEARLY"
		+ ",PREMIUM_ADJUST_HALFYEARLY"
		+ ",PREMIUM_ADJUST_QUARTERLY"
		+ ",PREMIUM_ADJUST_MONTHLY"
		+ ",PROPOSAL_TERM"
		+ ",COMMISSION_VERSION"
		+ ",INITIAL_SA"
		+ ",OLD_PRODUCT_CODE"
		+ ",COMPAIGN_CODE"
		+ ",INITIAL_CHARGE_MODE"
		+ ",CLAIM_STATUS"
		+ ",INITIAL_TOTAL_PREM"
		+ ",OVERWITHDRAW_INDI"
		+ ",PRODUCT_TYPE"
		+ ",INSURABILITY_INDICATOR"
		+ ",ETI_SUR"
		+ ",ETI_DAYS"
		+ ",PREMIUM_IS_ZERO_INDI"
		+ ",ILP_INCREASE_RATE"
		+ ",SPECIAL_EFFECT_DATE"
		+ ",NSP_AMOUNT"
		+ ",PROPOASL_TERM_TYPE"
		+ ",TOTAL_PAID_PREM"
		+ ",ETI_YEARS"
		+ ",LOADING_RATE_POINTER"
		+ ",CANCER_DATE"
		+ ",INSURABILITY_EXTRA_EXECUATE"
		+ ",LAST_CLAIM_DATE"
		+ ",GUARANTEE_OPTION"
		+ ",PREM_ALLOC_YEAR_INDI"
		+ ",NAR"
		+ ",HIDE_VALIDATE_DATE_INDI"
		+ ",ETI_SUR_PB"
		+ ",ETA_PAIDUP_AMOUNT"
		+ ",ORI_PREMIUM_ADJUST_YEARLY"
		+ ",ORI_PREMIUM_ADJUST_HALFYEARLY"
		+ ",ORI_PREMIUM_ADJUST_QUARTERLY"
		+ ",ORI_PREMIUM_ADJUST_MONTHLY"
		+ ",CLAIM_DISABILITY_RATE"
		+ ",HEALTH_INSURANCE_INDI"
		+ ",INSURANCE_NOTICE_INDI"
		+ ",SERVICES_BENEFIT_LEVEL"
		+ ",PAYMENT_FREQ"
		+ ",ISSUE_TYPE"
		+ ",LOADING_RATE_POINTER_REASON"
		+ ",RELATION_TO_PH_ROLE"
		+ ",MONEY_DENOMINATED"
		+ ",ORIGIN_PRODUCT_VERSION_ID"
		+ ",TRANSFORM_DATE"
		+ ",HEALTH_INSURANCE_VERSION"
		+ " from " + this.sourceTableName
		+ " a where ? <= a.LOG_ID and a.LOG_ID < ?";
	}

	@Override
	protected String getInsertSql() {
		return "insert into " + this.sinkTableName
				+ " (CHANGE_ID"
				+ ",LOG_TYPE"
				+ ",POLICY_CHG_ID"
				+ ",ITEM_ID"
				+ ",MASTER_ID"
				+ ",POLICY_ID"
				+ ",PRODUCT_ID"
				+ ",AMOUNT"
				+ ",UNIT"
				+ ",APPLY_DATE"
				+ ",EXPIRY_DATE"
				+ ",VALIDATE_DATE"
				+ ",PAIDUP_DATE"
				+ ",LIABILITY_STATE"
				+ ",END_CAUSE"
				+ ",INITIAL_TYPE"
				+ ",RENEWAL_TYPE"
				+ ",CHARGE_PERIOD"
				+ ",CHARGE_YEAR"
				+ ",COVERAGE_PERIOD"
				+ ",COVERAGE_YEAR"
				+ ",SHORT_END_TIME"
				+ ",EXCEPT_VALUE"
				+ ",BENEFIT_LEVEL"
				+ ",INSURED_CATEGORY"
				+ ",SUSPEND"
				+ ",SUSPEND_CAUSE"
				+ ",DERIVATION"
				+ ",PAY_MODE"
				+ ",EXPIRY_CASH_VALUE"
				+ ",DECISION_ID"
				+ ",COUNT_WAY"
				+ ",RENEW_DECISION"
				+ ",BONUS_SA"
				+ ",PAY_NEXT"
				+ ",ANNI_BALANCE"
				+ ",FIX_INCREMENT"
				+ ",CPF_COST"
				+ ",CASH_COST"
				+ ",ORIGIN_SA"
				+ ",ORIGIN_BONUS_SA"
				+ ",RISK_CODE"
				+ ",EXPOSURE_RATE"
				+ ",REINS_RATE"
				+ ",SUSPEND_CHG_ID"
				+ ",NEXT_AMOUNT"
				+ ",WAIVER_START"
				+ ",WAIVER_END"
				+ ",AUTO_PERMNT_LAPSE"
				+ ",PERMNT_LAPSE_NOTICE_DATE"
				+ ",WAIVER"
				+ ",WAIVED_SA"
				+ ",ISSUE_AGENT"
				+ ",MASTER_PRODUCT"
				+ ",STRATEGY_CODE"
				+ ",LOAN_TYPE"
				+ ",BEN_PERIOD_TYPE"
				+ ",BENEFIT_PERIOD"
				+ ",GURNT_START_DATE"
				+ ",GURNT_PERD_TYPE"
				+ ",GURNT_PERIOD"
				+ ",INVEST_HORIZON"
				+ ",MANUAL_SA"
				+ ",DEFER_PERIOD"
				+ ",WAIT_PERIOD"
				+ ",NEXT_DISCNTED_PREM_AF"
				+ ",NEXT_POLICY_FEE_AF"
				+ ",NEXT_GROSS_PREM_AF"
				+ ",NEXT_EXTRA_PREM_AF"
				+ ",NEXT_TOTAL_PREM_AF"
				+ ",NEXT_STD_PREM_AN"
				+ ",NEXT_DISCNT_PREM_AN"
				+ ",NEXT_DISCNTED_PREM_AN"
				+ ",NEXT_POLICY_FEE_AN"
				+ ",NEXT_EXTRA_PREM_AN"
				+ ",WAIV_ANUL_BENEFIT"
				+ ",WAIV_ANUL_PREM"
				+ ",LAPSE_CAUSE"
				+ ",PREM_CHANGE_TIME"
				+ ",SUBMISSION_DATE"
				+ ",NEXT_DISCNTED_PREM_BF"
				+ ",NEXT_POLICY_FEE_BF"
				+ ",NEXT_EXTRA_PREM_BF"
				+ ",SA_FACTOR"
				+ ",NEXT_STD_PREM_AF"
				+ ",NEXT_DISCNT_PREM_AF"
				+ ",STD_PREM_BF"
				+ ",DISCNT_PREM_BF"
				+ ",DISCNTED_PREM_BF"
				+ ",POLICY_FEE_BF"
				+ ",EXTRA_PREM_BF"
				+ ",STD_PREM_AF"
				+ ",DISCNT_PREM_AF"
				+ ",POLICY_FEE_AF"
				+ ",GROSS_PREM_AF"
				+ ",EXTRA_PREM_AF"
				+ ",TOTAL_PREM_AF"
				+ ",STD_PREM_AN"
				+ ",DISCNT_PREM_AN"
				+ ",DISCNTED_PREM_AN"
				+ ",POLICY_FEE_AN"
				+ ",EXTRA_PREM_AN"
				+ ",NEXT_STD_PREM_BF"
				+ ",NEXT_DISCNT_PREM_BF"
				+ ",DISCNTED_PREM_AF"
				+ ",POLICY_PREM_SOURCE"
				+ ",ACTUAL_VALIDATE"
				+ ",ENTITY_FUND"
				+ ",CAR_REG_NO"
				+ ",MANU_SURR_VALUE"
				+ ",LOG_ID"
				+ ",ILP_CALC_SA"
				+ ",P_LAPSE_DATE"
				+ ",INITIAL_VALI_DATE"
				+ ",AGE_INCREASE_INDI"
				+ ",PAIDUP_OPTION"
				+ ",NEXT_COUNT_WAY"
				+ ",NEXT_UNIT"
				+ ",INSUR_PREM_AF"
				+ ",INSUR_PREM_AN"
				+ ",NEXT_INSUR_PREM_AF"
				+ ",NEXT_INSUR_PREM_AN"
				+ ",CAR_REG_NO_START"
				+ ",PHD_PERIOD"
				+ ",ORIGIN_PRODUCT_ID"
				+ ",LAST_STATEMENT_DATE"
				+ ",PRE_WAR_INDI"
				+ ",WAIVER_CLAIM_TYPE"
				+ ",ISSUE_DATE"
				+ ",ADVANTAGE_INDI"
				+ ",RISK_COMMENCE_DATE"
				+ ",LAST_CMT_FLG"
				+ ",EMS_VERSION"
				+ ",INSERTED_BY"
				+ ",UPDATED_BY"
				+ ",INSERT_TIME"
				+ ",UPDATE_TIME"
				+ ",INSERT_TIMESTAMP"
				+ ",UPDATE_TIMESTAMP"
				+ ",LIABILITY_STATE_CAUSE"
				+ ",LIABILITY_STATE_DATE"
				+ ",INVEST_SCHEME"
				+ ",TARIFF_TYPE"
				+ ",INDX_SUSPEND_CAUSE"
				+ ",INDX_TYPE"
				+ ",INDX_CALC_BASIS"
				+ ",INDX_INDI"
				+ ",COOLING_OFF_OPTION"
				+ ",POSTPONE_PERIOD"
				+ ",NEXT_BENEFIT_LEVEL"
				+ ",PRODUCT_VERSION_ID"
				+ ",RENEW_INDI"
				+ ",AGENCY_ORGAN_ID"
				+ ",RELATION_ORGAN_TO_PH"
				+ ",PH_ROLE"
				+ ",NHI_INSUR_INDI"
				+ ",VERSION_TYPE_ID"
				+ ",AGREE_READ_INDI"
				+ ",ITEM_ORDER"
				+ ",CUSTOMIZED_PREM"
				+ ",SAME_STD_PREM_INDI"
				+ ",PREMIUM_ADJUST_YEARLY"
				+ ",PREMIUM_ADJUST_HALFYEARLY"
				+ ",PREMIUM_ADJUST_QUARTERLY"
				+ ",PREMIUM_ADJUST_MONTHLY"
				+ ",PROPOSAL_TERM"
				+ ",COMMISSION_VERSION"
				+ ",INITIAL_SA"
				+ ",OLD_PRODUCT_CODE"
				+ ",COMPAIGN_CODE"
				+ ",INITIAL_CHARGE_MODE"
				+ ",CLAIM_STATUS"
				+ ",INITIAL_TOTAL_PREM"
				+ ",OVERWITHDRAW_INDI"
				+ ",PRODUCT_TYPE"
				+ ",INSURABILITY_INDICATOR"
				+ ",ETI_SUR"
				+ ",ETI_DAYS"
				+ ",PREMIUM_IS_ZERO_INDI"
				+ ",ILP_INCREASE_RATE"
				+ ",SPECIAL_EFFECT_DATE"
				+ ",NSP_AMOUNT"
				+ ",PROPOASL_TERM_TYPE"
				+ ",TOTAL_PAID_PREM"
				+ ",ETI_YEARS"
				+ ",LOADING_RATE_POINTER"
				+ ",CANCER_DATE"
				+ ",INSURABILITY_EXTRA_EXECUATE"
				+ ",LAST_CLAIM_DATE"
				+ ",GUARANTEE_OPTION"
				+ ",PREM_ALLOC_YEAR_INDI"
				+ ",NAR"
				+ ",DATA_DATE" 		// ods add column 
				+ ",TBL_UPD_TIME"	// ods add column
				+ ",TBL_UPD_SCN)"	// new column
				+ " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
				+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_DATE,?)";
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

					sinkPstmt.setBigDecimal(1, rs.getBigDecimal("CHANGE_ID"));
					sinkPstmt.setString(2, rs.getString("LOG_TYPE"));
					sinkPstmt.setBigDecimal(3, rs.getBigDecimal("POLICY_CHG_ID"));
					sinkPstmt.setBigDecimal(4, rs.getBigDecimal("ITEM_ID"));
					sinkPstmt.setBigDecimal(5, rs.getBigDecimal("MASTER_ID"));
					sinkPstmt.setBigDecimal(6, rs.getBigDecimal("POLICY_ID"));
					sinkPstmt.setBigDecimal(7, rs.getBigDecimal("PRODUCT_ID"));
					sinkPstmt.setBigDecimal(8, rs.getBigDecimal("AMOUNT"));
					sinkPstmt.setBigDecimal(9, rs.getBigDecimal("UNIT"));
					sinkPstmt.setDate(10, rs.getDate("APPLY_DATE"));
					sinkPstmt.setDate(11, rs.getDate("EXPIRY_DATE"));
					sinkPstmt.setDate(12, rs.getDate("VALIDATE_DATE"));
					sinkPstmt.setDate(13, rs.getDate("PAIDUP_DATE"));
					sinkPstmt.setBigDecimal(14, rs.getBigDecimal("LIABILITY_STATE"));
					sinkPstmt.setBigDecimal(15, rs.getBigDecimal("END_CAUSE"));
					sinkPstmt.setString(16, rs.getString("INITIAL_TYPE"));
					sinkPstmt.setString(17, rs.getString("RENEWAL_TYPE"));
					sinkPstmt.setString(18, rs.getString("CHARGE_PERIOD"));
					sinkPstmt.setBigDecimal(19, rs.getBigDecimal("CHARGE_YEAR"));
					sinkPstmt.setString(20, rs.getString("COVERAGE_PERIOD"));
					sinkPstmt.setBigDecimal(21, rs.getBigDecimal("COVERAGE_YEAR"));
					sinkPstmt.setDate(22, rs.getDate("SHORT_END_TIME"));
					sinkPstmt.setBigDecimal(23, rs.getBigDecimal("EXCEPT_VALUE"));
					sinkPstmt.setString(24, rs.getString("BENEFIT_LEVEL"));
					sinkPstmt.setString(25, rs.getString("INSURED_CATEGORY"));
					sinkPstmt.setString(26, rs.getString("SUSPEND"));
					sinkPstmt.setBigDecimal(27, rs.getBigDecimal("SUSPEND_CAUSE"));
					sinkPstmt.setString(28, rs.getString("DERIVATION"));
					sinkPstmt.setBigDecimal(29, rs.getBigDecimal("PAY_MODE"));
					sinkPstmt.setBigDecimal(30, rs.getBigDecimal("EXPIRY_CASH_VALUE"));
					sinkPstmt.setBigDecimal(31, rs.getBigDecimal("DECISION_ID"));
					sinkPstmt.setString(32, rs.getString("COUNT_WAY"));
					sinkPstmt.setString(33, rs.getString("RENEW_DECISION"));
					sinkPstmt.setBigDecimal(34, rs.getBigDecimal("BONUS_SA"));
					sinkPstmt.setBigDecimal(35, rs.getBigDecimal("PAY_NEXT"));
					sinkPstmt.setBigDecimal(36, rs.getBigDecimal("ANNI_BALANCE"));
					sinkPstmt.setString(37, rs.getString("FIX_INCREMENT"));
					sinkPstmt.setBigDecimal(38, rs.getBigDecimal("CPF_COST"));
					sinkPstmt.setBigDecimal(39, rs.getBigDecimal("CASH_COST"));
					sinkPstmt.setBigDecimal(40, rs.getBigDecimal("ORIGIN_SA"));
					sinkPstmt.setBigDecimal(41, rs.getBigDecimal("ORIGIN_BONUS_SA"));
					sinkPstmt.setBigDecimal(42, rs.getBigDecimal("RISK_CODE"));
					sinkPstmt.setBigDecimal(43, rs.getBigDecimal("EXPOSURE_RATE"));
					sinkPstmt.setBigDecimal(44, rs.getBigDecimal("REINS_RATE"));
					sinkPstmt.setBigDecimal(45, rs.getBigDecimal("SUSPEND_CHG_ID"));
					sinkPstmt.setBigDecimal(46, rs.getBigDecimal("NEXT_AMOUNT"));
					sinkPstmt.setDate(47, rs.getDate("WAIVER_START"));
					sinkPstmt.setDate(48, rs.getDate("WAIVER_END"));
					sinkPstmt.setString(49, rs.getString("AUTO_PERMNT_LAPSE"));
					sinkPstmt.setDate(50, rs.getDate("PERMNT_LAPSE_NOTICE_DATE"));
					sinkPstmt.setString(51, rs.getString("WAIVER"));
					sinkPstmt.setBigDecimal(52, rs.getBigDecimal("WAIVED_SA"));
					sinkPstmt.setBigDecimal(53, rs.getBigDecimal("ISSUE_AGENT"));
					sinkPstmt.setBigDecimal(54, rs.getBigDecimal("MASTER_PRODUCT"));
					sinkPstmt.setString(55, rs.getString("STRATEGY_CODE"));
					sinkPstmt.setString(56, rs.getString("LOAN_TYPE"));
					sinkPstmt.setString(57, rs.getString("BEN_PERIOD_TYPE"));
					sinkPstmt.setBigDecimal(58, rs.getBigDecimal("BENEFIT_PERIOD"));
					sinkPstmt.setDate(59, rs.getDate("GURNT_START_DATE"));
					sinkPstmt.setString(60, rs.getString("GURNT_PERD_TYPE"));
					sinkPstmt.setBigDecimal(61, rs.getBigDecimal("GURNT_PERIOD"));
					sinkPstmt.setBigDecimal(62, rs.getBigDecimal("INVEST_HORIZON"));
					sinkPstmt.setString(63, rs.getString("MANUAL_SA"));
					sinkPstmt.setBigDecimal(64, rs.getBigDecimal("DEFER_PERIOD"));
					sinkPstmt.setBigDecimal(65, rs.getBigDecimal("WAIT_PERIOD"));
					sinkPstmt.setBigDecimal(66, rs.getBigDecimal("NEXT_DISCNTED_PREM_AF"));
					sinkPstmt.setBigDecimal(67, rs.getBigDecimal("NEXT_POLICY_FEE_AF"));
					sinkPstmt.setBigDecimal(68, rs.getBigDecimal("NEXT_GROSS_PREM_AF"));
					sinkPstmt.setBigDecimal(69, rs.getBigDecimal("NEXT_EXTRA_PREM_AF"));
					sinkPstmt.setBigDecimal(70, rs.getBigDecimal("NEXT_TOTAL_PREM_AF"));
					sinkPstmt.setBigDecimal(71, rs.getBigDecimal("NEXT_STD_PREM_AN"));
					sinkPstmt.setBigDecimal(72, rs.getBigDecimal("NEXT_DISCNT_PREM_AN"));
					sinkPstmt.setBigDecimal(73, rs.getBigDecimal("NEXT_DISCNTED_PREM_AN"));
					sinkPstmt.setBigDecimal(74, rs.getBigDecimal("NEXT_POLICY_FEE_AN"));
					sinkPstmt.setBigDecimal(75, rs.getBigDecimal("NEXT_EXTRA_PREM_AN"));
					sinkPstmt.setBigDecimal(76, rs.getBigDecimal("WAIV_ANUL_BENEFIT"));
					sinkPstmt.setBigDecimal(77, rs.getBigDecimal("WAIV_ANUL_PREM"));
					sinkPstmt.setBigDecimal(78, rs.getBigDecimal("LAPSE_CAUSE"));
					sinkPstmt.setDate(79, rs.getDate("PREM_CHANGE_TIME"));
					sinkPstmt.setDate(80, rs.getDate("SUBMISSION_DATE"));
					sinkPstmt.setBigDecimal(81, rs.getBigDecimal("NEXT_DISCNTED_PREM_BF"));
					sinkPstmt.setBigDecimal(82, rs.getBigDecimal("NEXT_POLICY_FEE_BF"));
					sinkPstmt.setBigDecimal(83, rs.getBigDecimal("NEXT_EXTRA_PREM_BF"));
					sinkPstmt.setBigDecimal(84, rs.getBigDecimal("SA_FACTOR"));
					sinkPstmt.setBigDecimal(85, rs.getBigDecimal("NEXT_STD_PREM_AF"));
					sinkPstmt.setBigDecimal(86, rs.getBigDecimal("NEXT_DISCNT_PREM_AF"));
					sinkPstmt.setBigDecimal(87, rs.getBigDecimal("STD_PREM_BF"));
					sinkPstmt.setBigDecimal(88, rs.getBigDecimal("DISCNT_PREM_BF"));
					sinkPstmt.setBigDecimal(89, rs.getBigDecimal("DISCNTED_PREM_BF"));
					sinkPstmt.setBigDecimal(90, rs.getBigDecimal("POLICY_FEE_BF"));
					sinkPstmt.setBigDecimal(91, rs.getBigDecimal("EXTRA_PREM_BF"));
					sinkPstmt.setBigDecimal(92, rs.getBigDecimal("STD_PREM_AF"));
					sinkPstmt.setBigDecimal(93, rs.getBigDecimal("DISCNT_PREM_AF"));
					sinkPstmt.setBigDecimal(94, rs.getBigDecimal("POLICY_FEE_AF"));
					sinkPstmt.setBigDecimal(95, rs.getBigDecimal("GROSS_PREM_AF"));
					sinkPstmt.setBigDecimal(96, rs.getBigDecimal("EXTRA_PREM_AF"));
					sinkPstmt.setBigDecimal(97, rs.getBigDecimal("TOTAL_PREM_AF"));
					sinkPstmt.setBigDecimal(98, rs.getBigDecimal("STD_PREM_AN"));
					sinkPstmt.setBigDecimal(99, rs.getBigDecimal("DISCNT_PREM_AN"));
					sinkPstmt.setBigDecimal(100, rs.getBigDecimal("DISCNTED_PREM_AN"));
					sinkPstmt.setBigDecimal(101, rs.getBigDecimal("POLICY_FEE_AN"));
					sinkPstmt.setBigDecimal(102, rs.getBigDecimal("EXTRA_PREM_AN"));
					sinkPstmt.setBigDecimal(103, rs.getBigDecimal("NEXT_STD_PREM_BF"));
					sinkPstmt.setBigDecimal(104, rs.getBigDecimal("NEXT_DISCNT_PREM_BF"));
					sinkPstmt.setBigDecimal(105, rs.getBigDecimal("DISCNTED_PREM_AF"));
					sinkPstmt.setString(106, rs.getString("POLICY_PREM_SOURCE"));
					sinkPstmt.setDate(107, rs.getDate("ACTUAL_VALIDATE"));
					sinkPstmt.setString(108, rs.getString("ENTITY_FUND"));
					sinkPstmt.setString(109, rs.getString("CAR_REG_NO"));
					sinkPstmt.setString(110, rs.getString("MANU_SURR_VALUE"));
					sinkPstmt.setBigDecimal(111, rs.getBigDecimal("LOG_ID"));
					sinkPstmt.setBigDecimal(112, rs.getBigDecimal("ILP_CALC_SA"));
					sinkPstmt.setDate(113, rs.getDate("P_LAPSE_DATE"));
					sinkPstmt.setDate(114, rs.getDate("INITIAL_VALI_DATE"));
					sinkPstmt.setString(115, rs.getString("AGE_INCREASE_INDI"));
					sinkPstmt.setString(116, rs.getString("PAIDUP_OPTION"));
					sinkPstmt.setString(117, rs.getString("NEXT_COUNT_WAY"));
					sinkPstmt.setBigDecimal(118, rs.getBigDecimal("NEXT_UNIT"));
					sinkPstmt.setBigDecimal(119, rs.getBigDecimal("INSUR_PREM_AF"));
					sinkPstmt.setBigDecimal(120, rs.getBigDecimal("INSUR_PREM_AN"));
					sinkPstmt.setBigDecimal(121, rs.getBigDecimal("NEXT_INSUR_PREM_AF"));
					sinkPstmt.setBigDecimal(122, rs.getBigDecimal("NEXT_INSUR_PREM_AN"));
					sinkPstmt.setDate(123, rs.getDate("CAR_REG_NO_START"));
					sinkPstmt.setBigDecimal(124, rs.getBigDecimal("PHD_PERIOD"));
					sinkPstmt.setBigDecimal(125, rs.getBigDecimal("ORIGIN_PRODUCT_ID"));
					sinkPstmt.setDate(126, rs.getDate("LAST_STATEMENT_DATE"));
					sinkPstmt.setString(127, rs.getString("PRE_WAR_INDI"));
					sinkPstmt.setBigDecimal(128, rs.getBigDecimal("WAIVER_CLAIM_TYPE"));
					sinkPstmt.setDate(129, rs.getDate("ISSUE_DATE"));
					sinkPstmt.setString(130, rs.getString("ADVANTAGE_INDI"));
					sinkPstmt.setDate(131, rs.getDate("RISK_COMMENCE_DATE"));
					sinkPstmt.setString(132, rs.getString("LAST_CMT_FLG"));
					sinkPstmt.setBigDecimal(133, rs.getBigDecimal("EMS_VERSION"));
					sinkPstmt.setBigDecimal(134, rs.getBigDecimal("INSERTED_BY"));
					sinkPstmt.setBigDecimal(135, rs.getBigDecimal("UPDATED_BY"));
					sinkPstmt.setDate(136, rs.getDate("INSERT_TIME"));
					sinkPstmt.setDate(137, rs.getDate("UPDATE_TIME"));
					sinkPstmt.setDate(138, rs.getDate("INSERT_TIMESTAMP"));
					sinkPstmt.setDate(139, rs.getDate("UPDATE_TIMESTAMP"));
					sinkPstmt.setBigDecimal(140, rs.getBigDecimal("LIABILITY_STATE_CAUSE"));
					sinkPstmt.setDate(141, rs.getDate("LIABILITY_STATE_DATE"));
					sinkPstmt.setString(142, rs.getString("INVEST_SCHEME"));
					sinkPstmt.setBigDecimal(143, rs.getBigDecimal("TARIFF_TYPE"));
					sinkPstmt.setBigDecimal(144, rs.getBigDecimal("INDX_SUSPEND_CAUSE"));
					sinkPstmt.setBigDecimal(145, rs.getBigDecimal("INDX_TYPE"));
					sinkPstmt.setBigDecimal(146, rs.getBigDecimal("INDX_CALC_BASIS"));
					sinkPstmt.setString(147, rs.getString("INDX_INDI"));
					sinkPstmt.setBigDecimal(148, rs.getBigDecimal("COOLING_OFF_OPTION"));
					sinkPstmt.setBigDecimal(149, rs.getBigDecimal("POSTPONE_PERIOD"));
					sinkPstmt.setString(150, rs.getString("NEXT_BENEFIT_LEVEL"));
					sinkPstmt.setBigDecimal(151, rs.getBigDecimal("PRODUCT_VERSION_ID"));
					sinkPstmt.setString(152, rs.getString("RENEW_INDI"));
					sinkPstmt.setString(153, rs.getString("AGENCY_ORGAN_ID"));
					sinkPstmt.setString(154, rs.getString("RELATION_ORGAN_TO_PH"));
					sinkPstmt.setString(155, rs.getString("PH_ROLE"));
					sinkPstmt.setString(156, rs.getString("NHI_INSUR_INDI"));
					sinkPstmt.setBigDecimal(157, rs.getBigDecimal("VERSION_TYPE_ID"));
					sinkPstmt.setString(158, rs.getString("AGREE_READ_INDI"));
					sinkPstmt.setBigDecimal(159, rs.getBigDecimal("ITEM_ORDER"));
					sinkPstmt.setBigDecimal(160, rs.getBigDecimal("CUSTOMIZED_PREM"));
					sinkPstmt.setString(161, rs.getString("SAME_STD_PREM_INDI"));
					sinkPstmt.setBigDecimal(162, rs.getBigDecimal("PREMIUM_ADJUST_YEARLY"));
					sinkPstmt.setBigDecimal(163, rs.getBigDecimal("PREMIUM_ADJUST_HALFYEARLY"));
					sinkPstmt.setBigDecimal(164, rs.getBigDecimal("PREMIUM_ADJUST_QUARTERLY"));
					sinkPstmt.setBigDecimal(165, rs.getBigDecimal("PREMIUM_ADJUST_MONTHLY"));
					sinkPstmt.setBigDecimal(166, rs.getBigDecimal("PROPOSAL_TERM"));
					sinkPstmt.setBigDecimal(167, rs.getBigDecimal("COMMISSION_VERSION"));
					sinkPstmt.setBigDecimal(168, rs.getBigDecimal("INITIAL_SA"));
					sinkPstmt.setString(169, rs.getString("OLD_PRODUCT_CODE"));
					sinkPstmt.setString(170, rs.getString("COMPAIGN_CODE"));
					sinkPstmt.setString(171, rs.getString("INITIAL_CHARGE_MODE"));
					sinkPstmt.setString(172, rs.getString("CLAIM_STATUS"));
					sinkPstmt.setBigDecimal(173, rs.getBigDecimal("INITIAL_TOTAL_PREM"));
					sinkPstmt.setString(174, rs.getString("OVERWITHDRAW_INDI"));
					sinkPstmt.setString(175, rs.getString("PRODUCT_TYPE"));
					sinkPstmt.setString(176, rs.getString("INSURABILITY_INDICATOR"));
					sinkPstmt.setBigDecimal(177, rs.getBigDecimal("ETI_SUR"));
					sinkPstmt.setBigDecimal(178, rs.getBigDecimal("ETI_DAYS"));
					sinkPstmt.setString(179, rs.getString("PREMIUM_IS_ZERO_INDI"));
					sinkPstmt.setBigDecimal(180, rs.getBigDecimal("ILP_INCREASE_RATE"));
					sinkPstmt.setDate(181, rs.getDate("SPECIAL_EFFECT_DATE"));
					sinkPstmt.setBigDecimal(182, rs.getBigDecimal("NSP_AMOUNT"));
					sinkPstmt.setString(183, rs.getString("PROPOASL_TERM_TYPE"));
					sinkPstmt.setBigDecimal(184, rs.getBigDecimal("TOTAL_PAID_PREM"));
					sinkPstmt.setBigDecimal(185, rs.getBigDecimal("ETI_YEARS"));
					sinkPstmt.setString(186, rs.getString("LOADING_RATE_POINTER"));
					sinkPstmt.setDate(187, rs.getDate("CANCER_DATE"));
					sinkPstmt.setString(188, rs.getString("INSURABILITY_EXTRA_EXECUATE"));
					sinkPstmt.setDate(189, rs.getDate("LAST_CLAIM_DATE"));
					sinkPstmt.setString(190, rs.getString("GUARANTEE_OPTION"));
					sinkPstmt.setString(191, rs.getString("PREM_ALLOC_YEAR_INDI"));
					sinkPstmt.setBigDecimal(192, rs.getBigDecimal("NAR"));
					sinkPstmt.setDate(193, dataDate);
					
					//TBL_UPD_TIME
					sinkPstmt.setLong(194, loadBean.currentScn);				// new column				// new column

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
