package com.transglobe.streamingetl.ods.load;

import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.Ignition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * InitialLoadApp [loaddata]
 *  dataload rule
 *  1. 因BSD規則調整,受益人的email部份,畫面並沒有輸入t_contract_bene.email雖有值但不做比對
 *  so, set email to null if role_type = 3
 *  
 *  
 *  select  a.LIST_ID,a.POLICY_ID,a.NAME, 
a.CERTI_CODE,a.MOBILE_TEL, a.EMAIL,a.ADDRESS_ID,b.ADDRESS_1
  from %tlogtable% a 
  left join T_ADDRESS b on a.ADDRESS_ID = b.ADDRESS_ID 
  where LAST_CMT_FLG = 'Y'
union  
select a.LIST_ID,a.POLICY_ID,a.NAME, a.CERTI_CODE,a.MOBILE_TEL,a.EMAIL,a.ADDRESS_ID,c.ADDRESS_1
  from %ttable% A
 inner join T_CONTRACT_MASTER B ON A.POLICY_ID=B.POLICY_ID 
  left join T_ADDRESS c on A.ADDRESS_ID = C.ADDRESS_ID 
  where B.LIABILITY_STATE = 0;


where %ttable% = T_POLICY_HOLDER,T_INSURED_LIST,T_CONTRACT_BENE 
and %tlogtable% = T_POLICY_HOLDER_LOG,T_INSURED_LIST_LOG,T_CONTRACT_BENE_LOG

 * @author oracle
 *
 */
public class InitialLoadApp {
	private static final Logger logger = LoggerFactory.getLogger(InitialLoadApp.class);

	private static final String CONFIG_FILE_NAME = "config.properties";
	private static final String CREATE_TABLE_FILE_NAME_CONTRACT_PRODUCT_LOG = "createtable-T_CONTRACT_PRODUCT_LOG.sql";
	private static final String CREATE_TABLE_FILE_NAME_PRODUCTION_DETAIL = "createtable-T_PRODUCTION_DETAIL.sql";
	private static final String CREATE_STREAMING_ETL_HEALTH_TABLE_FILE_NAME = "createtable-T_STREAMING_ETL_HEALTH.sql";

	private static final int THREADS = 15;

	//	private static final long SEQ_INTERVAL = 1000000L;

	private BasicDataSource sourceConnectionPool;
	private BasicDataSource sinkConnectionPool;

	public String sourceTableContractProductLog;
	public String sourceTableProductionDetail;

	public String sinkTableContractProductLog;
	public String sinkTableProductionDetail;
	public String sinkTableStreamingEtlHealth;

	static class LoadBean {
		String tableName;
		String selectSql;
		Long startSeq;
		Long endSeq;
	}
	private Config config;

	public InitialLoadApp(String fileName) throws Exception {
		config = Config.getConfig(fileName);

		sourceConnectionPool = new BasicDataSource();

		sourceConnectionPool.setUrl(config.sourceDbUrl);
		sourceConnectionPool.setUsername(config.sourceDbUsername);
		sourceConnectionPool.setPassword(config.sourceDbPassword);
		sourceConnectionPool.setDriverClassName(config.sourceDbDriver);
		sourceConnectionPool.setMaxTotal(THREADS);

		sinkConnectionPool = new BasicDataSource();
		sinkConnectionPool.setUrl(config.sinkDbUrl);
		sinkConnectionPool.setDriverClassName(config.sinkDbDriver);
		sinkConnectionPool.setMaxTotal(THREADS);

		sourceTableContractProductLog = config.sourceTableContractProductLog;
		sourceTableProductionDetail = config.sourceTableProductionDetail;

		sinkTableContractProductLog = config.sinkTableContractProductLog;
		sinkTableProductionDetail = config.sinkTableProductionDetail;
		sinkTableStreamingEtlHealth = config.sinkTableStreamingEtlHealth;
	}
	private void close() {
		try {
			if (sourceConnectionPool != null) sourceConnectionPool.close();
		} catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
		}
		try {
			if (sinkConnectionPool != null) sinkConnectionPool.close();
		} catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
		}
	}
	public static void main(String[] args) {
		logger.info(">>> start run InitialLoadApp");

		boolean noload = false;
		if (args.length != 0 && StringUtils.equals("noload", args[0])) {
			noload = true;
		}


		Long t0 = System.currentTimeMillis();

		String profileActive = System.getProperty("profile.active", "");
		logger.info(">>>>>profileActive={}", profileActive);
		try {
			String configFile = StringUtils.isBlank(profileActive)? CONFIG_FILE_NAME : profileActive + "/" + CONFIG_FILE_NAME;

			InitialLoadApp app = new InitialLoadApp(configFile);

			// create sink table
			logger.info(">>>  Start: dropTable");
			app.dropTable(app.sinkTableContractProductLog);
			app.dropTable(app.sinkTableProductionDetail);
			app.dropTable(app.sinkTableStreamingEtlHealth);
			logger.info(">>>  End: dropTable DONE!!!");

			logger.info(">>>  Start: createTable");			
			app.createTable(CREATE_TABLE_FILE_NAME_CONTRACT_PRODUCT_LOG);
			app.createTable(CREATE_TABLE_FILE_NAME_PRODUCTION_DETAIL);
			app.createTable(CREATE_STREAMING_ETL_HEALTH_TABLE_FILE_NAME);

			logger.info(">>>  End: createTable DONE!!!");

			logger.info("init tables span={}, ", (System.currentTimeMillis() - t0));						

			if (!noload) {
				app.run();

				//	app.runTLog();
			}
			logger.info("run load data span={}, ", (System.currentTimeMillis() - t0));

			// create indexes
			//	app.runCreateIndexes();


			app.close();


			logger.info("total load span={}, ", (System.currentTimeMillis() - t0));


			System.exit(0);

		} catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			System.exit(1);
		}

	}
	private Map<String, String> loadData(LoadBean loadBean){
		Map<String, String> map = new HashMap<>();
		if (StringUtils.equals(config.sourceTableContractProductLog, loadBean.tableName)) {
			map = loadContractProductLog(loadBean);
		}

		return map;
	}
	private Map<String, String> loadContractProductLog(LoadBean loadBean){
		Console cnsl = null;
		Map<String, String> map = new HashMap<>();
		Connection sourceConn = null;
		Connection sinkConn = null;
		PreparedStatement pstmtSource = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = null;
		try {
			sql = loadBean.selectSql;
			sourceConn = this.sourceConnectionPool.getConnection();
			sinkConn = this.sinkConnectionPool.getConnection();

			pstmtSource = sourceConn.prepareStatement(sql);
			pstmtSource.setLong(1, loadBean.startSeq);
			pstmtSource.setLong(2, loadBean.endSeq);
			rs = pstmtSource.executeQuery();

			sinkConn.setAutoCommit(false); 
			/*
			pstmt = sinkConn.prepareStatement(
					"insert into " + this.sinkTableContractProductLog 
						+ " (CHANGE_ID,LOG_TYPE,POLICY_CHG_ID,ITEM_ID,MASTER_ID,POLICY_ID,PRODUCT_ID,AMOUNT,UNIT,APPLY_DATE"
						+ ",EXPIRY_DATE,VALIDATE_DATE,PAIDUP_DATE,LIABILITY_STATE,END_CAUSE,INITIAL_TYPE,RENEWAL_TYPE,CHARGE_PERIOD,CHARGE_YEAR,COVERAGE_PERIOD,COVERAGE_YEAR,SHORT_END_TIME" 
						+ ",EXCEPT_VALUE,BENEFIT_LEVEL,INSURED_CATEGORY,SUSPEND,SUSPEND_CAUSE,DERIVATION,PAY_MODE,EXPIRY_CASH_VALUE
						,DECISION_ID,COUNT_WAY" 
						+ ",RENEW_DECISION,BONUS_SA,PAY_NEXT==,ANNI_BALANCE,FIX_INCREMENT,CPF_COST,CASH_COST,ORIGIN_SA,ORIGIN_BONUS_SA,==RISK_CODE"
						+ ",EXPOSURE_RATE,REINS_RATE,SUSPEND_CHG_ID,NEXT_AMOUNT,WAIVER_START,WAIVER_END,AUTO_PERMNT_LAPSE,PERMNT_LAPSE_NOTICE_DATE,WAIVER,WAIVED_SA" 
						+ ",ISSUE_AGENT,MASTER_PRODUCT,STRATEGY_CODE,LOAN_TYPE,BEN_PERIOD_TYPE,BENEFIT_PERIOD,GURNT_START_DATE,GURNT_PERD_TYPE,GURNT_PERIOD,INVEST_HORIZON"
						+ ",MANUAL_SA,DEFER_PERIOD,WAIT_PERIOD,NEXT_DISCNTED_PREM_AF,NEXT_POLICY_FEE_AF,NEXT_GROSS_PREM_AF,NEXT_EXTRA_PREM_AF,NEXT_TOTAL_PREM_AF,NEXT_STD_PREM_AN,NEXT_DISCNT_PREM_AN" + ",NEXT_DISCNTED_PREM_AN,NEXT_POLICY_FEE_AN,NEXT_EXTRA_PREM_AN,WAIV_ANUL_BENEFIT,WAIV_ANUL_PREM,LAPSE_CAUSE,PREM_CHANGE_TIME,SUBMISSION_DATE,NEXT_DISCNTED_PREM_BF,NEXT_POLICY_FEE_BF" + ",NEXT_EXTRA_PREM_BF,SA_FACTOR,NEXT_STD_PREM_AF,NEXT_DISCNT_PREM_AF,STD_PREM_BF,DISCNT_PREM_BF,DISCNTED_PREM_BF,POLICY_FEE_BF,EXTRA_PREM_BF,STD_PREM_AF"
						+ ",DISCNT_PREM_AF,POLICY_FEE_AF,GROSS_PREM_AF,EXTRA_PREM_AF,TOTAL_PREM_AF,STD_PREM_AN,DISCNT_PREM_AN,DISCNTED_PREM_AN,POLICY_FEE_AN,EXTRA_PREM_AN"
						+ ",NEXT_STD_PREM_BF,NEXT_DISCNT_PREM_BF,DISCNTED_PREM_AF,POLICY_PREM_SOURCE,ACTUAL_VALIDATE,ENTITY_FUND,CAR_REG_NO,MANU_SURR_VALUE,LOG_ID,ILP_CALC_SA" 
						+ ",P_LAPSE_DATE,INITIAL_VALI_DATE,AGE_INCREASE_INDI,PAIDUP_OPTION,NEXT_COUNT_WAY" 
						+ ",NEXT_UNIT,INSUR_PREM_AF,INSUR_PREM_AN,NEXT_INSUR_PREM_AF,NEXT_INSUR_PREM_AN" 
						+ ",CAR_REG_NO_START,PHD_PERIOD,ORIGIN_PRODUCT_ID,LAST_STATEMENT_DATE,PRE_WAR_INDI" 
						+ ",WAIVER_CLAIM_TYPE,ISSUE_DATE,ADVANTAGE_INDI,RISK_COMMENCE_DATE,LAST_CMT_FLG" 
						+ ",EMS_VERSION,INSERTED_BY,UPDATED_BY,INSERT_TIME,UPDATE_TIME" 
						+ ",INSERT_TIMESTAMP,UPDATE_TIMESTAMP,LIABILITY_STATE_CAUSE,LIABILITY_STATE_DATE,INVEST_SCHEME" 
						+ ",TARIFF_TYPE,INDX_SUSPEND_CAUSE,INDX_TYPE,INDX_CALC_BASIS,INDX_INDI" 
						+ ",COOLING_OFF_OPTION,POSTPONE_PERIOD,NEXT_BENEFIT_LEVEL,PRODUCT_VERSION_ID,RENEW_INDI" 
						+ ",AGENCY_ORGAN_ID,RELATION_ORGAN_TO_PH,PH_ROLE,NHI_INSUR_INDI,VERSION_TYPE_ID" 
						+ ",AGREE_READ_INDI,ITEM_ORDER,CUSTOMIZED_PREM,SAME_STD_PREM_INDI,PREMIUM_ADJUST_YEARLY" 
						+ ",PREMIUM_ADJUST_HALFYEARLY,PREMIUM_ADJUST_QUARTERLY,PREMIUM_ADJUST_MONTHLY,PROPOSAL_TERM,COMMISSION_VERSION" 
						+ ",INITIAL_SA,OLD_PRODUCT_CODE,COMPAIGN_CODE,INITIAL_CHARGE_MODE,CLAIM_STATUS" 
						+ ",INITIAL_TOTAL_PREM,OVERWITHDRAW_INDI,PRODUCT_TYPE,INSURABILITY_INDICATOR,ETI_SUR" 
						+ ",ETI_DAYS,PREMIUM_IS_ZERO_INDI,ILP_INCREASE_RATE,SPECIAL_EFFECT_DATE,NSP_AMOUNT" 
						+ ",PROPOASL_TERM_TYPE,TOTAL_PAID_PREM,ETI_YEARS,LOADING_RATE_POINTER,CANCER_DATE" 
						+ ",INSURABILITY_EXTRA_EXECUATE,LAST_CLAIM_DATE,GUARANTEE_OPTION,PREM_ALLOC_YEAR_INDI,NAR" 
						+ ",HIDE_VALIDATE_DATE_INDI,ETI_SUR_PB,ETA_PAIDUP_AMOUNT,ORI_PREMIUM_ADJUST_YEARLY,ORI_PREMIUM_ADJUST_HALFYEARLY" 
						+ ",ORI_PREMIUM_ADJUST_QUARTERLY,ORI_PREMIUM_ADJUST_MONTHLY,CLAIM_DISABILITY_RATE,HEALTH_INSURANCE_INDI,INSURANCE_NOTICE_INDI" 
						+ ",SERVICES_BENEFIT_LEVEL,PAYMENT_FREQ,ISSUE_TYPE,LOADING_RATE_POINTER_REASON,RELATION_TO_PH_ROLE" 
						+ ",MONEY_DENOMINATED,ORIGIN_PRODUCT_VERSION_ID,TR,HEALTH_INSURANCE_VERSION) " 
							+ " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
								+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
								+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
								+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
								+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
								+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
								+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
								+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
								+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
								+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
								+ ",?,?,?,?,?,?,?,?,?,?,?)");
			 */

			pstmt = sinkConn.prepareStatement(
					"insert into " + this.sinkTableContractProductLog 
					+ " (CHANGE_ID,LOG_TYPE,POLICY_CHG_ID,ITEM_ID,MASTER_ID"
					+ ",POLICY_ID,PRODUCT_ID,AMOUNT,UNIT,APPLY_DATE"
					+ ",EXPIRY_DATE,VALIDATE_DATE,PAIDUP_DATE,LIABILITY_STATE,END_CAUSE"
					+ ",INITIAL_TYPE,RENEWAL_TYPE,CHARGE_PERIOD,CHARGE_YEAR,COVERAGE_PERIOD"
					+ ",COVERAGE_YEAR,SHORT_END_TIME,EXCEPT_VALUE,BENEFIT_LEVEL,INSURED_CATEGORY"
					+ ",SUSPEND,SUSPEND_CAUSE,DERIVATION,PAY_MODE,EXPIRY_CASH_VALUE"
					+ ",DECISION_ID,COUNT_WAY,RENEW_DECISION,BONUS_SA,PAY_NEXT" 
					+ ",ANNI_BALANCE,FIX_INCREMENT,CPF_COST,CASH_COST,ORIGIN_SA"
					+ ",ORIGIN_BONUS_SA,RISK_CODE,EXPOSURE_RATE,REINS_RATE,SUSPEND_CHG_ID"
					+ ",NEXT_AMOUNT,WAIVER_START,WAIVER_END,AUTO_PERMNT_LAPSE,PERMNT_LAPSE_NOTICE_DATE"
					+ ",WAIVER,WAIVED_SA,ISSUE_AGENT,MASTER_PRODUCT,STRATEGY_CODE"
					+ ",LOAN_TYPE,BEN_PERIOD_TYPE,BENEFIT_PERIOD,GURNT_START_DATE,GURNT_PERD_TYPE"
					+ ",GURNT_PERIOD,INVEST_HORIZON,MANUAL_SA,DEFER_PERIOD,WAIT_PERIOD"
					+ ",NEXT_DISCNTED_PREM_AF,NEXT_POLICY_FEE_AF,NEXT_GROSS_PREM_AF,NEXT_EXTRA_PREM_AF,NEXT_TOTAL_PREM_AF"
					+ ",NEXT_STD_PREM_AN,NEXT_DISCNT_PREM_AN,NEXT_DISCNTED_PREM_AN,NEXT_POLICY_FEE_AN,NEXT_EXTRA_PREM_AN"
					+ ",WAIV_ANUL_BENEFIT,WAIV_ANUL_PREM,LAPSE_CAUSE,PREM_CHANGE_TIME,SUBMISSION_DATE"
					+ ",NEXT_DISCNTED_PREM_BF,NEXT_POLICY_FEE_BF,NEXT_EXTRA_PREM_BF,SA_FACTOR,NEXT_STD_PREM_AF"
					+ ",NEXT_DISCNT_PREM_AF,STD_PREM_BF,DISCNT_PREM_BF,DISCNTED_PREM_BF,POLICY_FEE_BF"
					+ ",EXTRA_PREM_BF,STD_PREM_AF,DISCNT_PREM_AF,POLICY_FEE_AF,GROSS_PREM_AF"
					+ ",EXTRA_PREM_AF,TOTAL_PREM_AF,STD_PREM_AN,DISCNT_PREM_AN,DISCNTED_PREM_AN"
					+ ",POLICY_FEE_AN,EXTRA_PREM_AN,NEXT_STD_PREM_BF,NEXT_DISCNT_PREM_BF,DISCNTED_PREM_AF"
					+ ",POLICY_PREM_SOURCE,ACTUAL_VALIDATE,ENTITY_FUND,CAR_REG_NO,MANU_SURR_VALUE"
					+ ",LOG_ID,ILP_CALC_SA,P_LAPSE_DATE,INITIAL_VALI_DATE,AGE_INCREASE_INDI"
					+ ",PAIDUP_OPTION,NEXT_COUNT_WAY,NEXT_UNIT,INSUR_PREM_AF,INSUR_PREM_AN"
					+ ")"
					+ " values (?,?,?,?,?,?,?,?,?,?"
					+ " ,?,?,?,?,?,?,?,?,?,?"
					+ " ,?,?,?,?,?,?,?,?,?,?"
					+ " ,?,?,?,?,?,?,?,?,?,?"
					+ " ,?,?,?,?,?,?,?,?,?,?"
					+ " ,?,?,?,?,?,?,?,?,?,?"
					+ " ,?,?,?,?,?,?,?,?,?,?"
					+ " ,?,?,?,?,?,?,?,?,?,?"
					+ " ,?,?,?,?,?,?,?,?,?,?"
					+ " ,?,?,?,?,?,?,?,?,?,?"
					+ " ,?,?,?,?,?,?,?,?,?,?"
					+ " ,?,?,?,?,?,?,?,?,?,?"
					+ ")");

			Long count = 0L;
			while (rs.next()) {
				count++;
				pstmt.setLong(1, rs.getLong("CHANGE_ID"));
				pstmt.setString(2, rs.getString("LOG_TYPE"));
				pstmt.setLong(3, rs.getLong("POLICY_CHG_ID"));
				pstmt.setLong(4, rs.getLong("ITEM_ID"));
				pstmt.setLong(5, rs.getLong("MASTER_ID"));
				pstmt.setLong(6, rs.getLong("POLICY_ID"));
				pstmt.setLong(7, rs.getLong("PRODUCT_ID"));
				pstmt.setBigDecimal(8, rs.getBigDecimal("AMOUNT"));
				pstmt.setBigDecimal(9, rs.getBigDecimal("UNIT"));
				pstmt.setTimestamp(10, ((rs.getDate("APPLY_DATE") == null)? null : new Timestamp(rs.getDate("APPLY_DATE").getTime())));
				pstmt.setTimestamp(11, ((rs.getDate("EXPIRY_DATE") == null)? null : new Timestamp(rs.getDate("EXPIRY_DATE").getTime())));
				pstmt.setTimestamp(12, ((rs.getDate("VALIDATE_DATE") == null)? null : new Timestamp(rs.getDate("VALIDATE_DATE").getTime())));
				pstmt.setTimestamp(13, ((rs.getDate("PAIDUP_DATE") == null)? null : new Timestamp(rs.getDate("PAIDUP_DATE").getTime())));
				pstmt.setInt(14, rs.getInt("LIABILITY_STATE"));
				pstmt.setInt(15, rs.getInt("END_CAUSE"));
				pstmt.setString(16, rs.getString("INITIAL_TYPE"));
				pstmt.setString(17, rs.getString("RENEWAL_TYPE"));
				pstmt.setString(18, rs.getString("CHARGE_PERIOD"));
				pstmt.setInt(19, rs.getInt("CHARGE_YEAR"));
				pstmt.setString(20, rs.getString("COVERAGE_PERIOD"));
				pstmt.setInt(21, rs.getInt("COVERAGE_YEAR"));
				pstmt.setTimestamp(22, ((rs.getDate("SHORT_END_TIME") == null)? null : new Timestamp(rs.getDate("SHORT_END_TIME").getTime())));
				pstmt.setLong(23, rs.getLong("EXCEPT_VALUE"));
				pstmt.setString(24, rs.getString("BENEFIT_LEVEL"));
				pstmt.setString(25, rs.getString("INSURED_CATEGORY"));
				pstmt.setString(26, rs.getString("SUSPEND"));
				pstmt.setInt(27, rs.getInt("SUSPEND_CAUSE"));
				pstmt.setString(28, rs.getString("DERIVATION"));
				pstmt.setInt(29, rs.getInt("PAY_MODE"));
				pstmt.setBigDecimal(30, rs.getBigDecimal("EXPIRY_CASH_VALUE"));
				pstmt.setInt(31, rs.getInt("DECISION_ID"));
				pstmt.setString(32, rs.getString("COUNT_WAY"));
				pstmt.setString(33, rs.getString("RENEW_DECISION"));
				pstmt.setBigDecimal(34, rs.getBigDecimal("BONUS_SA"));
				pstmt.setInt(35, rs.getInt("PAY_NEXT"));
				pstmt.setBigDecimal(36, rs.getBigDecimal("ANNI_BALANCE"));
				pstmt.setString(37, rs.getString("FIX_INCREMENT"));
				pstmt.setBigDecimal(38, rs.getBigDecimal("CPF_COST"));
				pstmt.setBigDecimal(39, rs.getBigDecimal("CASH_COST"));
				pstmt.setBigDecimal(40, rs.getBigDecimal("ORIGIN_SA"));
				pstmt.setBigDecimal(41, rs.getBigDecimal("ORIGIN_BONUS_SA"));
				pstmt.setBigDecimal(42, rs.getBigDecimal("RISK_CODE"));
				pstmt.setBigDecimal(43, rs.getBigDecimal("EXPOSURE_RATE"));
				pstmt.setBigDecimal(44, rs.getBigDecimal("REINS_RATE"));
				pstmt.setBigDecimal(45, rs.getBigDecimal("SUSPEND_CHG_ID"));
				pstmt.setBigDecimal(46, rs.getBigDecimal("NEXT_AMOUNT"));
				pstmt.setTimestamp(47, ((rs.getDate("WAIVER_START") == null)? null : new Timestamp(rs.getDate("WAIVER_START").getTime())));
				pstmt.setTimestamp(48, ((rs.getDate("WAIVER_END") == null)? null : new Timestamp(rs.getDate("WAIVER_END").getTime())));
				pstmt.setString(49, rs.getString("AUTO_PERMNT_LAPSE"));
				pstmt.setTimestamp(50, ((rs.getDate("PERMNT_LAPSE_NOTICE_DATE") == null)? null : new Timestamp(rs.getDate("PERMNT_LAPSE_NOTICE_DATE").getTime())));
				pstmt.setString(51, rs.getString("WAIVER"));
				pstmt.setBigDecimal(52, rs.getBigDecimal("WAIVED_SA"));
				pstmt.setBigDecimal(53, rs.getBigDecimal("ISSUE_AGENT"));
				pstmt.setBigDecimal(54, rs.getBigDecimal("MASTER_PRODUCT"));
				pstmt.setString(55, rs.getString("STRATEGY_CODE"));
				pstmt.setString(56, rs.getString("LOAN_TYPE"));
				pstmt.setString(57, rs.getString("BEN_PERIOD_TYPE"));
				pstmt.setInt(58, rs.getInt("BENEFIT_PERIOD"));
				pstmt.setTimestamp(59, ((rs.getDate("GURNT_START_DATE") == null)? null : new Timestamp(rs.getDate("GURNT_START_DATE").getTime())));
				pstmt.setString(60, rs.getString("GURNT_PERD_TYPE"));
				pstmt.setBigDecimal(61, rs.getBigDecimal("GURNT_PERIOD"));
				pstmt.setBigDecimal(62, rs.getBigDecimal("INVEST_HORIZON"));
				pstmt.setString(63, rs.getString("MANUAL_SA"));
				pstmt.setBigDecimal(64, rs.getBigDecimal("DEFER_PERIOD"));
				pstmt.setBigDecimal(65, rs.getBigDecimal("WAIT_PERIOD"));
				pstmt.setBigDecimal(66, rs.getBigDecimal("NEXT_DISCNTED_PREM_AF"));
				pstmt.setBigDecimal(67, rs.getBigDecimal("NEXT_POLICY_FEE_AF"));
				pstmt.setBigDecimal(68, rs.getBigDecimal("NEXT_GROSS_PREM_AF"));
				pstmt.setBigDecimal(69, rs.getBigDecimal("NEXT_EXTRA_PREM_AF"));
				pstmt.setBigDecimal(70, rs.getBigDecimal("NEXT_TOTAL_PREM_AF"));
				pstmt.setBigDecimal(71, rs.getBigDecimal("NEXT_STD_PREM_AN"));
				pstmt.setBigDecimal(72, rs.getBigDecimal("NEXT_DISCNT_PREM_AN"));
				pstmt.setBigDecimal(73, rs.getBigDecimal("NEXT_DISCNTED_PREM_AN"));
				pstmt.setBigDecimal(74, rs.getBigDecimal("NEXT_POLICY_FEE_AN"));
				pstmt.setBigDecimal(75, rs.getBigDecimal("NEXT_EXTRA_PREM_AN"));
				pstmt.setBigDecimal(76, rs.getBigDecimal("WAIV_ANUL_BENEFIT"));
				pstmt.setBigDecimal(77, rs.getBigDecimal("WAIV_ANUL_PREM"));
				pstmt.setBigDecimal(78, rs.getBigDecimal("LAPSE_CAUSE"));
				pstmt.setTimestamp(79, ((rs.getDate("PREM_CHANGE_TIME") == null)? null : new Timestamp(rs.getDate("PREM_CHANGE_TIME").getTime())));
				pstmt.setTimestamp(80, ((rs.getDate("SUBMISSION_DATE") == null)? null : new Timestamp(rs.getDate("SUBMISSION_DATE").getTime())));
				pstmt.setBigDecimal(81, rs.getBigDecimal("NEXT_DISCNTED_PREM_BF"));
				pstmt.setBigDecimal(82, rs.getBigDecimal("NEXT_POLICY_FEE_BF"));
				pstmt.setBigDecimal(83, rs.getBigDecimal("NEXT_EXTRA_PREM_BF"));
				pstmt.setBigDecimal(84, rs.getBigDecimal("SA_FACTOR"));
				pstmt.setBigDecimal(85, rs.getBigDecimal("NEXT_STD_PREM_AF"));
				pstmt.setBigDecimal(86, rs.getBigDecimal("NEXT_DISCNT_PREM_AF"));
				pstmt.setBigDecimal(87, rs.getBigDecimal("STD_PREM_BF"));
				pstmt.setBigDecimal(88, rs.getBigDecimal("DISCNT_PREM_BF"));
				pstmt.setBigDecimal(89, rs.getBigDecimal("DISCNTED_PREM_BF"));
				pstmt.setBigDecimal(90, rs.getBigDecimal("POLICY_FEE_BF"));
				pstmt.setBigDecimal(91, rs.getBigDecimal("EXTRA_PREM_BF"));
				pstmt.setBigDecimal(92, rs.getBigDecimal("STD_PREM_AF"));
				pstmt.setBigDecimal(93, rs.getBigDecimal("DISCNT_PREM_AF"));
				pstmt.setBigDecimal(94, rs.getBigDecimal("POLICY_FEE_AF"));
				pstmt.setBigDecimal(95, rs.getBigDecimal("GROSS_PREM_AF"));
				pstmt.setBigDecimal(96, rs.getBigDecimal("EXTRA_PREM_AF"));
				pstmt.setBigDecimal(97, rs.getBigDecimal("TOTAL_PREM_AF"));
				pstmt.setBigDecimal(98, rs.getBigDecimal("STD_PREM_AN"));
				pstmt.setBigDecimal(99, rs.getBigDecimal("DISCNT_PREM_AN"));
				pstmt.setBigDecimal(100, rs.getBigDecimal("POLICY_FEE_AN"));
				pstmt.setBigDecimal(101, rs.getBigDecimal("DISCNTED_PREM_AN"));
				pstmt.setBigDecimal(102, rs.getBigDecimal("EXTRA_PREM_AN"));
				pstmt.setBigDecimal(103, rs.getBigDecimal("NEXT_STD_PREM_BF"));
				pstmt.setBigDecimal(104, rs.getBigDecimal("NEXT_DISCNT_PREM_BF"));
				pstmt.setBigDecimal(105, rs.getBigDecimal("DISCNTED_PREM_AF"));
				pstmt.setString(106, rs.getString("POLICY_PREM_SOURCE"));
				pstmt.setTimestamp(107, ((rs.getDate("ACTUAL_VALIDATE") == null)? null : new Timestamp(rs.getDate("ACTUAL_VALIDATE").getTime())));
				pstmt.setString(108, rs.getString("ENTITY_FUND"));
				pstmt.setString(109, rs.getString("CAR_REG_NO"));
				pstmt.setString(110, rs.getString("MANU_SURR_VALUE"));
				pstmt.setLong(111, rs.getLong("LOG_ID"));
				pstmt.setBigDecimal(112, rs.getBigDecimal("ILP_CALC_SA"));
				pstmt.setTimestamp(113, ((rs.getDate("P_LAPSE_DATE") == null)? null : new Timestamp(rs.getDate("P_LAPSE_DATE").getTime())));
				pstmt.setTimestamp(114, ((rs.getDate("INITIAL_VALI_DATE") == null)? null : new Timestamp(rs.getDate("INITIAL_VALI_DATE").getTime())));
				pstmt.setString(115, rs.getString("AGE_INCREASE_INDI"));
				pstmt.setString(116, rs.getString("PAIDUP_OPTION"));
				pstmt.setString(117, rs.getString("NEXT_COUNT_WAY"));
				pstmt.setBigDecimal(118, rs.getBigDecimal("NEXT_UNIT"));
				pstmt.setBigDecimal(119, rs.getBigDecimal("INSUR_PREM_AF"));
				pstmt.setBigDecimal(120, rs.getBigDecimal("INSUR_PREM_AN"));
				
				pstmt.addBatch();

				if (count % 3000 == 0) {
					pstmt.executeBatch();//executing the batch  
					sinkConn.commit(); 
					pstmt.clearBatch();
				}
			}
			//	if (startSeq % 50000000 == 0) {
			//				
			cnsl = System.console();
			//				logger.info("   >>>roletype={}, startSeq={}, count={}, span={} ", roleType, startSeq, count, (System.currentTimeMillis() - t0));
			cnsl.printf("   >>>table=%s, startSeq=%d, endSeq=%d, count=%d \n", loadBean.tableName, loadBean.startSeq, loadBean.endSeq, count);

			//				cnsl.printf("   >>>roletype=" + roleType + ", startSeq=" + startSeq + ", count=" + count +", span=" + ",span=" + (System.currentTimeMillis() - t0));
			cnsl.flush();
			//		}

			pstmt.executeBatch();
			if (count > 0) sinkConn.commit(); 

			logger.info(">>>>> loadContractProductLog count={}, sql={}, startSeq={}, endSeq={}", count, sql, loadBean.startSeq, loadBean.endSeq);
		}  catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			map.put("RETURN_CODE", "-999");
			map.put("SQL", sql);
			map.put("SINK_TABLE", this.sinkTableProductionDetail);
			map.put("ERROR_MSG", e.getMessage());
			map.put("STACK_TRACE", ExceptionUtils.getStackTrace(e));
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (pstmtSource != null) {
				try {
					pstmtSource.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (sourceConn != null) {
				try {
					sourceConn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (sinkConn != null) {
				try {
					sinkConn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return map;
	}


	//	private Map<String, String> loadData(String sql, LoadBean loadBean){
	//		//		logger.info(">>> run loadInterestedPartyContact, table={}, roleType={}", sourceTableName, roleType);
	//
	//		Console cnsl = null;
	//		Map<String, String> map = new HashMap<>();
	//		Connection sourceConn = null;
	//		Connection sinkConn = null;
	//		PreparedStatement pstmt = null;
	//		ResultSet rs = null;
	//		try {
	//
	//			sourceConn = this.sourceConnectionPool.getConnection();
	//			sinkConn = this.sinkConnectionPool.getConnection();
	//
	//			Statement stmt = sourceConn.createStatement();
	//			ResultSet resultSet = stmt.executeQuery(sql);
	//
	//			sinkConn.setAutoCommit(false); 
	//			pstmt = sinkConn.prepareStatement(
	//					"insert into " + this.sinkTableProductionDetail + " (ROLE_TYPE,LIST_ID,POLICY_ID,NAME,CERTI_CODE,MOBILE_TEL,EMAIL,ADDRESS_ID,ADDRESS_1) " 
	//							+ " values (?,?,?,?,?,?,?,?,?)");
	//
	//			Long count = 0L;
	//			while (resultSet.next()) {
	//				count++;
	//
	//				Long listId = resultSet.getLong("LIST_ID");
	//				Long policyId = resultSet.getLong("POLICY_ID");
	//				String name = resultSet.getString("NAME");
	//				String certiCode = resultSet.getString("CERTI_CODE");
	//				String mobileTel = resultSet.getString("MOBILE_TEL");
	//				//因BSD規則調整,受益人的email部份,畫面並沒有輸入t_contract_bene.email雖有值但不做比對
	//				String email = (loadBean.roleType == 3)? null : resultSet.getString("EMAIL");
	//				Long addressId = resultSet.getLong("ADDRESS_ID");
	//				String address1 = resultSet.getString("ADDRESS_1");
	//
	//				if (listId.longValue() == 2437872) {
	//					logger.error("listid = 2437872, sql={}", sql); 
	//				}
	//
	//
	//				pstmt.setInt(1, loadBean.roleType);
	//				pstmt.setLong(2, listId);
	//				pstmt.setLong(3, policyId);
	//
	//				if (name == null) {
	//					pstmt.setNull(4, Types.VARCHAR);
	//				} else {
	//					pstmt.setString(4, name);
	//				}
	//				if (certiCode== null) {
	//					pstmt.setNull(5, Types.VARCHAR);
	//				} else {
	//					pstmt.setString(5, certiCode);
	//				}
	//				if (mobileTel == null) {
	//					pstmt.setNull(6, Types.VARCHAR);
	//				} else {
	//					pstmt.setString(6, StringUtils.trim(mobileTel));
	//				}
	//				if (email == null) {
	//					pstmt.setNull(7, Types.VARCHAR);
	//				} else {
	//					pstmt.setString(7, StringUtils.trim(email.toLowerCase()));
	//				}
	//
	//				pstmt.setLong(8, addressId);
	//
	//				if (address1 == null) {
	//					pstmt.setNull(9, Types.VARCHAR);
	//				} else {
	//					pstmt.setString(9, StringUtils.trim(address1));
	//				}
	//
	//				pstmt.addBatch();
	//
	//				if (count % 3000 == 0) {
	//					pstmt.executeBatch();//executing the batch  
	//					sinkConn.commit(); 
	//					pstmt.clearBatch();
	//				}
	//			}
	//			//	if (startSeq % 50000000 == 0) {
	//			//				
	//			cnsl = System.console();
	//			//				logger.info("   >>>roletype={}, startSeq={}, count={}, span={} ", roleType, startSeq, count, (System.currentTimeMillis() - t0));
	//			cnsl.printf("   >>>roletype=%d, startSeq=%d, endSeq=%d, count=%d \n", loadBean.roleType, loadBean.startSeq, loadBean.endSeq, count);
	//
	//			//				cnsl.printf("   >>>roletype=" + roleType + ", startSeq=" + startSeq + ", count=" + count +", span=" + ",span=" + (System.currentTimeMillis() - t0));
	//			cnsl.flush();
	//			//		}
	//
	//			pstmt.executeBatch();
	//			if (pstmt != null) pstmt.close();
	//			if (count > 0) sinkConn.commit(); 
	//
	//			resultSet.close();
	//			stmt.close();
	//
	//			sourceConn.close();
	//			sinkConn.close();
	//
	//			//			map.put("RETURN_CODE", "0");
	//			//			map.put("SQL", sourceTableName);
	//			//			map.put("SINK_TABLE", config.sinkTablePartyContact);
	//			//			map.put("RECORD_COUNT", String.valueOf(count));
	//
	//		}  catch (Exception e) {
	//			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
	//			map.put("RETURN_CODE", "-999");
	//			map.put("SQL", sql);
	//			map.put("SINK_TABLE", this.sinkTableProductionDetail);
	//			map.put("ERROR_MSG", e.getMessage());
	//			map.put("STACK_TRACE", ExceptionUtils.getStackTrace(e));
	//		} finally {
	//			if (rs != null) {
	//				try {
	//					rs.close();
	//				} catch (SQLException e) {
	//					// TODO Auto-generated catch block
	//					e.printStackTrace();
	//				}
	//			}
	//			if (pstmt != null) {
	//				try {
	//					pstmt.close();
	//				} catch (SQLException e) {
	//					// TODO Auto-generated catch block
	//					e.printStackTrace();
	//				}
	//			}
	//			if (sourceConn != null) {
	//				try {
	//					sourceConn.close();
	//				} catch (SQLException e) {
	//					// TODO Auto-generated catch block
	//					e.printStackTrace();
	//				}
	//			}
	//			if (sinkConn != null) {
	//				try {
	//					sinkConn.close();
	//				} catch (SQLException e) {
	//					// TODO Auto-generated catch block
	//					e.printStackTrace();
	//				}
	//			}
	//			
	//		}
	//		return map;
	//	}
	private void run() throws Exception {

		ExecutorService executor = Executors.newFixedThreadPool(THREADS);

		String sql = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {

			Connection sourceConn = this.sourceConnectionPool.getConnection();
			Map<String, String> tablemap = new HashMap<>();
			List<Map<String, String>> tablemapList = new ArrayList<>();
			tablemap.put("TABLE_NAME", this.sourceTableContractProductLog);
			tablemap.put("SELECT_ID_SQL", "select min(LOG_ID) as MIN_ID, max(LOG_ID) as MAX_ID from " + this.sourceTableContractProductLog);
			tablemap.put("SELECT_SQL", "select a.* from " + this.sourceTableContractProductLog + " a where ? <= a.log_id and a.log_id < ?");

			tablemapList.add(tablemap);

			String table = null;
			for (Map<String, String> map : tablemapList) {
				table = map.get("TABLE_NAME");
				sql = map.get("SELECT_ID_SQL");
				String selectSql = map.get("SELECT_SQL");

				//				sql = "select min(list_id) as MIN_LIST_ID, max(list_id) as MAX_LIST_ID from " 
				//				+ table + " where list_id >= 31000000";
				pstmt = sourceConn.prepareStatement(sql);
				rs = pstmt.executeQuery();
				long maxId = 0;
				long minId = 0;
				while (rs.next()) {
					minId = rs.getLong("MIN_ID");
					maxId = rs.getLong("MAX_ID");
				}
				rs.close();
				pstmt.close();

				long stepSize = 10000;
				long startIndex = minId;

				List<LoadBean> loadBeanList = new ArrayList<>();
				while (startIndex <= maxId) {
					long endIndex = startIndex + stepSize;


					int j = 0;
					long  subStepSize = stepSize;
					LoadBean loadBean = new LoadBean();
					loadBean.tableName = table;
					loadBean.startSeq = startIndex + j * subStepSize;
					loadBean.endSeq = startIndex + (j + 1) * subStepSize;
					loadBean.selectSql = selectSql;

					loadBeanList.add(loadBean);
					startIndex = endIndex;
				}

				logger.info("table={}, maxId={}, minId={}, size={}", table, maxId, minId, loadBeanList.size());

				List<CompletableFuture<Map<String, String>>> futures = 
						loadBeanList.stream().map(t -> CompletableFuture.supplyAsync(
								() -> {
									//									String sqlStr = "select a.* from " 
									//											+ t.tableName 
									//											+ " a where " + t.startSeq + " <= a.list_id and a.list_id < " + t.endSeq ;
									return loadData(t);
								}
								, executor)
								)
						.collect(Collectors.toList());			

				List<Map<String, String>> result = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());

			}

		} finally {
			if (executor != null) executor.shutdown();

		}
	}
	//	private void runTLog() throws Exception {
	//
	//		ExecutorService executor = Executors.newFixedThreadPool(THREADS);
	//
	//		String sql = null;
	//		PreparedStatement pstmt = null;
	//		ResultSet rs = null;
	//		try {
	//
	//			Connection sourceConn = this.sourceConnectionPool.getConnection();
	//
	//			String table = null;
	//			Integer roleType = null;
	//			for (int i = 0; i < 3; i++) {
	//				if ( i == 0) {
	//					table = this.sourceTablePolicyHolderLog;
	//					roleType = 1;
	//				} else if ( i == 1) {
	//					table = this.sourceTableInsuredListLog;
	//					roleType = 2;
	//				} else if ( i == 2) {
	//					table = this.sourceTableContractBeneLog;
	//					roleType = 3;
	//				}
	//				sql = "select min(log_id) as MIN_LOG_ID, max(log_id) as MAX_LOG_ID from " + table;
	//				
	////				sql = "select min(list_id) as MIN_LIST_ID, max(list_id) as MAX_LIST_ID from " 
	////				+ table + " where list_id >= 31000000";
	//				pstmt = sourceConn.prepareStatement(sql);
	//				rs = pstmt.executeQuery();
	//				long maxLogId = 0;
	//				long minLogId = 0;
	//				while (rs.next()) {
	//					minLogId = rs.getLong("MIN_LOG_ID");
	//					maxLogId = rs.getLong("MAX_LOG_ID");
	//				}
	//				rs.close();
	//				pstmt.close();
	//
	//				long stepSize = 10000;
	//				long startIndex = minLogId;
	//
	//				int totalPartyCount = 0;
	//				List<LoadBean> loadBeanList = new ArrayList<>();
	//				while (startIndex <= maxLogId) {
	//					long endIndex = startIndex + stepSize;
	//
	//
	//					int j = 0;
	//					long  subStepSize = stepSize;
	//					LoadBean loadBean = new LoadBean();
	//					loadBean.tableName = table;
	//					loadBean.roleType = roleType;
	//					loadBean.startSeq = startIndex + j * subStepSize;
	//					loadBean.endSeq = startIndex + (j + 1) * subStepSize;
	//					loadBeanList.add(loadBean);
	//
	//					startIndex = endIndex;
	//				}
	//
	//				logger.info("table={}, maxlistid={}, minListId={}, size={}, total partyCount={}", table, maxLogId, minLogId, loadBeanList.size(), totalPartyCount);
	//
	//				List<CompletableFuture<Map<String, String>>> futures = 
	//						loadBeanList.stream().map(t -> CompletableFuture.supplyAsync(
	//								() -> {
	//										String sqlStr = "select a.LIST_ID,a.POLICY_ID,a.NAME,a.CERTI_CODE,a.MOBILE_TEL,a.EMAIL,a.ADDRESS_ID,c.ADDRESS_1 from " 
	//											+ t.tableName 
	//											+ " a left join " + this.sourceTableAddress + " c on a.address_id = c.address_id "
	//											+ " where a.LAST_CMT_FLG = 'Y' and " + t.startSeq + " <= a.log_id and a.log_id < " + t.endSeq ;
	//										return loadPartyContact(sqlStr, t);
	//									}
	//								, executor)
	//								)
	//						.collect(Collectors.toList());			
	//
	//				List<Map<String, String>> result = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());
	//
	//			}
	//
	//		} finally {
	//			if (executor != null) executor.shutdown();
	//
	//		}
	//	}
	private void dropTable(String tableName) throws SQLException {
		Connection conn = sinkConnectionPool.getConnection();

		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("DROP TABLE IF EXISTS " + tableName);
			stmt.close();
		} catch (java.sql.SQLException e) {
			logger.info(">>>  table:" + tableName + " does not exists!!!");
		} finally {
			if (conn != null) { 
				conn.close();
			}
		}
	}
	private void createTable(String createTableFile) throws Exception {
		Connection conn = sinkConnectionPool.getConnection();

		Statement stmt = null;
		ClassLoader loader = Thread.currentThread().getContextClassLoader();	
		try (InputStream inputStream = loader.getResourceAsStream(createTableFile)) {
			String createTableScript = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
			//	logger.info(">>>  createTableScript={}", createTableScript);
			stmt = conn.createStatement();
			stmt.executeUpdate(createTableScript);
		} catch (SQLException | IOException e) {
			if (stmt != null) stmt.close();
			throw e;
		}

		conn.close();

	}

	private Integer createIndex(String sql) {

		Connection sinkConn = null;
		Statement stmt = null;
		int ret = 0;
		try {
			sinkConn = this.sinkConnectionPool.getConnection();
			stmt = sinkConn.createStatement();
			ret = stmt.executeUpdate(sql);

		} catch(Exception e) {
			logger.error(">>>error={}", ExceptionUtils.getStackTrace(e));
		} finally {
			try {
				if (stmt != null) stmt.close();
				if (sinkConn != null) sinkConn.close();
			}	catch(Exception e) {
				logger.error(">>>error={}", ExceptionUtils.getStackTrace(e));
			} 
		}
		return ret;
	}

	//	private void runCreateIndexes() {
	//
	//		long t0 = System.currentTimeMillis();
	//		createIndex("CREATE INDEX IDX_PARTY_CONTACT_1 ON " + this.sinkTablePartyContact + " (MOBILE_TEL) INLINE_SIZE 10 PARALLEL 8");
	//		logger.info(">>>>> create index mobile span={}", (System.currentTimeMillis() - t0));
	//
	//		t0 = System.currentTimeMillis();
	//		createIndex("CREATE INDEX IDX_PARTY_CONTACT_2 ON " + this.sinkTablePartyContact + " (EMAIL)  INLINE_SIZE 20 PARALLEL 8");
	//		logger.info(">>>>> create index email span={}", (System.currentTimeMillis() - t0));
	//
	//		t0 = System.currentTimeMillis();
	//		createIndex("CREATE INDEX IDX_PARTY_CONTACT_3 ON " + this.sinkTablePartyContact + " (ADDRESS_1)  INLINE_SIZE 60 PARALLEL 8");
	//		logger.info(">>>>> create index address span={}", (System.currentTimeMillis() - t0));
	//
	//		t0 = System.currentTimeMillis();
	//		createIndex("CREATE INDEX IDX_STREAMING_ETL_HEALTH_1 ON " + this.sinkTableStreamingEtlHealth + " (CDC_TIME) PARALLEL 8");
	//		logger.info(">>>>> create index streamingetlhealth cdc_time span={}", (System.currentTimeMillis() - t0));
	//
	//		
	//		/*
	//		List<String> indexSqlList = new ArrayList<>();
	//		indexSqlList.add("CREATE INDEX IDX_PARTY_CONTACT_1 ON " + config.sinkTablePartyContact + " (MOBILE_TEL)");
	//		indexSqlList.add("CREATE INDEX IDX_PARTY_CONTACT_2 ON " + config.sinkTablePartyContact + " (EMAIL)");
	//		indexSqlList.add("CREATE INDEX IDX_PARTY_CONTACT_3 ON " + config.sinkTablePartyContact + " (ADDRESS_ID)");
	//		indexSqlList.add("CREATE INDEX IDX_PARTY_CONTACT_TEMP_1 ON " + config.sinkTablePartyContactTemp + " (ADDRESS_ID)");
	//		ExecutorService executor = Executors.newFixedThreadPool(THREADS);
	//
	//		List<CompletableFuture<Integer>> futures = 
	//				indexSqlList.stream().map(t -> CompletableFuture.supplyAsync(
	//						() -> createIndex(t), executor))
	//				.collect(Collectors.toList());			
	//
	//
	//		List<Integer> result = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());
	//		 */
	//	}
}
