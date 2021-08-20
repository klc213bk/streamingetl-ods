package com.transglobe.streamingetl.ods.load.bean;

import java.io.Console;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ContractProductLogBean {
	private static final Logger logger = LoggerFactory.getLogger(ContractProductLogBean.class);

	private static int BATCH_COMMIT_SIZE = 5000;
	
	public static Map<String, String> loadToSinkTable(LoadBean loadBean, 
			BasicDataSource sourceConnectionPool, BasicDataSource sinkConnectionPool){
		Console cnsl = null;
		Map<String, String> map = new HashMap<>();
		Connection sourceConn = null;
		Connection sinkConn = null;
		PreparedStatement pstmtSource = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = null;
		long t0 = System.currentTimeMillis();
		long t1 = 0L;
		long t2 = 0L;
		try {
			sql = "";
			sourceConn = sourceConnectionPool.getConnection();
			sinkConn = sinkConnectionPool.getConnection();

			pstmtSource = sourceConn.prepareStatement(sql);
			pstmtSource.setLong(1, loadBean.startSeq);
			pstmtSource.setLong(2, loadBean.endSeq);
			rs = pstmtSource.executeQuery();

			t1 = System.currentTimeMillis();
			
			sinkConn.setAutoCommit(false); 

			pstmt = sinkConn.prepareStatement(
					"insert into " + "SINK_TABLE_NAME_CONTRACT_PRODUCT_LOG "
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
					+ ",NEXT_INSUR_PREM_AF,NEXT_INSUR_PREM_AN,CAR_REG_NO_START,PHD_PERIOD,ORIGIN_PRODUCT_ID"
					+ ",LAST_STATEMENT_DATE,PRE_WAR_INDI,WAIVER_CLAIM_TYPE,ISSUE_DATE,ADVANTAGE_INDI"
					+ ",RISK_COMMENCE_DATE,LAST_CMT_FLG,EMS_VERSION,INSERTED_BY,UPDATED_BY"
					+ ",INSERT_TIME,UPDATE_TIME,INSERT_TIMESTAMP,UPDATE_TIMESTAMP,LIABILITY_STATE_CAUSE"
					+ ",LIABILITY_STATE_DATE,INVEST_SCHEME,TARIFF_TYPE,INDX_SUSPEND_CAUSE,INDX_TYPE"
					+ ",INDX_CALC_BASIS,INDX_INDI,COOLING_OFF_OPTION,POSTPONE_PERIOD,NEXT_BENEFIT_LEVEL"
					+ ",PRODUCT_VERSION_ID,RENEW_INDI,AGENCY_ORGAN_ID,RELATION_ORGAN_TO_PH,PH_ROLE"
					+ ",NHI_INSUR_INDI,VERSION_TYPE_ID,AGREE_READ_INDI,ITEM_ORDER,CUSTOMIZED_PREM"
					+ ",SAME_STD_PREM_INDI,PREMIUM_ADJUST_YEARLY,PREMIUM_ADJUST_HALFYEARLY,PREMIUM_ADJUST_QUARTERLY,PREMIUM_ADJUST_MONTHLY"
					+ ",PROPOSAL_TERM,COMMISSION_VERSION,INITIAL_SA,OLD_PRODUCT_CODE,COMPAIGN_CODE"
					+ ",INITIAL_CHARGE_MODE,CLAIM_STATUS,INITIAL_TOTAL_PREM,OVERWITHDRAW_INDI,PRODUCT_TYPE"
					+ ",INSURABILITY_INDICATOR,ETI_SUR,ETI_DAYS,PREMIUM_IS_ZERO_INDI,ILP_INCREASE_RATE"
					+ ",SPECIAL_EFFECT_DATE,NSP_AMOUNT,PROPOASL_TERM_TYPE,TOTAL_PAID_PREM,ETI_YEARS"
					+ ",LOADING_RATE_POINTER,CANCER_DATE,INSURABILITY_EXTRA_EXECUATE,LAST_CLAIM_DATE,GUARANTEE_OPTION"
					+ ",PREM_ALLOC_YEAR_INDI,NAR,HIDE_VALIDATE_DATE_INDI,ETI_SUR_PB,ETA_PAIDUP_AMOUNT"
					+ ",ORI_PREMIUM_ADJUST_YEARLY,ORI_PREMIUM_ADJUST_HALFYEARLY,ORI_PREMIUM_ADJUST_QUARTERLY,ORI_PREMIUM_ADJUST_MONTHLY,CLAIM_DISABILITY_RATE"
					+ ",HEALTH_INSURANCE_INDI,INSURANCE_NOTICE_INDI,SERVICES_BENEFIT_LEVEL,PAYMENT_FREQ,ISSUE_TYPE"
					+ ",LOADING_RATE_POINTER_REASON,RELATION_TO_PH_ROLE,MONEY_DENOMINATED,ORIGIN_PRODUCT_VERSION_ID,TRANSFORM_DATE"
					+ ",HEALTH_INSURANCE_VERSION"
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
					+ " ,?,?,?,?,?,?,?,?,?,?"
					+ " ,?,?,?,?,?,?,?,?,?,?"
					+ " ,?,?,?,?,?,?,?,?,?,?"
					+ " ,?,?,?,?,?,?,?,?,?,?"
					+ " ,?,?,?,?,?,?,?,?,?,?"
					+ " ,?,?,?,?,?,?,?,?,?,?"
					+ " ,?,?,?,?,?,?,?,?,?,?"
					+ " ,?,?,?,?,?,?,?,?,?,?"
					+ " ,?,?,?,?,?,?,?,?,?,?"
					+ " ,?"
					+ ")");

			Long count = 0L;
			while (rs.next()) {
				count++;
				pstmt.setBigDecimal(1, rs.getBigDecimal("CHANGE_ID"));
				pstmt.setString(2, rs.getString("LOG_TYPE"));
				pstmt.setBigDecimal(3, rs.getBigDecimal("POLICY_CHG_ID"));
				pstmt.setBigDecimal(4, rs.getBigDecimal("ITEM_ID"));
				pstmt.setBigDecimal(5, rs.getBigDecimal("MASTER_ID"));
				pstmt.setBigDecimal(6, rs.getBigDecimal("POLICY_ID"));
				pstmt.setBigDecimal(7, rs.getBigDecimal("PRODUCT_ID"));
				pstmt.setBigDecimal(8, rs.getBigDecimal("AMOUNT"));
				pstmt.setBigDecimal(9, rs.getBigDecimal("UNIT"));
				pstmt.setTimestamp(10, ((rs.getDate("APPLY_DATE") == null)? null : new Timestamp(rs.getDate("APPLY_DATE").getTime())));
				pstmt.setTimestamp(11, ((rs.getDate("EXPIRY_DATE") == null)? null : new Timestamp(rs.getDate("EXPIRY_DATE").getTime())));
				pstmt.setTimestamp(12, ((rs.getDate("VALIDATE_DATE") == null)? null : new Timestamp(rs.getDate("VALIDATE_DATE").getTime())));
				pstmt.setTimestamp(13, ((rs.getDate("PAIDUP_DATE") == null)? null : new Timestamp(rs.getDate("PAIDUP_DATE").getTime())));
				pstmt.setBigDecimal(14, rs.getBigDecimal("LIABILITY_STATE"));
				pstmt.setBigDecimal(15, rs.getBigDecimal("END_CAUSE"));
				pstmt.setString(16, rs.getString("INITIAL_TYPE"));
				pstmt.setString(17, rs.getString("RENEWAL_TYPE"));
				pstmt.setString(18, rs.getString("CHARGE_PERIOD"));
				pstmt.setBigDecimal(19, rs.getBigDecimal("CHARGE_YEAR"));
				pstmt.setString(20, rs.getString("COVERAGE_PERIOD"));
				pstmt.setBigDecimal(21, rs.getBigDecimal("COVERAGE_YEAR"));
				pstmt.setTimestamp(22, ((rs.getDate("SHORT_END_TIME") == null)? null : new Timestamp(rs.getDate("SHORT_END_TIME").getTime())));
				pstmt.setBigDecimal(23, rs.getBigDecimal("EXCEPT_VALUE"));
				pstmt.setString(24, rs.getString("BENEFIT_LEVEL"));
				pstmt.setString(25, rs.getString("INSURED_CATEGORY"));
				pstmt.setString(26, rs.getString("SUSPEND"));
				pstmt.setBigDecimal(27, rs.getBigDecimal("SUSPEND_CAUSE"));
				pstmt.setString(28, rs.getString("DERIVATION"));
				pstmt.setInt(29, rs.getInt("PAY_MODE"));
				pstmt.setBigDecimal(30, rs.getBigDecimal("EXPIRY_CASH_VALUE"));
				pstmt.setBigDecimal(31, rs.getBigDecimal("DECISION_ID"));
				pstmt.setString(32, rs.getString("COUNT_WAY"));
				pstmt.setString(33, rs.getString("RENEW_DECISION"));
				pstmt.setBigDecimal(34, rs.getBigDecimal("BONUS_SA"));
				pstmt.setBigDecimal(35, rs.getBigDecimal("PAY_NEXT"));
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
				pstmt.setBigDecimal(58, rs.getBigDecimal("BENEFIT_PERIOD"));
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
				pstmt.setBigDecimal(111, rs.getBigDecimal("LOG_ID"));
				pstmt.setBigDecimal(112, rs.getBigDecimal("ILP_CALC_SA"));
				pstmt.setTimestamp(113, ((rs.getDate("P_LAPSE_DATE") == null)? null : new Timestamp(rs.getDate("P_LAPSE_DATE").getTime())));
				pstmt.setTimestamp(114, ((rs.getDate("INITIAL_VALI_DATE") == null)? null : new Timestamp(rs.getDate("INITIAL_VALI_DATE").getTime())));
				pstmt.setString(115, rs.getString("AGE_INCREASE_INDI"));
				pstmt.setString(116, rs.getString("PAIDUP_OPTION"));
				pstmt.setString(117, rs.getString("NEXT_COUNT_WAY"));
				pstmt.setBigDecimal(118, rs.getBigDecimal("NEXT_UNIT"));
				pstmt.setBigDecimal(119, rs.getBigDecimal("INSUR_PREM_AF"));
				pstmt.setBigDecimal(120, rs.getBigDecimal("INSUR_PREM_AN"));
				pstmt.setBigDecimal(121, rs.getBigDecimal("NEXT_INSUR_PREM_AF"));
				pstmt.setBigDecimal(122, rs.getBigDecimal("NEXT_INSUR_PREM_AN"));
				pstmt.setTimestamp(123, ((rs.getDate("CAR_REG_NO_START") == null)? null : new Timestamp(rs.getDate("CAR_REG_NO_START").getTime())));
				pstmt.setBigDecimal(124, rs.getBigDecimal("PHD_PERIOD"));
				pstmt.setBigDecimal(125, rs.getBigDecimal("ORIGIN_PRODUCT_ID"));
				pstmt.setTimestamp(126, ((rs.getDate("LAST_STATEMENT_DATE") == null)? null : new Timestamp(rs.getDate("LAST_STATEMENT_DATE").getTime())));
				pstmt.setString(127, rs.getString("PRE_WAR_INDI"));
				pstmt.setBigDecimal(128, rs.getBigDecimal("WAIVER_CLAIM_TYPE"));
				pstmt.setTimestamp(129, ((rs.getDate("ISSUE_DATE") == null)? null : new Timestamp(rs.getDate("ISSUE_DATE").getTime())));
				pstmt.setString(130, rs.getString("ADVANTAGE_INDI"));
				pstmt.setTimestamp(131, ((rs.getDate("RISK_COMMENCE_DATE") == null)? null : new Timestamp(rs.getDate("RISK_COMMENCE_DATE").getTime())));
				pstmt.setString(132, rs.getString("LAST_CMT_FLG"));
				pstmt.setBigDecimal(133, rs.getBigDecimal("EMS_VERSION"));
				pstmt.setBigDecimal(134, rs.getBigDecimal("INSERTED_BY"));
				pstmt.setBigDecimal(135, rs.getBigDecimal("UPDATED_BY"));
				pstmt.setTimestamp(136, ((rs.getDate("INSERT_TIME") == null)? null : new Timestamp(rs.getDate("INSERT_TIME").getTime())));
				pstmt.setTimestamp(137, ((rs.getDate("UPDATE_TIME") == null)? null : new Timestamp(rs.getDate("UPDATE_TIME").getTime())));
				pstmt.setTimestamp(138, ((rs.getDate("INSERT_TIMESTAMP") == null)? null : new Timestamp(rs.getDate("INSERT_TIMESTAMP").getTime())));
				pstmt.setTimestamp(139, ((rs.getDate("UPDATE_TIMESTAMP") == null)? null : new Timestamp(rs.getDate("UPDATE_TIMESTAMP").getTime())));
				pstmt.setBigDecimal(140, rs.getBigDecimal("LIABILITY_STATE_CAUSE"));
				pstmt.setTimestamp(141, ((rs.getDate("LIABILITY_STATE_DATE") == null)? null : new Timestamp(rs.getDate("LIABILITY_STATE_DATE").getTime())));
				pstmt.setString(142, rs.getString("INVEST_SCHEME"));
				pstmt.setBigDecimal(143, rs.getBigDecimal("TARIFF_TYPE"));
				pstmt.setBigDecimal(144, rs.getBigDecimal("INDX_SUSPEND_CAUSE"));
				pstmt.setBigDecimal(145, rs.getBigDecimal("INDX_TYPE"));
				pstmt.setBigDecimal(146, rs.getBigDecimal("INDX_CALC_BASIS"));
				pstmt.setString(147, rs.getString("INDX_INDI"));
				pstmt.setBigDecimal(148, rs.getBigDecimal("COOLING_OFF_OPTION"));
				pstmt.setBigDecimal(149, rs.getBigDecimal("POSTPONE_PERIOD"));
				pstmt.setString(150, rs.getString("NEXT_BENEFIT_LEVEL"));
				pstmt.setBigDecimal(151, rs.getBigDecimal("PRODUCT_VERSION_ID"));
				pstmt.setString(152, rs.getString("RENEW_INDI"));
				pstmt.setString(153, rs.getString("AGENCY_ORGAN_ID"));
				pstmt.setString(154, rs.getString("RELATION_ORGAN_TO_PH"));
				pstmt.setString(155, rs.getString("PH_ROLE"));
				pstmt.setString(156, rs.getString("NHI_INSUR_INDI"));
				pstmt.setBigDecimal(157, rs.getBigDecimal("VERSION_TYPE_ID"));
				pstmt.setString(158, rs.getString("AGREE_READ_INDI"));
				pstmt.setBigDecimal(159, rs.getBigDecimal("ITEM_ORDER"));
				pstmt.setBigDecimal(160, rs.getBigDecimal("CUSTOMIZED_PREM"));
				pstmt.setString(161, rs.getString("SAME_STD_PREM_INDI"));
				pstmt.setBigDecimal(162, rs.getBigDecimal("PREMIUM_ADJUST_YEARLY"));
				pstmt.setBigDecimal(163, rs.getBigDecimal("PREMIUM_ADJUST_HALFYEARLY"));
				pstmt.setBigDecimal(164, rs.getBigDecimal("PREMIUM_ADJUST_QUARTERLY"));
				pstmt.setBigDecimal(165, rs.getBigDecimal("PREMIUM_ADJUST_MONTHLY"));
				pstmt.setBigDecimal(166, rs.getBigDecimal("PROPOSAL_TERM"));
				pstmt.setBigDecimal(167, rs.getBigDecimal("COMMISSION_VERSION"));
				pstmt.setBigDecimal(168, rs.getBigDecimal("INITIAL_SA"));
				pstmt.setString(169, rs.getString("OLD_PRODUCT_CODE"));
				pstmt.setString(170, rs.getString("COMPAIGN_CODE"));
				pstmt.setString(171, rs.getString("INITIAL_CHARGE_MODE"));
				pstmt.setString(172, rs.getString("CLAIM_STATUS"));
				pstmt.setBigDecimal(173, rs.getBigDecimal("INITIAL_TOTAL_PREM"));
				pstmt.setString(174, rs.getString("OVERWITHDRAW_INDI"));
				pstmt.setString(175, rs.getString("PRODUCT_TYPE"));
				pstmt.setString(176, rs.getString("INSURABILITY_INDICATOR"));
				pstmt.setBigDecimal(177, rs.getBigDecimal("ETI_SUR"));
				pstmt.setBigDecimal(178, rs.getBigDecimal("ETI_DAYS"));
				pstmt.setString(179, rs.getString("PREMIUM_IS_ZERO_INDI"));
				pstmt.setBigDecimal(180, rs.getBigDecimal("ILP_INCREASE_RATE"));
				pstmt.setTimestamp(181, ((rs.getDate("SPECIAL_EFFECT_DATE") == null)? null : new Timestamp(rs.getDate("SPECIAL_EFFECT_DATE").getTime())));
				pstmt.setBigDecimal(182, rs.getBigDecimal("NSP_AMOUNT"));
				pstmt.setString(183, rs.getString("PROPOASL_TERM_TYPE"));
				pstmt.setBigDecimal(184, rs.getBigDecimal("TOTAL_PAID_PREM"));
				pstmt.setBigDecimal(185, rs.getBigDecimal("ETI_YEARS"));
				pstmt.setString(186, rs.getString("LOADING_RATE_POINTER"));
				pstmt.setTimestamp(187, ((rs.getDate("CANCER_DATE") == null)? null : new Timestamp(rs.getDate("CANCER_DATE").getTime())));
				pstmt.setString(188, rs.getString("INSURABILITY_EXTRA_EXECUATE"));
				pstmt.setTimestamp(189, ((rs.getDate("LAST_CLAIM_DATE") == null)? null : new Timestamp(rs.getDate("LAST_CLAIM_DATE").getTime())));
				pstmt.setString(190, rs.getString("GUARANTEE_OPTION"));
				pstmt.setString(191, rs.getString("PREM_ALLOC_YEAR_INDI"));
				pstmt.setBigDecimal(192, rs.getBigDecimal("NAR"));
				pstmt.setString(193, rs.getString("HIDE_VALIDATE_DATE_INDI"));
				pstmt.setBigDecimal(194, rs.getBigDecimal("ETI_SUR_PB"));
				pstmt.setBigDecimal(195, rs.getBigDecimal("ETA_PAIDUP_AMOUNT"));
				pstmt.setBigDecimal(196, rs.getBigDecimal("ORI_PREMIUM_ADJUST_YEARLY"));
				pstmt.setBigDecimal(197, rs.getBigDecimal("ORI_PREMIUM_ADJUST_HALFYEARLY"));
				pstmt.setBigDecimal(198, rs.getBigDecimal("ORI_PREMIUM_ADJUST_QUARTERLY"));
				pstmt.setBigDecimal(199, rs.getBigDecimal("ORI_PREMIUM_ADJUST_MONTHLY"));
				pstmt.setBigDecimal(200, rs.getBigDecimal("CLAIM_DISABILITY_RATE"));
				pstmt.setString(201, rs.getString("HEALTH_INSURANCE_INDI"));
				pstmt.setString(202, rs.getString("INSURANCE_NOTICE_INDI"));
				pstmt.setString(203, rs.getString("SERVICES_BENEFIT_LEVEL"));
				pstmt.setString(204, rs.getString("PAYMENT_FREQ"));
				pstmt.setString(205, rs.getString("ISSUE_TYPE"));
				pstmt.setString(206, rs.getString("LOADING_RATE_POINTER_REASON"));
				pstmt.setString(207, rs.getString("RELATION_TO_PH_ROLE"));
				pstmt.setBigDecimal(208, rs.getBigDecimal("MONEY_DENOMINATED"));
				pstmt.setBigDecimal(209, rs.getBigDecimal("ORIGIN_PRODUCT_VERSION_ID"));
				pstmt.setTimestamp(210, ((rs.getDate("TRANSFORM_DATE") == null)? null : new Timestamp(rs.getDate("TRANSFORM_DATE").getTime())));
				pstmt.setString(211, rs.getString("HEALTH_INSURANCE_VERSION"));

				pstmt.addBatch();

				if (count % BATCH_COMMIT_SIZE == 0) {
					pstmt.executeBatch();//executing the batch  
					sinkConn.commit(); 
					pstmt.clearBatch();
				}
			}
			t2 = System.currentTimeMillis();
			
			pstmt.executeBatch();
			if (count > 0) {
				double avgTotal = ((double)(t2 - t0)) / count;
				sinkConn.commit(); 
				cnsl = System.console();
				cnsl.printf("   >>>insert into %s count=%d, startSeq=%d, endSeq=%d, queryspan=%d, insertspan=%d, avgTotal=%f \n", 
						"SINK_TABLE_NAME_CONTRACT_PRODUCT_LOG" , count, loadBean.startSeq, loadBean.endSeq, (t1 - t0), (t2 - t1), avgTotal);
				cnsl.flush();
			}


		}  catch (Exception e) {
			map.put("RETURN_CODE", "-999");
			map.put("SQL", sql);
			map.put("SINK_TABLE", "SINK_TABLE_NAME_CONTRACT_PRODUCT_LOG" );
			map.put("ERROR_MSG", e.getMessage());
			map.put("STACK_TRACE", ExceptionUtils.getStackTrace(e));
			logger.error("message={}, error map={}", e.getMessage(), map);

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
}
