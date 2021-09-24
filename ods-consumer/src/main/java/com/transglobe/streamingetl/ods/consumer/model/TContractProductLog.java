package com.transglobe.streamingetl.ods.consumer.model;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Json Property properties based on EBAO.T_Contract_Product_Log
 * Using Long for DATE type.
 *
 */
public class TContractProductLog {
	@JsonProperty("CHANGE_ID")
	private BigDecimal changeId;

	@JsonProperty("LOG_TYPE")
	private String logType;

	@JsonProperty("POLICY_CHG_ID")
	private BigDecimal policyChgId;

	@JsonProperty("ITEM_ID")
	private BigDecimal itemId;

	@JsonProperty("MASTER_ID")
	private BigDecimal masterId;

	@JsonProperty("POLICY_ID")
	private BigDecimal policyId;

	@JsonProperty("PRODUCT_ID")
	private BigDecimal productId;

	@JsonProperty("AMOUNT")
	private BigDecimal amount;

	@JsonProperty("UNIT")
	private BigDecimal unit;

	@JsonProperty("APPLY_DATE")
	private Long applyDate;

	@JsonProperty("EXPIRY_DATE")
	private Long expiryDate;

	@JsonProperty("VALIDATE_DATE")
	private Long validateDate;

	@JsonProperty("PAIDUP_DATE")
	private Long paidupDate;

	@JsonProperty("LIABILITY_STATE")
	private BigDecimal liabilityState;

	@JsonProperty("END_CAUSE")
	private BigDecimal endCause;

	@JsonProperty("INITIAL_TYPE")
	private String initialType;

	@JsonProperty("RENEWAL_TYPE")
	private String renewalType;

	@JsonProperty("CHARGE_PERIOD")
	private String chargePeriod;

	@JsonProperty("CHARGE_YEAR")
	private BigDecimal chargeYear;

	@JsonProperty("COVERAGE_PERIOD")
	private String coveragePeriod;

	@JsonProperty("COVERAGE_YEAR")
	private BigDecimal coverageYear;

	@JsonProperty("SHORT_END_TIME")
	private Long shortEndTime;

	@JsonProperty("EXCEPT_VALUE")
	private BigDecimal exceptValue;

	@JsonProperty("BENEFIT_LEVEL")
	private String benefitLevel;

	@JsonProperty("INSURED_CATEGORY")
	private String insuredCategory;

	@JsonProperty("SUSPEND")
	private String suspend;

	@JsonProperty("SUSPEND_CAUSE")
	private BigDecimal suspendCause;

	@JsonProperty("DERIVATION")
	private String derivation;

	@JsonProperty("PAY_MODE")
	private BigDecimal payMode;

	@JsonProperty("EXPIRY_CASH_VALUE")
	private BigDecimal expiryCashValue;

	@JsonProperty("DECISION_ID")
	private BigDecimal decisionId;

	@JsonProperty("COUNT_WAY")
	private String countWay;

	@JsonProperty("RENEW_DECISION")
	private String renewDecision;

	@JsonProperty("BONUS_SA")
	private BigDecimal bonusSa;

	@JsonProperty("PAY_NEXT")
	private BigDecimal payNext;

	@JsonProperty("ANNI_BALANCE")
	private BigDecimal anniBalance;

	@JsonProperty("FIX_INCREMENT")
	private String fixIncrement;

	@JsonProperty("CPF_COST")
	private BigDecimal cpfCost;

	@JsonProperty("CASH_COST")
	private BigDecimal cashCost;

	@JsonProperty("ORIGIN_SA")
	private BigDecimal origionSa;

	@JsonProperty("ORIGIN_BONUS_SA")
	private BigDecimal origionBonusSa;

	@JsonProperty("RISK_CODE")
	private BigDecimal riskCode;

	@JsonProperty("EXPOSURE_RATE")
	private BigDecimal exposureRate;

	@JsonProperty("REINS_RATE")
	private BigDecimal reinsRate;

	@JsonProperty("SUSPEND_CHG_ID")
	private BigDecimal suspendChgId;

	@JsonProperty("NEXT_AMOUNT")
	private BigDecimal nextAmount;

	@JsonProperty("WAIVER_START")
	private Long waiverStart;

	@JsonProperty("WAIVER_END")
	private Long eaiverEnd;

	@JsonProperty("AUTO_PERMNT_LAPSE")
	private String autoPermntCapse;

	@JsonProperty("PERMNT_LAPSE_NOTICE_DATE")
	private Long permntLapseNoticeDate;

	@JsonProperty("WAIVER")
	private String waiver;

	@JsonProperty("WAIVED_SA")
	private BigDecimal waivedSa;

	@JsonProperty("ISSUE_AGENT")
	private BigDecimal issueAgent;

	@JsonProperty("MASTER_PRODUCT")
	private BigDecimal masterproduct;

	@JsonProperty("STRATEGY_CODE")
	private String strategyCode;

	@JsonProperty("LOAN_TYPE")
	private String loanType;

	@JsonProperty("BEN_PERIOD_TYPE")
	private String benPeriodType;

	@JsonProperty("BENEFIT_PERIOD")
	private BigDecimal benefitPeriod;

	@JsonProperty("GURNT_START_DATE")
	private Long gurntStartDate;

	@JsonProperty("GURNT_PERD_TYPE")
	private String gurntPerdType;

	@JsonProperty("GURNT_PERIOD")
	private BigDecimal gurntPeriod;

	@JsonProperty("INVEST_HORIZON")
	private BigDecimal investHorizon;

	@JsonProperty("MANUAL_SA")
	private String manualSa;

	@JsonProperty("DEFER_PERIOD")
	private BigDecimal deferPeriod;

	@JsonProperty("WAIT_PERIOD")
	private BigDecimal waitPeriod;

	@JsonProperty("NEXT_DISCNTED_PREM_AF")
	private BigDecimal nextDiscntedPremAf;

	@JsonProperty("NEXT_POLICY_FEE_AF")
	private BigDecimal nextPolicyFeeAf;

	@JsonProperty("NEXT_GROSS_PREM_AF")
	private BigDecimal nextGrossPremAf;

	@JsonProperty("NEXT_EXTRA_PREM_AF")
	private BigDecimal nextExtraPremAf;

	@JsonProperty("NEXT_TOTAL_PREM_AF")
	private BigDecimal nextTotalPremAf;

	@JsonProperty("NEXT_STD_PREM_AN")
	private BigDecimal nextStdPremAn;

	@JsonProperty("NEXT_DISCNT_PREM_AN")
	private BigDecimal nextDisntPremAn;

	@JsonProperty("NEXT_DISCNTED_PREM_AN")
	private BigDecimal nextDisntedPremAn;

	@JsonProperty("NEXT_POLICY_FEE_AN")
	private BigDecimal nextPolicyFeeAn;

	@JsonProperty("NEXT_EXTRA_PREM_AN")
	private BigDecimal nextExtraPremAn;

	@JsonProperty("WAIV_ANUL_BENEFIT")
	private BigDecimal waivAnulBenefit;

	@JsonProperty("WAIV_ANUL_PREM")
	private BigDecimal waivAnulPrem;

	@JsonProperty("LAPSE_CAUSE")
	private BigDecimal lapseCause;

	@JsonProperty("PREM_CHANGE_TIME")
	private Long premChangeTime;

	@JsonProperty("SUBMISSION_DATE")
	private Long subMissionDate;

	@JsonProperty("NEXT_DISCNTED_PREM_BF")
	private BigDecimal nextDiscntedPremBf;

	@JsonProperty("NEXT_POLICY_FEE_BF")
	private BigDecimal nextPolicyFeeBf;

	@JsonProperty("NEXT_EXTRA_PREM_BF")
	private BigDecimal nextExtraPremBf;

	@JsonProperty("SA_FACTOR")
	private BigDecimal saFactor;

	@JsonProperty("NEXT_STD_PREM_AF")
	private BigDecimal nextStdPremAf;

	@JsonProperty("NEXT_DISCNT_PREM_AF")
	private BigDecimal nextDiscntPremAf;

	@JsonProperty("STD_PREM_BF")
	private BigDecimal stdPremBf;

	@JsonProperty("DISCNT_PREM_BF")
	private BigDecimal discntPremBf;

	@JsonProperty("DISCNTED_PREM_BF")
	private BigDecimal discntedPremBf;

	@JsonProperty("POLICY_FEE_BF")
	private BigDecimal policyFeeBf;

	@JsonProperty("EXTRA_PREM_BF")
	private BigDecimal extraPremBf;

	@JsonProperty("STD_PREM_AF")
	private BigDecimal stdPremAf;

	@JsonProperty("DISCNT_PREM_AF")
	private BigDecimal discntPremAf;

	@JsonProperty("POLICY_FEE_AF")
	private BigDecimal policyFeeAf;

	@JsonProperty("GROSS_PREM_AF")
	private BigDecimal grossPremAf;

	@JsonProperty("EXTRA_PREM_AF")
	private BigDecimal extraPremAf;

	@JsonProperty("TOTAL_PREM_AF")
	private BigDecimal totalPremAf;

	@JsonProperty("STD_PREM_AN")
	private BigDecimal stdPremAn;

	@JsonProperty("DISCNT_PREM_AN")
	private BigDecimal discntPremAn;

	@JsonProperty("DISCNTED_PREM_AN")
	private BigDecimal discntedPremAn;

	@JsonProperty("POLICY_FEE_AN")
	private BigDecimal policyFeeAn;

	@JsonProperty("EXTRA_PREM_AN")
	private BigDecimal extraPremAn;

	@JsonProperty("NEXT_STD_PREM_BF")
	private BigDecimal nextStdPremBf;

	@JsonProperty("NEXT_DISCNT_PREM_BF")
	private BigDecimal nextDiscntPremBf;

	@JsonProperty("DISCNTED_PREM_AF")
	private BigDecimal discntedpremAf;

	@JsonProperty("POLICY_PREM_SOURCE")
	private String policyPremSource;

	@JsonProperty("ACTUAL_VALIDATE")
	private Long actualValidate;

	@JsonProperty("ENTITY_FUND")
	private String entityFund;

	@JsonProperty("CAR_REG_NO")
	private String carRegNo;

	@JsonProperty("MANU_SURR_VALUE")
	private String manuSurrValue;

	@JsonProperty("LOG_ID")
	private BigDecimal logId;

	@JsonProperty("ILP_CALC_SA")
	private BigDecimal ilpCalcSa;

	@JsonProperty("P_LAPSE_DATE")
	private Long pLapseDate;

	@JsonProperty("INITIAL_VALI_DATE")
	private Long initialValiDate;

	@JsonProperty("AGE_INCREASE_INDI")
	private String ageIncreaseIndi;

	@JsonProperty("PAIDUP_OPTION")
	private String paidupOption;

	@JsonProperty("NEXT_COUNT_WAY")
	private String nextCountWay;

	@JsonProperty("NEXT_UNIT")
	private BigDecimal nextUnit;

	@JsonProperty("INSUR_PREM_AF")
	private BigDecimal insurPremAf;

	@JsonProperty("INSUR_PREM_AN")
	private BigDecimal insurPremAn;

	@JsonProperty("NEXT_INSUR_PREM_AF")
	private BigDecimal nextInsurPremAf;

	@JsonProperty("NEXT_INSUR_PREM_AN")
	private BigDecimal nextInsurPremAn;

	@JsonProperty("CAR_REG_NO_START")
	private Long carRegNoStart;

	@JsonProperty("PHD_PERIOD")
	private BigDecimal phdPeriod;

	@JsonProperty("ORIGIN_PRODUCT_ID")
	private BigDecimal origionProductId;

	@JsonProperty("LAST_STATEMENT_DATE")
	private Long lastStatementDate;

	@JsonProperty("PRE_WAR_INDI")
	private String preWarindi;

	@JsonProperty("WAIVER_CLAIM_TYPE")
	private BigDecimal waiverClaimType;

	@JsonProperty("ISSUE_DATE")
	private Long issueDate;

	@JsonProperty("ADVANTAGE_INDI")
	private String advantageIndi;

	@JsonProperty("RISK_COMMENCE_DATE")
	private Long riskCommenceDate;

	@JsonProperty("LAST_CMT_FLG")
	private String lastCmtFlg;

	@JsonProperty("EMS_VERSION")
	private BigDecimal emsVersion;

	@JsonProperty("INSERTED_BY")
	private BigDecimal insertedBy;

	@JsonProperty("UPDATED_BY")
	private BigDecimal updatedBy;

	@JsonProperty("INSERT_TIME")
	private Long insertTime;

	@JsonProperty("UPDATE_TIME")
	private Long updateTime;

	@JsonProperty("INSERT_TIMESTAMP")
	private Long insertTimestamp;

	@JsonProperty("UPDATE_TIMESTAMP")
	private Long updateTimestamp;

	@JsonProperty("LIABILITY_STATE_CAUSE")
	private BigDecimal liabilityStateCause;

	@JsonProperty("LIABILITY_STATE_DATE")
	private Long liabilityStateDate;

	@JsonProperty("INVEST_SCHEME")
	private String investScheme;

	@JsonProperty("TARIFF_TYPE")
	private BigDecimal tariffType;

	@JsonProperty("INDX_SUSPEND_CAUSE")
	private BigDecimal indxSuspendCause;

	@JsonProperty("INDX_TYPE")
	private BigDecimal indxType;

	@JsonProperty("INDX_CALC_BASIS")
	private BigDecimal indxCalcBasis;

	@JsonProperty("INDX_INDI")
	private String indxIndi;

	@JsonProperty("COOLING_OFF_OPTION")
	private BigDecimal coolingOffOption;

	@JsonProperty("POSTPONE_PERIOD")
	private BigDecimal postponePeriod;

	@JsonProperty("NEXT_BENEFIT_LEVEL")
	private String nextBenefitLevel;

	@JsonProperty("PRODUCT_VERSION_ID")
	private BigDecimal productVersionId;

	@JsonProperty("RENEW_INDI")
	private String renewIndi;

	@JsonProperty("AGENCY_ORGAN_ID")
	private String agentOrganId;

	@JsonProperty("RELATION_ORGAN_TO_PH")
	private String relationOrganToPh;

	@JsonProperty("PH_ROLE")
	private String phRole;

	@JsonProperty("NHI_INSUR_INDI")
	private String nhiInsurIndi;

	@JsonProperty("VERSION_TYPE_ID")
	private BigDecimal versionTypeId;

	@JsonProperty("AGREE_READ_INDI")
	private String agreeReadIndi;

	@JsonProperty("ITEM_ORDER")
	private BigDecimal itemOrder;

	@JsonProperty("CUSTOMIZED_PREM")
	private BigDecimal customizedPrem;

	@JsonProperty("SAME_STD_PREM_INDI")
	private String sameStdPremIndi;

	@JsonProperty("PREMIUM_ADJUST_YEARLY")
	private BigDecimal premiumAdjustYearly;

	@JsonProperty("PREMIUM_ADJUST_HALFYEARLY")
	private BigDecimal premiumAdjustHalfYearly;

	@JsonProperty("PREMIUM_ADJUST_QUARTERLY")
	private BigDecimal premiumAdjustQuarterly;

	@JsonProperty("PREMIUM_ADJUST_MONTHLY")
	private BigDecimal premiumAdjustMonthly;

	@JsonProperty("PROPOSAL_TERM")
	private BigDecimal proposalTerm;

	@JsonProperty("COMMISSION_VERSION")
	private BigDecimal commisionVersion;

	@JsonProperty("INITIAL_SA")
	private BigDecimal initialSa;

	@JsonProperty("OLD_PRODUCT_CODE")
	private String oldProductCode;

	@JsonProperty("COMPAIGN_CODE")
	private Long compaignCode;

	@JsonProperty("INITIAL_CHARGE_MODE")
	private String initialChargeMode;

	@JsonProperty("CLAIM_STATUS")
	private String claimStatus;

	@JsonProperty("INITIAL_TOTAL_PREM")
	private BigDecimal initialTotalPrem;

	@JsonProperty("OVERWITHDRAW_INDI")
	private String overWithdrawIndi;

	@JsonProperty("PRODUCT_TYPE")
	private String productType;

	@JsonProperty("INSURABILITY_INDICATOR")
	private String insurabilityIndicator;

	@JsonProperty("ETI_SUR")
	private BigDecimal etiSur;

	@JsonProperty("ETI_DAYS")
	private BigDecimal etiDays;

	@JsonProperty("PREMIUM_IS_ZERO_INDI")
	private String permiumIsSeroindi;

	@JsonProperty("ILP_INCREASE_RATE")
	private String ilpIncreaseRate;

	@JsonProperty("SPECIAL_EFFECT_DATE")
	private Long specialEffectDate;

	@JsonProperty("NSP_AMOUNT")
	private BigDecimal nspAmount;

	@JsonProperty("PROPOASL_TERM_TYPE")
	private String proposalTermType;

	@JsonProperty("TOTAL_PAID_PREM")
	private BigDecimal totalPaidPrem;

	@JsonProperty("ETI_YEARS")
	private BigDecimal etiYears;

	@JsonProperty("LOADING_RATE_POINTER")
	private String loadingRatePointer;

	@JsonProperty("CANCER_DATE")
	private Long cancerDate;

	@JsonProperty("INSURABILITY_EXTRA_EXECUATE")
	private String insurabilityExtraExecuate;

	@JsonProperty("LAST_CLAIM_DATE")
	private Long lastClaimDate;

	@JsonProperty("GUARANTEE_OPTION")
	private String guaranteeOption;

	@JsonProperty("PREM_ALLOC_YEAR_INDI")
	private String premAllocYearIndi;

	@JsonProperty("NAR")
	private BigDecimal nar;

	@JsonProperty("HIDE_VALIDATE_DATE_INDI")
	private String hideValidateDateIndi;

	@JsonProperty("ETI_SUR_PB")
	private BigDecimal etiSurPb;

	@JsonProperty("ETA_PAIDUP_AMOUNT")
	private BigDecimal etaPaidupAmount;

	@JsonProperty("ORI_PREMIUM_ADJUST_YEARLY")
	private BigDecimal oriPremiumAdjustYearly;

	@JsonProperty("ORI_PREMIUM_ADJUST_HALFYEARLY")
	private BigDecimal oriPremiumAdjustHalfyearly;

	@JsonProperty("ORI_PREMIUM_ADJUST_QUARTERLY")
	private BigDecimal oriPremiumAdjustQuarterly;

	@JsonProperty("ORI_PREMIUM_ADJUST_MONTHLY")
	private BigDecimal oriPremiumAdjustMonthly;

	@JsonProperty("CLAIM_DISABILITY_RATE")
	private BigDecimal claimdisabilityRate;

	@JsonProperty("HEALTH_INSURANCE_INDI")
	private String healthInsuranceIndi;

	@JsonProperty("INSURANCE_NOTICE_INDI")
	private String insuranceNoticeIndi;

	@JsonProperty("SERVICES_BENEFIT_LEVEL")
	private String servicesBenefitLevel;

	@JsonProperty("PAYMENT_FREQ")
	private String paymentFreq;

	@JsonProperty("ISSUE_TYPE")
	private String issueType;

	@JsonProperty("LOADING_RATE_POINTER_REASON")
	private String loadingRatePointerReason;

	@JsonProperty("RELATION_TO_PH_ROLE")
	private String relationToPhRole;

	@JsonProperty("MONEY_DENOMINATED")
	private BigDecimal moneyDenominated;

	@JsonProperty("ORIGIN_PRODUCT_VERSION_ID")
	private BigDecimal origionProductVersionId;

	@JsonProperty("TRANSFORM_DATE")
	private Long transformDate;

	@JsonProperty("HEALTH_INSURANCE_VERSION")
	private String healthInsuranceVersion;

	public BigDecimal getChangeId() {
		return changeId;
	}

	public void setChangeId(BigDecimal changeId) {
		this.changeId = changeId;
	}

	public String getLogType() {
		return logType;
	}

	public void setLogType(String logType) {
		this.logType = logType;
	}

	public BigDecimal getPolicyChgId() {
		return policyChgId;
	}

	public void setPolicyChgId(BigDecimal policyChgId) {
		this.policyChgId = policyChgId;
	}

	public BigDecimal getItemId() {
		return itemId;
	}

	public void setItemId(BigDecimal itemId) {
		this.itemId = itemId;
	}

	public BigDecimal getMasterId() {
		return masterId;
	}

	public void setMasterId(BigDecimal masterId) {
		this.masterId = masterId;
	}

	public BigDecimal getPolicyId() {
		return policyId;
	}

	public void setPolicyId(BigDecimal policyId) {
		this.policyId = policyId;
	}

	public BigDecimal getProductId() {
		return productId;
	}

	public void setProductId(BigDecimal productId) {
		this.productId = productId;
	}

	public BigDecimal getAmount() {
		return amount;
	}

	public void setAmount(BigDecimal amount) {
		this.amount = amount;
	}

	public BigDecimal getUnit() {
		return unit;
	}

	public void setUnit(BigDecimal unit) {
		this.unit = unit;
	}

	public Long getApplyDate() {
		return applyDate;
	}

	public void setApplyDate(Long applyDate) {
		this.applyDate = applyDate;
	}

	public Long getExpiryDate() {
		return expiryDate;
	}

	public void setExpiryDate(Long expiryDate) {
		this.expiryDate = expiryDate;
	}

	public Long getValidateDate() {
		return validateDate;
	}

	public void setValidateDate(Long validateDate) {
		this.validateDate = validateDate;
	}

	public Long getPaidupDate() {
		return paidupDate;
	}

	public void setPaidupDate(Long paidupDate) {
		this.paidupDate = paidupDate;
	}

	public BigDecimal getLiabilityState() {
		return liabilityState;
	}

	public void setLiabilityState(BigDecimal liabilityState) {
		this.liabilityState = liabilityState;
	}

	public BigDecimal getEndCause() {
		return endCause;
	}

	public void setEndCause(BigDecimal endCause) {
		this.endCause = endCause;
	}

	public String getInitialType() {
		return initialType;
	}

	public void setInitialType(String initialType) {
		this.initialType = initialType;
	}

	public String getRenewalType() {
		return renewalType;
	}

	public void setRenewalType(String renewalType) {
		this.renewalType = renewalType;
	}

	public String getChargePeriod() {
		return chargePeriod;
	}

	public void setChargePeriod(String chargePeriod) {
		this.chargePeriod = chargePeriod;
	}

	public BigDecimal getChargeYear() {
		return chargeYear;
	}

	public void setChargeYear(BigDecimal chargeYear) {
		this.chargeYear = chargeYear;
	}

	public String getCoveragePeriod() {
		return coveragePeriod;
	}

	public void setCoveragePeriod(String coveragePeriod) {
		this.coveragePeriod = coveragePeriod;
	}

	public BigDecimal getCoverageYear() {
		return coverageYear;
	}

	public void setCoverageYear(BigDecimal coverageYear) {
		this.coverageYear = coverageYear;
	}

	public Long getShortEndTime() {
		return shortEndTime;
	}

	public void setShortEndTime(Long shortEndTime) {
		this.shortEndTime = shortEndTime;
	}

	public BigDecimal getExceptValue() {
		return exceptValue;
	}

	public void setExceptValue(BigDecimal exceptValue) {
		this.exceptValue = exceptValue;
	}

	public String getBenefitLevel() {
		return benefitLevel;
	}

	public void setBenefitLevel(String benefitLevel) {
		this.benefitLevel = benefitLevel;
	}

	public String getInsuredCategory() {
		return insuredCategory;
	}

	public void setInsuredCategory(String insuredCategory) {
		this.insuredCategory = insuredCategory;
	}

	public String getSuspend() {
		return suspend;
	}

	public void setSuspend(String suspend) {
		this.suspend = suspend;
	}

	public BigDecimal getSuspendCause() {
		return suspendCause;
	}

	public void setSuspendCause(BigDecimal suspendCause) {
		this.suspendCause = suspendCause;
	}

	public String getDerivation() {
		return derivation;
	}

	public void setDerivation(String derivation) {
		this.derivation = derivation;
	}

	public BigDecimal getPayMode() {
		return payMode;
	}

	public void setPayMode(BigDecimal payMode) {
		this.payMode = payMode;
	}

	public BigDecimal getExpiryCashValue() {
		return expiryCashValue;
	}

	public void setExpiryCashValue(BigDecimal expiryCashValue) {
		this.expiryCashValue = expiryCashValue;
	}

	public BigDecimal getDecisionId() {
		return decisionId;
	}

	public void setDecisionId(BigDecimal decisionId) {
		this.decisionId = decisionId;
	}

	public String getCountWay() {
		return countWay;
	}

	public void setCountWay(String countWay) {
		this.countWay = countWay;
	}

	public String getRenewDecision() {
		return renewDecision;
	}

	public void setRenewDecision(String renewDecision) {
		this.renewDecision = renewDecision;
	}

	public BigDecimal getBonusSa() {
		return bonusSa;
	}

	public void setBonusSa(BigDecimal bonusSa) {
		this.bonusSa = bonusSa;
	}

	public BigDecimal getPayNext() {
		return payNext;
	}

	public void setPayNext(BigDecimal payNext) {
		this.payNext = payNext;
	}

	public BigDecimal getAnniBalance() {
		return anniBalance;
	}

	public void setAnniBalance(BigDecimal anniBalance) {
		this.anniBalance = anniBalance;
	}

	public String getFixIncrement() {
		return fixIncrement;
	}

	public void setFixIncrement(String fixIncrement) {
		this.fixIncrement = fixIncrement;
	}

	public BigDecimal getCpfCost() {
		return cpfCost;
	}

	public void setCpfCost(BigDecimal cpfCost) {
		this.cpfCost = cpfCost;
	}

	public BigDecimal getCashCost() {
		return cashCost;
	}

	public void setCashCost(BigDecimal cashCost) {
		this.cashCost = cashCost;
	}

	public BigDecimal getOrigionSa() {
		return origionSa;
	}

	public void setOrigionSa(BigDecimal origionSa) {
		this.origionSa = origionSa;
	}

	public BigDecimal getOrigionBonusSa() {
		return origionBonusSa;
	}

	public void setOrigionBonusSa(BigDecimal origionBonusSa) {
		this.origionBonusSa = origionBonusSa;
	}

	public BigDecimal getRiskCode() {
		return riskCode;
	}

	public void setRiskCode(BigDecimal riskCode) {
		this.riskCode = riskCode;
	}

	public BigDecimal getExposureRate() {
		return exposureRate;
	}

	public void setExposureRate(BigDecimal exposureRate) {
		this.exposureRate = exposureRate;
	}

	public BigDecimal getReinsRate() {
		return reinsRate;
	}

	public void setReinsRate(BigDecimal reinsRate) {
		this.reinsRate = reinsRate;
	}

	public BigDecimal getSuspendChgId() {
		return suspendChgId;
	}

	public void setSuspendChgId(BigDecimal suspendChgId) {
		this.suspendChgId = suspendChgId;
	}

	public BigDecimal getNextAmount() {
		return nextAmount;
	}

	public void setNextAmount(BigDecimal nextAmount) {
		this.nextAmount = nextAmount;
	}

	public Long getWaiverStart() {
		return waiverStart;
	}

	public void setWaiverStart(Long waiverStart) {
		this.waiverStart = waiverStart;
	}

	public Long getEaiverEnd() {
		return eaiverEnd;
	}

	public void setEaiverEnd(Long eaiverEnd) {
		this.eaiverEnd = eaiverEnd;
	}

	public String getAutoPermntCapse() {
		return autoPermntCapse;
	}

	public void setAutoPermntCapse(String autoPermntCapse) {
		this.autoPermntCapse = autoPermntCapse;
	}

	public Long getPermntLapseNoticeDate() {
		return permntLapseNoticeDate;
	}

	public void setPermntLapseNoticeDate(Long permntLapseNoticeDate) {
		this.permntLapseNoticeDate = permntLapseNoticeDate;
	}

	public String getWaiver() {
		return waiver;
	}

	public void setWaiver(String waiver) {
		this.waiver = waiver;
	}

	public BigDecimal getWaivedSa() {
		return waivedSa;
	}

	public void setWaivedSa(BigDecimal waivedSa) {
		this.waivedSa = waivedSa;
	}

	public BigDecimal getIssueAgent() {
		return issueAgent;
	}

	public void setIssueAgent(BigDecimal issueAgent) {
		this.issueAgent = issueAgent;
	}

	public BigDecimal getMasterproduct() {
		return masterproduct;
	}

	public void setMasterproduct(BigDecimal masterproduct) {
		this.masterproduct = masterproduct;
	}

	public String getStrategyCode() {
		return strategyCode;
	}

	public void setStrategyCode(String strategyCode) {
		this.strategyCode = strategyCode;
	}

	public String getLoanType() {
		return loanType;
	}

	public void setLoanType(String loanType) {
		this.loanType = loanType;
	}

	public String getBenPeriodType() {
		return benPeriodType;
	}

	public void setBenPeriodType(String benPeriodType) {
		this.benPeriodType = benPeriodType;
	}

	public BigDecimal getBenefitPeriod() {
		return benefitPeriod;
	}

	public void setBenefitPeriod(BigDecimal benefitPeriod) {
		this.benefitPeriod = benefitPeriod;
	}

	public Long getGurntStartDate() {
		return gurntStartDate;
	}

	public void setGurntStartDate(Long gurntStartDate) {
		this.gurntStartDate = gurntStartDate;
	}

	public String getGurntPerdType() {
		return gurntPerdType;
	}

	public void setGurntPerdType(String gurntPerdType) {
		this.gurntPerdType = gurntPerdType;
	}

	public BigDecimal getGurntPeriod() {
		return gurntPeriod;
	}

	public void setGurntPeriod(BigDecimal gurntPeriod) {
		this.gurntPeriod = gurntPeriod;
	}

	public BigDecimal getInvestHorizon() {
		return investHorizon;
	}

	public void setInvestHorizon(BigDecimal investHorizon) {
		this.investHorizon = investHorizon;
	}

	public String getManualSa() {
		return manualSa;
	}

	public void setManualSa(String manualSa) {
		this.manualSa = manualSa;
	}

	public BigDecimal getDeferPeriod() {
		return deferPeriod;
	}

	public void setDeferPeriod(BigDecimal deferPeriod) {
		this.deferPeriod = deferPeriod;
	}

	public BigDecimal getWaitPeriod() {
		return waitPeriod;
	}

	public void setWaitPeriod(BigDecimal waitPeriod) {
		this.waitPeriod = waitPeriod;
	}

	public BigDecimal getNextDiscntedPremAf() {
		return nextDiscntedPremAf;
	}

	public void setNextDiscntedPremAf(BigDecimal nextDiscntedPremAf) {
		this.nextDiscntedPremAf = nextDiscntedPremAf;
	}

	public BigDecimal getNextPolicyFeeAf() {
		return nextPolicyFeeAf;
	}

	public void setNextPolicyFeeAf(BigDecimal nextPolicyFeeAf) {
		this.nextPolicyFeeAf = nextPolicyFeeAf;
	}

	public BigDecimal getNextGrossPremAf() {
		return nextGrossPremAf;
	}

	public void setNextGrossPremAf(BigDecimal nextGrossPremAf) {
		this.nextGrossPremAf = nextGrossPremAf;
	}

	public BigDecimal getNextExtraPremAf() {
		return nextExtraPremAf;
	}

	public void setNextExtraPremAf(BigDecimal nextExtraPremAf) {
		this.nextExtraPremAf = nextExtraPremAf;
	}

	public BigDecimal getNextTotalPremAf() {
		return nextTotalPremAf;
	}

	public void setNextTotalPremAf(BigDecimal nextTotalPremAf) {
		this.nextTotalPremAf = nextTotalPremAf;
	}

	public BigDecimal getNextStdPremAn() {
		return nextStdPremAn;
	}

	public void setNextStdPremAn(BigDecimal nextStdPremAn) {
		this.nextStdPremAn = nextStdPremAn;
	}

	public BigDecimal getNextDisntPremAn() {
		return nextDisntPremAn;
	}

	public void setNextDisntPremAn(BigDecimal nextDisntPremAn) {
		this.nextDisntPremAn = nextDisntPremAn;
	}

	public BigDecimal getNextDisntedPremAn() {
		return nextDisntedPremAn;
	}

	public void setNextDisntedPremAn(BigDecimal nextDisntedPremAn) {
		this.nextDisntedPremAn = nextDisntedPremAn;
	}

	public BigDecimal getNextPolicyFeeAn() {
		return nextPolicyFeeAn;
	}

	public void setNextPolicyFeeAn(BigDecimal nextPolicyFeeAn) {
		this.nextPolicyFeeAn = nextPolicyFeeAn;
	}

	public BigDecimal getNextExtraPremAn() {
		return nextExtraPremAn;
	}

	public void setNextExtraPremAn(BigDecimal nextExtraPremAn) {
		this.nextExtraPremAn = nextExtraPremAn;
	}

	public BigDecimal getWaivAnulBenefit() {
		return waivAnulBenefit;
	}

	public void setWaivAnulBenefit(BigDecimal waivAnulBenefit) {
		this.waivAnulBenefit = waivAnulBenefit;
	}

	public BigDecimal getWaivAnulPrem() {
		return waivAnulPrem;
	}

	public void setWaivAnulPrem(BigDecimal waivAnulPrem) {
		this.waivAnulPrem = waivAnulPrem;
	}

	public BigDecimal getLapseCause() {
		return lapseCause;
	}

	public void setLapseCause(BigDecimal lapseCause) {
		this.lapseCause = lapseCause;
	}

	public Long getPremChangeTime() {
		return premChangeTime;
	}

	public void setPremChangeTime(Long premChangeTime) {
		this.premChangeTime = premChangeTime;
	}

	public Long getSubMissionDate() {
		return subMissionDate;
	}

	public void setSubMissionDate(Long subMissionDate) {
		this.subMissionDate = subMissionDate;
	}

	public BigDecimal getNextDiscntedPremBf() {
		return nextDiscntedPremBf;
	}

	public void setNextDiscntedPremBf(BigDecimal nextDiscntedPremBf) {
		this.nextDiscntedPremBf = nextDiscntedPremBf;
	}

	public BigDecimal getNextPolicyFeeBf() {
		return nextPolicyFeeBf;
	}

	public void setNextPolicyFeeBf(BigDecimal nextPolicyFeeBf) {
		this.nextPolicyFeeBf = nextPolicyFeeBf;
	}

	public BigDecimal getNextExtraPremBf() {
		return nextExtraPremBf;
	}

	public void setNextExtraPremBf(BigDecimal nextExtraPremBf) {
		this.nextExtraPremBf = nextExtraPremBf;
	}

	public BigDecimal getSaFactor() {
		return saFactor;
	}

	public void setSaFactor(BigDecimal saFactor) {
		this.saFactor = saFactor;
	}

	public BigDecimal getNextStdPremAf() {
		return nextStdPremAf;
	}

	public void setNextStdPremAf(BigDecimal nextStdPremAf) {
		this.nextStdPremAf = nextStdPremAf;
	}

	public BigDecimal getNextDiscntPremAf() {
		return nextDiscntPremAf;
	}

	public void setNextDiscntPremAf(BigDecimal nextDiscntPremAf) {
		this.nextDiscntPremAf = nextDiscntPremAf;
	}

	public BigDecimal getStdPremBf() {
		return stdPremBf;
	}

	public void setStdPremBf(BigDecimal stdPremBf) {
		this.stdPremBf = stdPremBf;
	}

	public BigDecimal getDiscntPremBf() {
		return discntPremBf;
	}

	public void setDiscntPremBf(BigDecimal discntPremBf) {
		this.discntPremBf = discntPremBf;
	}

	public BigDecimal getDiscntedPremBf() {
		return discntedPremBf;
	}

	public void setDiscntedPremBf(BigDecimal discntedPremBf) {
		this.discntedPremBf = discntedPremBf;
	}

	public BigDecimal getPolicyFeeBf() {
		return policyFeeBf;
	}

	public void setPolicyFeeBf(BigDecimal policyFeeBf) {
		this.policyFeeBf = policyFeeBf;
	}

	public BigDecimal getExtraPremBf() {
		return extraPremBf;
	}

	public void setExtraPremBf(BigDecimal extraPremBf) {
		this.extraPremBf = extraPremBf;
	}

	public BigDecimal getStdPremAf() {
		return stdPremAf;
	}

	public void setStdPremAf(BigDecimal stdPremAf) {
		this.stdPremAf = stdPremAf;
	}

	public BigDecimal getDiscntPremAf() {
		return discntPremAf;
	}

	public void setDiscntPremAf(BigDecimal discntPremAf) {
		this.discntPremAf = discntPremAf;
	}

	public BigDecimal getPolicyFeeAf() {
		return policyFeeAf;
	}

	public void setPolicyFeeAf(BigDecimal policyFeeAf) {
		this.policyFeeAf = policyFeeAf;
	}

	public BigDecimal getGrossPremAf() {
		return grossPremAf;
	}

	public void setGrossPremAf(BigDecimal grossPremAf) {
		this.grossPremAf = grossPremAf;
	}

	public BigDecimal getExtraPremAf() {
		return extraPremAf;
	}

	public void setExtraPremAf(BigDecimal extraPremAf) {
		this.extraPremAf = extraPremAf;
	}

	public BigDecimal getTotalPremAf() {
		return totalPremAf;
	}

	public void setTotalPremAf(BigDecimal totalPremAf) {
		this.totalPremAf = totalPremAf;
	}

	public BigDecimal getStdPremAn() {
		return stdPremAn;
	}

	public void setStdPremAn(BigDecimal stdPremAn) {
		this.stdPremAn = stdPremAn;
	}

	public BigDecimal getDiscntPremAn() {
		return discntPremAn;
	}

	public void setDiscntPremAn(BigDecimal discntPremAn) {
		this.discntPremAn = discntPremAn;
	}

	public BigDecimal getDiscntedPremAn() {
		return discntedPremAn;
	}

	public void setDiscntedPremAn(BigDecimal discntedPremAn) {
		this.discntedPremAn = discntedPremAn;
	}

	public BigDecimal getPolicyFeeAn() {
		return policyFeeAn;
	}

	public void setPolicyFeeAn(BigDecimal policyFeeAn) {
		this.policyFeeAn = policyFeeAn;
	}

	public BigDecimal getExtraPremAn() {
		return extraPremAn;
	}

	public void setExtraPremAn(BigDecimal extraPremAn) {
		this.extraPremAn = extraPremAn;
	}

	public BigDecimal getNextStdPremBf() {
		return nextStdPremBf;
	}

	public void setNextStdPremBf(BigDecimal nextStdPremBf) {
		this.nextStdPremBf = nextStdPremBf;
	}

	public BigDecimal getNextDiscntPremBf() {
		return nextDiscntPremBf;
	}

	public void setNextDiscntPremBf(BigDecimal nextDiscntPremBf) {
		this.nextDiscntPremBf = nextDiscntPremBf;
	}

	public BigDecimal getDiscntedpremAf() {
		return discntedpremAf;
	}

	public void setDiscntedpremAf(BigDecimal discntedpremAf) {
		this.discntedpremAf = discntedpremAf;
	}

	public String getPolicyPremSource() {
		return policyPremSource;
	}

	public void setPolicyPremSource(String policyPremSource) {
		this.policyPremSource = policyPremSource;
	}

	public Long getActualValidate() {
		return actualValidate;
	}

	public void setActualValidate(Long actualValidate) {
		this.actualValidate = actualValidate;
	}

	public String getEntityFund() {
		return entityFund;
	}

	public void setEntityFund(String entityFund) {
		this.entityFund = entityFund;
	}

	public String getCarRegNo() {
		return carRegNo;
	}

	public void setCarRegNo(String carRegNo) {
		this.carRegNo = carRegNo;
	}

	public String getManuSurrValue() {
		return manuSurrValue;
	}

	public void setManuSurrValue(String manuSurrValue) {
		this.manuSurrValue = manuSurrValue;
	}

	public BigDecimal getLogId() {
		return logId;
	}

	public void setLogId(BigDecimal logId) {
		this.logId = logId;
	}

	public BigDecimal getIlpCalcSa() {
		return ilpCalcSa;
	}

	public void setIlpCalcSa(BigDecimal ilpCalcSa) {
		this.ilpCalcSa = ilpCalcSa;
	}

	public Long getpLapseDate() {
		return pLapseDate;
	}

	public void setpLapseDate(Long pLapseDate) {
		this.pLapseDate = pLapseDate;
	}

	public Long getInitialValiDate() {
		return initialValiDate;
	}

	public void setInitialValiDate(Long initialValiDate) {
		this.initialValiDate = initialValiDate;
	}

	public String getAgeIncreaseIndi() {
		return ageIncreaseIndi;
	}

	public void setAgeIncreaseIndi(String ageIncreaseIndi) {
		this.ageIncreaseIndi = ageIncreaseIndi;
	}

	public String getPaidupOption() {
		return paidupOption;
	}

	public void setPaidupOption(String paidupOption) {
		this.paidupOption = paidupOption;
	}

	public String getNextCountWay() {
		return nextCountWay;
	}

	public void setNextCountWay(String nextCountWay) {
		this.nextCountWay = nextCountWay;
	}

	public BigDecimal getNextUnit() {
		return nextUnit;
	}

	public void setNextUnit(BigDecimal nextUnit) {
		this.nextUnit = nextUnit;
	}

	public BigDecimal getInsurPremAf() {
		return insurPremAf;
	}

	public void setInsurPremAf(BigDecimal insurPremAf) {
		this.insurPremAf = insurPremAf;
	}

	public BigDecimal getInsurPremAn() {
		return insurPremAn;
	}

	public void setInsurPremAn(BigDecimal insurPremAn) {
		this.insurPremAn = insurPremAn;
	}

	public BigDecimal getNextInsurPremAf() {
		return nextInsurPremAf;
	}

	public void setNextInsurPremAf(BigDecimal nextInsurPremAf) {
		this.nextInsurPremAf = nextInsurPremAf;
	}

	public BigDecimal getNextInsurPremAn() {
		return nextInsurPremAn;
	}

	public void setNextInsurPremAn(BigDecimal nextInsurPremAn) {
		this.nextInsurPremAn = nextInsurPremAn;
	}

	public Long getCarRegNoStart() {
		return carRegNoStart;
	}

	public void setCarRegNoStart(Long carRegNoStart) {
		this.carRegNoStart = carRegNoStart;
	}

	public BigDecimal getPhdPeriod() {
		return phdPeriod;
	}

	public void setPhdPeriod(BigDecimal phdPeriod) {
		this.phdPeriod = phdPeriod;
	}

	public BigDecimal getOrigionProductId() {
		return origionProductId;
	}

	public void setOrigionProductId(BigDecimal origionProductId) {
		this.origionProductId = origionProductId;
	}

	public Long getLastStatementDate() {
		return lastStatementDate;
	}

	public void setLastStatementDate(Long lastStatementDate) {
		this.lastStatementDate = lastStatementDate;
	}

	public String getPreWarindi() {
		return preWarindi;
	}

	public void setPreWarindi(String preWarindi) {
		this.preWarindi = preWarindi;
	}

	public BigDecimal getWaiverClaimType() {
		return waiverClaimType;
	}

	public void setWaiverClaimType(BigDecimal waiverClaimType) {
		this.waiverClaimType = waiverClaimType;
	}

	public Long getIssueDate() {
		return issueDate;
	}

	public void setIssueDate(Long issueDate) {
		this.issueDate = issueDate;
	}

	public String getAdvantageIndi() {
		return advantageIndi;
	}

	public void setAdvantageIndi(String advantageIndi) {
		this.advantageIndi = advantageIndi;
	}

	public Long getRiskCommenceDate() {
		return riskCommenceDate;
	}

	public void setRiskCommenceDate(Long riskCommenceDate) {
		this.riskCommenceDate = riskCommenceDate;
	}

	public String getLastCmtFlg() {
		return lastCmtFlg;
	}

	public void setLastCmtFlg(String lastCmtFlg) {
		this.lastCmtFlg = lastCmtFlg;
	}

	public BigDecimal getEmsVersion() {
		return emsVersion;
	}

	public void setEmsVersion(BigDecimal emsVersion) {
		this.emsVersion = emsVersion;
	}

	public BigDecimal getInsertedBy() {
		return insertedBy;
	}

	public void setInsertedBy(BigDecimal insertedBy) {
		this.insertedBy = insertedBy;
	}

	public BigDecimal getUpdatedBy() {
		return updatedBy;
	}

	public void setUpdatedBy(BigDecimal updatedBy) {
		this.updatedBy = updatedBy;
	}

	public Long getInsertTime() {
		return insertTime;
	}

	public void setInsertTime(Long insertTime) {
		this.insertTime = insertTime;
	}

	public Long getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Long updateTime) {
		this.updateTime = updateTime;
	}

	public Long getInsertTimestamp() {
		return insertTimestamp;
	}

	public void setInsertTimestamp(Long insertTimestamp) {
		this.insertTimestamp = insertTimestamp;
	}

	public Long getUpdateTimestamp() {
		return updateTimestamp;
	}

	public void setUpdateTimestamp(Long updateTimestamp) {
		this.updateTimestamp = updateTimestamp;
	}

	public BigDecimal getLiabilityStateCause() {
		return liabilityStateCause;
	}

	public void setLiabilityStateCause(BigDecimal liabilityStateCause) {
		this.liabilityStateCause = liabilityStateCause;
	}

	public Long getLiabilityStateDate() {
		return liabilityStateDate;
	}

	public void setLiabilityStateDate(Long liabilityStateDate) {
		this.liabilityStateDate = liabilityStateDate;
	}

	public String getInvestScheme() {
		return investScheme;
	}

	public void setInvestScheme(String investScheme) {
		this.investScheme = investScheme;
	}

	public BigDecimal getTariffType() {
		return tariffType;
	}

	public void setTariffType(BigDecimal tariffType) {
		this.tariffType = tariffType;
	}

	public BigDecimal getIndxSuspendCause() {
		return indxSuspendCause;
	}

	public void setIndxSuspendCause(BigDecimal indxSuspendCause) {
		this.indxSuspendCause = indxSuspendCause;
	}

	public BigDecimal getIndxType() {
		return indxType;
	}

	public void setIndxType(BigDecimal indxType) {
		this.indxType = indxType;
	}

	public BigDecimal getIndxCalcBasis() {
		return indxCalcBasis;
	}

	public void setIndxCalcBasis(BigDecimal indxCalcBasis) {
		this.indxCalcBasis = indxCalcBasis;
	}

	public String getIndxIndi() {
		return indxIndi;
	}

	public void setIndxIndi(String indxIndi) {
		this.indxIndi = indxIndi;
	}

	public BigDecimal getCoolingOffOption() {
		return coolingOffOption;
	}

	public void setCoolingOffOption(BigDecimal coolingOffOption) {
		this.coolingOffOption = coolingOffOption;
	}

	public BigDecimal getPostponePeriod() {
		return postponePeriod;
	}

	public void setPostponePeriod(BigDecimal postponePeriod) {
		this.postponePeriod = postponePeriod;
	}

	public String getNextBenefitLevel() {
		return nextBenefitLevel;
	}

	public void setNextBenefitLevel(String nextBenefitLevel) {
		this.nextBenefitLevel = nextBenefitLevel;
	}

	public BigDecimal getProductVersionId() {
		return productVersionId;
	}

	public void setProductVersionId(BigDecimal productVersionId) {
		this.productVersionId = productVersionId;
	}

	public String getRenewIndi() {
		return renewIndi;
	}

	public void setRenewIndi(String renewIndi) {
		this.renewIndi = renewIndi;
	}

	public String getAgentOrganId() {
		return agentOrganId;
	}

	public void setAgentOrganId(String agentOrganId) {
		this.agentOrganId = agentOrganId;
	}

	public String getRelationOrganToPh() {
		return relationOrganToPh;
	}

	public void setRelationOrganToPh(String relationOrganToPh) {
		this.relationOrganToPh = relationOrganToPh;
	}

	public String getPhRole() {
		return phRole;
	}

	public void setPhRole(String phRole) {
		this.phRole = phRole;
	}

	public String getNhiInsurIndi() {
		return nhiInsurIndi;
	}

	public void setNhiInsurIndi(String nhiInsurIndi) {
		this.nhiInsurIndi = nhiInsurIndi;
	}

	public BigDecimal getVersionTypeId() {
		return versionTypeId;
	}

	public void setVersionTypeId(BigDecimal versionTypeId) {
		this.versionTypeId = versionTypeId;
	}

	public String getAgreeReadIndi() {
		return agreeReadIndi;
	}

	public void setAgreeReadIndi(String agreeReadIndi) {
		this.agreeReadIndi = agreeReadIndi;
	}

	public BigDecimal getItemOrder() {
		return itemOrder;
	}

	public void setItemOrder(BigDecimal itemOrder) {
		this.itemOrder = itemOrder;
	}

	public BigDecimal getCustomizedPrem() {
		return customizedPrem;
	}

	public void setCustomizedPrem(BigDecimal customizedPrem) {
		this.customizedPrem = customizedPrem;
	}

	public String getSameStdPremIndi() {
		return sameStdPremIndi;
	}

	public void setSameStdPremIndi(String sameStdPremIndi) {
		this.sameStdPremIndi = sameStdPremIndi;
	}

	public BigDecimal getPremiumAdjustYearly() {
		return premiumAdjustYearly;
	}

	public void setPremiumAdjustYearly(BigDecimal premiumAdjustYearly) {
		this.premiumAdjustYearly = premiumAdjustYearly;
	}

	public BigDecimal getPremiumAdjustHalfYearly() {
		return premiumAdjustHalfYearly;
	}

	public void setPremiumAdjustHalfYearly(BigDecimal premiumAdjustHalfYearly) {
		this.premiumAdjustHalfYearly = premiumAdjustHalfYearly;
	}

	public BigDecimal getPremiumAdjustQuarterly() {
		return premiumAdjustQuarterly;
	}

	public void setPremiumAdjustQuarterly(BigDecimal premiumAdjustQuarterly) {
		this.premiumAdjustQuarterly = premiumAdjustQuarterly;
	}

	public BigDecimal getPremiumAdjustMonthly() {
		return premiumAdjustMonthly;
	}

	public void setPremiumAdjustMonthly(BigDecimal premiumAdjustMonthly) {
		this.premiumAdjustMonthly = premiumAdjustMonthly;
	}

	public BigDecimal getProposalTerm() {
		return proposalTerm;
	}

	public void setProposalTerm(BigDecimal proposalTerm) {
		this.proposalTerm = proposalTerm;
	}

	public BigDecimal getCommisionVersion() {
		return commisionVersion;
	}

	public void setCommisionVersion(BigDecimal commisionVersion) {
		this.commisionVersion = commisionVersion;
	}

	public BigDecimal getInitialSa() {
		return initialSa;
	}

	public void setInitialSa(BigDecimal initialSa) {
		this.initialSa = initialSa;
	}

	public String getOldProductCode() {
		return oldProductCode;
	}

	public void setOldProductCode(String oldProductCode) {
		this.oldProductCode = oldProductCode;
	}

	public Long getCompaignCode() {
		return compaignCode;
	}

	public void setCompaignCode(Long compaignCode) {
		this.compaignCode = compaignCode;
	}

	public String getInitialChargeMode() {
		return initialChargeMode;
	}

	public void setInitialChargeMode(String initialChargeMode) {
		this.initialChargeMode = initialChargeMode;
	}

	public String getClaimStatus() {
		return claimStatus;
	}

	public void setClaimStatus(String claimStatus) {
		this.claimStatus = claimStatus;
	}

	public BigDecimal getInitialTotalPrem() {
		return initialTotalPrem;
	}

	public void setInitialTotalPrem(BigDecimal initialTotalPrem) {
		this.initialTotalPrem = initialTotalPrem;
	}

	public String getOverWithdrawIndi() {
		return overWithdrawIndi;
	}

	public void setOverWithdrawIndi(String overWithdrawIndi) {
		this.overWithdrawIndi = overWithdrawIndi;
	}

	public String getProductType() {
		return productType;
	}

	public void setProductType(String productType) {
		this.productType = productType;
	}

	public String getInsurabilityIndicator() {
		return insurabilityIndicator;
	}

	public void setInsurabilityIndicator(String insurabilityIndicator) {
		this.insurabilityIndicator = insurabilityIndicator;
	}

	public BigDecimal getEtiSur() {
		return etiSur;
	}

	public void setEtiSur(BigDecimal etiSur) {
		this.etiSur = etiSur;
	}

	public BigDecimal getEtiDays() {
		return etiDays;
	}

	public void setEtiDays(BigDecimal etiDays) {
		this.etiDays = etiDays;
	}

	public String getPermiumIsSeroindi() {
		return permiumIsSeroindi;
	}

	public void setPermiumIsSeroindi(String permiumIsSeroindi) {
		this.permiumIsSeroindi = permiumIsSeroindi;
	}

	public String getIlpIncreaseRate() {
		return ilpIncreaseRate;
	}

	public void setIlpIncreaseRate(String ilpIncreaseRate) {
		this.ilpIncreaseRate = ilpIncreaseRate;
	}

	public Long getSpecialEffectDate() {
		return specialEffectDate;
	}

	public void setSpecialEffectDate(Long specialEffectDate) {
		this.specialEffectDate = specialEffectDate;
	}

	public BigDecimal getNspAmount() {
		return nspAmount;
	}

	public void setNspAmount(BigDecimal nspAmount) {
		this.nspAmount = nspAmount;
	}

	public String getProposalTermType() {
		return proposalTermType;
	}

	public void setProposalTermType(String proposalTermType) {
		this.proposalTermType = proposalTermType;
	}

	public BigDecimal getTotalPaidPrem() {
		return totalPaidPrem;
	}

	public void setTotalPaidPrem(BigDecimal totalPaidPrem) {
		this.totalPaidPrem = totalPaidPrem;
	}

	public BigDecimal getEtiYears() {
		return etiYears;
	}

	public void setEtiYears(BigDecimal etiYears) {
		this.etiYears = etiYears;
	}

	public String getLoadingRatePointer() {
		return loadingRatePointer;
	}

	public void setLoadingRatePointer(String loadingRatePointer) {
		this.loadingRatePointer = loadingRatePointer;
	}

	public Long getCancerDate() {
		return cancerDate;
	}

	public void setCancerDate(Long cancerDate) {
		this.cancerDate = cancerDate;
	}

	public String getInsurabilityExtraExecuate() {
		return insurabilityExtraExecuate;
	}

	public void setInsurabilityExtraExecuate(String insurabilityExtraExecuate) {
		this.insurabilityExtraExecuate = insurabilityExtraExecuate;
	}

	public Long getLastClaimDate() {
		return lastClaimDate;
	}

	public void setLastClaimDate(Long lastClaimDate) {
		this.lastClaimDate = lastClaimDate;
	}

	public String getGuaranteeOption() {
		return guaranteeOption;
	}

	public void setGuaranteeOption(String guaranteeOption) {
		this.guaranteeOption = guaranteeOption;
	}

	public String getPremAllocYearIndi() {
		return premAllocYearIndi;
	}

	public void setPremAllocYearIndi(String premAllocYearIndi) {
		this.premAllocYearIndi = premAllocYearIndi;
	}

	public BigDecimal getNar() {
		return nar;
	}

	public void setNar(BigDecimal nar) {
		this.nar = nar;
	}

	public String getHideValidateDateIndi() {
		return hideValidateDateIndi;
	}

	public void setHideValidateDateIndi(String hideValidateDateIndi) {
		this.hideValidateDateIndi = hideValidateDateIndi;
	}

	public BigDecimal getEtiSurPb() {
		return etiSurPb;
	}

	public void setEtiSurPb(BigDecimal etiSurPb) {
		this.etiSurPb = etiSurPb;
	}

	public BigDecimal getEtaPaidupAmount() {
		return etaPaidupAmount;
	}

	public void setEtaPaidupAmount(BigDecimal etaPaidupAmount) {
		this.etaPaidupAmount = etaPaidupAmount;
	}

	public BigDecimal getOriPremiumAdjustYearly() {
		return oriPremiumAdjustYearly;
	}

	public void setOriPremiumAdjustYearly(BigDecimal oriPremiumAdjustYearly) {
		this.oriPremiumAdjustYearly = oriPremiumAdjustYearly;
	}

	public BigDecimal getOriPremiumAdjustHalfyearly() {
		return oriPremiumAdjustHalfyearly;
	}

	public void setOriPremiumAdjustHalfyearly(BigDecimal oriPremiumAdjustHalfyearly) {
		this.oriPremiumAdjustHalfyearly = oriPremiumAdjustHalfyearly;
	}

	public BigDecimal getOriPremiumAdjustQuarterly() {
		return oriPremiumAdjustQuarterly;
	}

	public void setOriPremiumAdjustQuarterly(BigDecimal oriPremiumAdjustQuarterly) {
		this.oriPremiumAdjustQuarterly = oriPremiumAdjustQuarterly;
	}

	public BigDecimal getOriPremiumAdjustMonthly() {
		return oriPremiumAdjustMonthly;
	}

	public void setOriPremiumAdjustMonthly(BigDecimal oriPremiumAdjustMonthly) {
		this.oriPremiumAdjustMonthly = oriPremiumAdjustMonthly;
	}

	public BigDecimal getClaimdisabilityRate() {
		return claimdisabilityRate;
	}

	public void setClaimdisabilityRate(BigDecimal claimdisabilityRate) {
		this.claimdisabilityRate = claimdisabilityRate;
	}

	public String getHealthInsuranceIndi() {
		return healthInsuranceIndi;
	}

	public void setHealthInsuranceIndi(String healthInsuranceIndi) {
		this.healthInsuranceIndi = healthInsuranceIndi;
	}

	public String getInsuranceNoticeIndi() {
		return insuranceNoticeIndi;
	}

	public void setInsuranceNoticeIndi(String insuranceNoticeIndi) {
		this.insuranceNoticeIndi = insuranceNoticeIndi;
	}

	public String getServicesBenefitLevel() {
		return servicesBenefitLevel;
	}

	public void setServicesBenefitLevel(String servicesBenefitLevel) {
		this.servicesBenefitLevel = servicesBenefitLevel;
	}

	public String getPaymentFreq() {
		return paymentFreq;
	}

	public void setPaymentFreq(String paymentFreq) {
		this.paymentFreq = paymentFreq;
	}

	public String getIssueType() {
		return issueType;
	}

	public void setIssueType(String issueType) {
		this.issueType = issueType;
	}

	public String getLoadingRatePointerReason() {
		return loadingRatePointerReason;
	}

	public void setLoadingRatePointerReason(String loadingRatePointerReason) {
		this.loadingRatePointerReason = loadingRatePointerReason;
	}

	public String getRelationToPhRole() {
		return relationToPhRole;
	}

	public void setRelationToPhRole(String relationToPhRole) {
		this.relationToPhRole = relationToPhRole;
	}

	public BigDecimal getMoneyDenominated() {
		return moneyDenominated;
	}

	public void setMoneyDenominated(BigDecimal moneyDenominated) {
		this.moneyDenominated = moneyDenominated;
	}

	public BigDecimal getOrigionProductVersionId() {
		return origionProductVersionId;
	}

	public void setOrigionProductVersionId(BigDecimal origionProductVersionId) {
		this.origionProductVersionId = origionProductVersionId;
	}

	public Long getTransformDate() {
		return transformDate;
	}

	public void setTransformDate(Long transformDate) {
		this.transformDate = transformDate;
	}

	public String getHealthInsuranceVersion() {
		return healthInsuranceVersion;
	}

	public void setHealthInsuranceVersion(String healthInsuranceVersion) {
		this.healthInsuranceVersion = healthInsuranceVersion;
	}
	
	
}
