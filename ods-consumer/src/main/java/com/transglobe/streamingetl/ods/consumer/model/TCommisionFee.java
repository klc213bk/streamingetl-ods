package com.transglobe.streamingetl.ods.consumer.model;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Json Property properties based on EBAO.T_PRODUCTION_DETAIL
 * Using Long for DATE type.
 *
 */
public class TCommisionFee {

	@JsonProperty("FEE_ID")
	private BigDecimal feeId;
	
	@JsonProperty("SOURCE_ID")
	private BigDecimal sourceId;
	
	@JsonProperty("SOURCE_TABLE")
	private BigDecimal sourceTable;
	
	@JsonProperty("COMMISION_TYPE_ID")
	private BigDecimal commisionTypeId;
	
	@JsonProperty("COMMISION")
	private BigDecimal commision;
	
	@JsonProperty("AGENT_ID")
	private BigDecimal agentId;
	
	@JsonProperty("AGENT_CATE")
	private String agentCate;
	
	@JsonProperty("AGENT_GRADE")
	private String agentGrade;
	
	@JsonProperty("CHANNEL_ORG_ID")
	private BigDecimal channelOrgId;
	
	@JsonProperty("HEAD_ID")
	private BigDecimal headId;
	
	@JsonProperty("BRANCH_ID")
	private BigDecimal branchId;
	
	@JsonProperty("ORGAN_ID")
	private BigDecimal organId;

	@JsonProperty("POLICY_ID")
	private BigDecimal policyId;
	
	@JsonProperty("POLICY_CHG_ID")
	private BigDecimal policyChgId;
	
	@JsonProperty("POLICY_TYPE")
	private String policyType;
	
	@JsonProperty("BENEFIT_ITEM_ID")
	private BigDecimal benefitItemId;
	
	@JsonProperty("PRODUCT_ID")
	private BigDecimal productId;
	
	@JsonProperty("POLICY_YEAR")
	private BigDecimal policyYear;
	
	@JsonProperty("FEE_TYPE")
	private BigDecimal feeType;
	
	@JsonProperty("HAPPEN_TIME")
	private Long happenTime;
	
	@JsonProperty("DUE_TIME")
	private Long dueTime;
	
	@JsonProperty("CONFIRM_DATE")
	private Long confirmDate;
	
	@JsonProperty("ACCOUNTING_DATE")
	private Long accountingDate;
	
	@JsonProperty("JE_CREATOR_ID")
	private BigDecimal jeCreatorId;
	
	@JsonProperty("JE_POSTING_ID")
	private BigDecimal jePostingId;
	
	@JsonProperty("POSTED")
	private String posted;
	
	@JsonProperty("DR_SEG1")
	private String drSeg1;
	
	@JsonProperty("DR_SEG2")
	private String drSeg2;
	
	@JsonProperty("DR_SEG3")
	private String drSeg3;
	
	@JsonProperty("DR_SEG4")
	private String drSeg4;
	
	@JsonProperty("DR_SEG5")
	private String drSeg5;
	
	@JsonProperty("DR_SEG6")
	private String drSeg6;
	
	@JsonProperty("DR_SEG7")
	private String drSeg7;
	
	@JsonProperty("DR_SEG8")
	private String drSeg8;
	
	@JsonProperty("DR_SEG9")
	private String drSeg9;
	
	@JsonProperty("DR_SEG10")
	private String drSeg10;
	
	@JsonProperty("DR_SEG11")
	private String drSeg11;
	
	@JsonProperty("DR_SEG12")
	private String drSeg12;
	
	@JsonProperty("DR_SEG13")
	private String drSeg13;
	
	@JsonProperty("DR_SEG14")
	private String drSeg14;
	
	@JsonProperty("DR_SEG15")
	private String drSeg15;
	
	@JsonProperty("DR_SEG16")
	private String drSeg16;
	
	@JsonProperty("DR_SEG17")
	private String drSeg17;
	
	@JsonProperty("DR_SEG18")
	private String drSeg18;
	
	@JsonProperty("DR_SEG19")
	private String drSeg19;
	
	@JsonProperty("DR_SEG20")
	private String drSeg20;
	
	@JsonProperty("CR_SEG1")
	private String crSeg1;
	
	@JsonProperty("CR_SEG2")
	private String crSeg2;
	
	@JsonProperty("CR_SEG3")
	private String crSeg3;
	
	@JsonProperty("CR_SEG4")
	private String crSeg4;
	
	@JsonProperty("CR_SEG5")
	private String crSeg5;
	
	@JsonProperty("CR_SEG6")
	private String crSeg6;
	
	@JsonProperty("CR_SEG7")
	private String crSeg7;
	
	@JsonProperty("CR_SEG8")
	private String crSeg8;
	
	@JsonProperty("CR_SEG9")
	private String crSeg9;
	
	@JsonProperty("CR_SEG10")
	private String crSeg10;
	
	@JsonProperty("CR_SEG11")
	private String crSeg11;
	
	@JsonProperty("CR_SEG12")
	private String crSeg12;
	
	@JsonProperty("CR_SEG13")
	private String crSeg13;
	
	@JsonProperty("CR_SEG14")
	private String crSeg14;
	
	@JsonProperty("CR_SEG15")
	private String crSeg15;
	
	@JsonProperty("CR_SEG16")
	private String crSeg16;
	
	@JsonProperty("CR_SEG17")
	private String crSeg17;
	
	@JsonProperty("CR_SEG18")
	private String crSeg18;
	
	@JsonProperty("CR_SEG19")
	private String crSeg19;
	
	@JsonProperty("CR_SEG20")
	private String crSeg20;
	
	@JsonProperty("ADJUST_ITEM_ID")
	private BigDecimal adjustItemId;
	
	@JsonProperty("MONEY_ID")
	private BigDecimal moneyId;
	
	@JsonProperty("AGGREGATION_ID")
	private BigDecimal aggregationId;
	
	@JsonProperty("PRODUCT_VERSION")
	private BigDecimal productVersion;
	
	@JsonProperty("COMMISSION_VERSION")
	private BigDecimal commisionVersion; 
	
	@JsonProperty("POL_CURRENCY_ID")
	private BigDecimal polCurrencyId; 
	
	@JsonProperty("POL_COMMISION")
	private BigDecimal polCommision;
	
	@JsonProperty("CHEQUE_ID")
	private BigDecimal chequeId;
	
	@JsonProperty("DIVISION_INDI")
	private String divisionIndi;
	
	@JsonProperty("REPLY_CODE")
	private String replyCode;
	
	@JsonProperty("TRIGGER_CODE")
	private String triggerCode;
	
	@JsonProperty("PREM_YEAR")
	private String premYear;
	
	@JsonProperty("COMM_STATUS")
	private BigDecimal commStatus;
	
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
	
	@JsonProperty("EXCHANGE_DATE")
	private Long exchangeDate;
	
	@JsonProperty("EXCHANGE_RATE")
	private String exchangeRate;
	
	@JsonProperty("HEAD_AGENT_ID")
	private BigDecimal headAgentId;

	public BigDecimal getFeeId() {
		return feeId;
	}

	public void setFeeId(BigDecimal feeId) {
		this.feeId = feeId;
	}

	public BigDecimal getSourceId() {
		return sourceId;
	}

	public void setSourceId(BigDecimal sourceId) {
		this.sourceId = sourceId;
	}

	public BigDecimal getSourceTable() {
		return sourceTable;
	}

	public void setSourceTable(BigDecimal sourceTable) {
		this.sourceTable = sourceTable;
	}

	public BigDecimal getCommisionTypeId() {
		return commisionTypeId;
	}

	public void setCommisionTypeId(BigDecimal commisionTypeId) {
		this.commisionTypeId = commisionTypeId;
	}

	public BigDecimal getCommision() {
		return commision;
	}

	public void setCommision(BigDecimal commision) {
		this.commision = commision;
	}

	public BigDecimal getAgentId() {
		return agentId;
	}

	public void setAgentId(BigDecimal agentId) {
		this.agentId = agentId;
	}

	public String getAgentCate() {
		return agentCate;
	}

	public void setAgentCate(String agentCate) {
		this.agentCate = agentCate;
	}

	public String getAgentGrade() {
		return agentGrade;
	}

	public void setAgentGrade(String agentGrade) {
		this.agentGrade = agentGrade;
	}

	public BigDecimal getChannelOrgId() {
		return channelOrgId;
	}

	public void setChannelOrgId(BigDecimal channelOrgId) {
		this.channelOrgId = channelOrgId;
	}

	public BigDecimal getHeadId() {
		return headId;
	}

	public void setHeadId(BigDecimal headId) {
		this.headId = headId;
	}

	public BigDecimal getBranchId() {
		return branchId;
	}

	public void setBranchId(BigDecimal branchId) {
		this.branchId = branchId;
	}

	public BigDecimal getOrganId() {
		return organId;
	}

	public void setOrganId(BigDecimal organId) {
		this.organId = organId;
	}

	public BigDecimal getPolicyId() {
		return policyId;
	}

	public void setPolicyId(BigDecimal policyId) {
		this.policyId = policyId;
	}

	public BigDecimal getPolicyChgId() {
		return policyChgId;
	}

	public void setPolicyChgId(BigDecimal policyChgId) {
		this.policyChgId = policyChgId;
	}

	public String getPolicyType() {
		return policyType;
	}

	public void setPolicyType(String policyType) {
		this.policyType = policyType;
	}

	public BigDecimal getBenefitItemId() {
		return benefitItemId;
	}

	public void setBenefitItemId(BigDecimal benefitItemId) {
		this.benefitItemId = benefitItemId;
	}

	public BigDecimal getProductId() {
		return productId;
	}

	public void setProductId(BigDecimal productId) {
		this.productId = productId;
	}

	public BigDecimal getPolicyYear() {
		return policyYear;
	}

	public void setPolicyYear(BigDecimal policyYear) {
		this.policyYear = policyYear;
	}

	public BigDecimal getFeeType() {
		return feeType;
	}

	public void setFeeType(BigDecimal feeType) {
		this.feeType = feeType;
	}

	public Long getHappenTime() {
		return happenTime;
	}

	public void setHappenTime(Long happenTime) {
		this.happenTime = happenTime;
	}

	public Long getDueTime() {
		return dueTime;
	}

	public void setDueTime(Long dueTime) {
		this.dueTime = dueTime;
	}

	public Long getConfirmDate() {
		return confirmDate;
	}

	public void setConfirmDate(Long confirmDate) {
		this.confirmDate = confirmDate;
	}

	public Long getAccountingDate() {
		return accountingDate;
	}

	public void setAccountingDate(Long accountingDate) {
		this.accountingDate = accountingDate;
	}

	public BigDecimal getJeCreatorId() {
		return jeCreatorId;
	}

	public void setJeCreatorId(BigDecimal jeCreatorId) {
		this.jeCreatorId = jeCreatorId;
	}

	public BigDecimal getJePostingId() {
		return jePostingId;
	}

	public void setJePostingId(BigDecimal jePostingId) {
		this.jePostingId = jePostingId;
	}

	public String getPosted() {
		return posted;
	}

	public void setPosted(String posted) {
		this.posted = posted;
	}

	public String getDrSeg1() {
		return drSeg1;
	}

	public void setDrSeg1(String drSeg1) {
		this.drSeg1 = drSeg1;
	}

	public String getDrSeg2() {
		return drSeg2;
	}

	public void setDrSeg2(String drSeg2) {
		this.drSeg2 = drSeg2;
	}

	public String getDrSeg3() {
		return drSeg3;
	}

	public void setDrSeg3(String drSeg3) {
		this.drSeg3 = drSeg3;
	}

	public String getDrSeg4() {
		return drSeg4;
	}

	public void setDrSeg4(String drSeg4) {
		this.drSeg4 = drSeg4;
	}

	public String getDrSeg5() {
		return drSeg5;
	}

	public void setDrSeg5(String drSeg5) {
		this.drSeg5 = drSeg5;
	}

	public String getDrSeg6() {
		return drSeg6;
	}

	public void setDrSeg6(String drSeg6) {
		this.drSeg6 = drSeg6;
	}

	public String getDrSeg7() {
		return drSeg7;
	}

	public void setDrSeg7(String drSeg7) {
		this.drSeg7 = drSeg7;
	}

	public String getDrSeg8() {
		return drSeg8;
	}

	public void setDrSeg8(String drSeg8) {
		this.drSeg8 = drSeg8;
	}

	public String getDrSeg9() {
		return drSeg9;
	}

	public void setDrSeg9(String drSeg9) {
		this.drSeg9 = drSeg9;
	}

	public String getDrSeg10() {
		return drSeg10;
	}

	public void setDrSeg10(String drSeg10) {
		this.drSeg10 = drSeg10;
	}

	public String getDrSeg11() {
		return drSeg11;
	}

	public void setDrSeg11(String drSeg11) {
		this.drSeg11 = drSeg11;
	}

	public String getDrSeg12() {
		return drSeg12;
	}

	public void setDrSeg12(String drSeg12) {
		this.drSeg12 = drSeg12;
	}

	public String getDrSeg13() {
		return drSeg13;
	}

	public void setDrSeg13(String drSeg13) {
		this.drSeg13 = drSeg13;
	}

	public String getDrSeg14() {
		return drSeg14;
	}

	public void setDrSeg14(String drSeg14) {
		this.drSeg14 = drSeg14;
	}

	public String getDrSeg15() {
		return drSeg15;
	}

	public void setDrSeg15(String drSeg15) {
		this.drSeg15 = drSeg15;
	}

	public String getDrSeg16() {
		return drSeg16;
	}

	public void setDrSeg16(String drSeg16) {
		this.drSeg16 = drSeg16;
	}

	public String getDrSeg17() {
		return drSeg17;
	}

	public void setDrSeg17(String drSeg17) {
		this.drSeg17 = drSeg17;
	}

	public String getDrSeg18() {
		return drSeg18;
	}

	public void setDrSeg18(String drSeg18) {
		this.drSeg18 = drSeg18;
	}

	public String getDrSeg19() {
		return drSeg19;
	}

	public void setDrSeg19(String drSeg19) {
		this.drSeg19 = drSeg19;
	}

	public String getDrSeg20() {
		return drSeg20;
	}

	public void setDrSeg20(String drSeg20) {
		this.drSeg20 = drSeg20;
	}

	public String getCrSeg1() {
		return crSeg1;
	}

	public void setCrSeg1(String crSeg1) {
		this.crSeg1 = crSeg1;
	}

	public String getCrSeg2() {
		return crSeg2;
	}

	public void setCrSeg2(String crSeg2) {
		this.crSeg2 = crSeg2;
	}

	public String getCrSeg3() {
		return crSeg3;
	}

	public void setCrSeg3(String crSeg3) {
		this.crSeg3 = crSeg3;
	}

	public String getCrSeg4() {
		return crSeg4;
	}

	public void setCrSeg4(String crSeg4) {
		this.crSeg4 = crSeg4;
	}

	public String getCrSeg5() {
		return crSeg5;
	}

	public void setCrSeg5(String crSeg5) {
		this.crSeg5 = crSeg5;
	}

	public String getCrSeg6() {
		return crSeg6;
	}

	public void setCrSeg6(String crSeg6) {
		this.crSeg6 = crSeg6;
	}

	public String getCrSeg7() {
		return crSeg7;
	}

	public void setCrSeg7(String crSeg7) {
		this.crSeg7 = crSeg7;
	}

	public String getCrSeg8() {
		return crSeg8;
	}

	public void setCrSeg8(String crSeg8) {
		this.crSeg8 = crSeg8;
	}

	public String getCrSeg9() {
		return crSeg9;
	}

	public void setCrSeg9(String crSeg9) {
		this.crSeg9 = crSeg9;
	}

	public String getCrSeg10() {
		return crSeg10;
	}

	public void setCrSeg10(String crSeg10) {
		this.crSeg10 = crSeg10;
	}

	public String getCrSeg11() {
		return crSeg11;
	}

	public void setCrSeg11(String crSeg11) {
		this.crSeg11 = crSeg11;
	}

	public String getCrSeg12() {
		return crSeg12;
	}

	public void setCrSeg12(String crSeg12) {
		this.crSeg12 = crSeg12;
	}

	public String getCrSeg13() {
		return crSeg13;
	}

	public void setCrSeg13(String crSeg13) {
		this.crSeg13 = crSeg13;
	}

	public String getCrSeg14() {
		return crSeg14;
	}

	public void setCrSeg14(String crSeg14) {
		this.crSeg14 = crSeg14;
	}

	public String getCrSeg15() {
		return crSeg15;
	}

	public void setCrSeg15(String crSeg15) {
		this.crSeg15 = crSeg15;
	}

	public String getCrSeg16() {
		return crSeg16;
	}

	public void setCrSeg16(String crSeg16) {
		this.crSeg16 = crSeg16;
	}

	public String getCrSeg17() {
		return crSeg17;
	}

	public void setCrSeg17(String crSeg17) {
		this.crSeg17 = crSeg17;
	}

	public String getCrSeg18() {
		return crSeg18;
	}

	public void setCrSeg18(String crSeg18) {
		this.crSeg18 = crSeg18;
	}

	public String getCrSeg19() {
		return crSeg19;
	}

	public void setCrSeg19(String crSeg19) {
		this.crSeg19 = crSeg19;
	}

	public String getCrSeg20() {
		return crSeg20;
	}

	public void setCrSeg20(String crSeg20) {
		this.crSeg20 = crSeg20;
	}

	public BigDecimal getAdjustItemId() {
		return adjustItemId;
	}

	public void setAdjustItemId(BigDecimal adjustItemId) {
		this.adjustItemId = adjustItemId;
	}

	public BigDecimal getMoneyId() {
		return moneyId;
	}

	public void setMoneyId(BigDecimal moneyId) {
		this.moneyId = moneyId;
	}

	public BigDecimal getAggregationId() {
		return aggregationId;
	}

	public void setAggregationId(BigDecimal aggregationId) {
		this.aggregationId = aggregationId;
	}

	public BigDecimal getProductVersion() {
		return productVersion;
	}

	public void setProductVersion(BigDecimal productVersion) {
		this.productVersion = productVersion;
	}

	public BigDecimal getCommisionVersion() {
		return commisionVersion;
	}

	public void setCommisionVersion(BigDecimal commisionVersion) {
		this.commisionVersion = commisionVersion;
	}

	public BigDecimal getPolCurrencyId() {
		return polCurrencyId;
	}

	public void setPolCurrencyId(BigDecimal polCurrencyId) {
		this.polCurrencyId = polCurrencyId;
	}

	public BigDecimal getPolCommision() {
		return polCommision;
	}

	public void setPolCommision(BigDecimal polCommision) {
		this.polCommision = polCommision;
	}

	public BigDecimal getChequeId() {
		return chequeId;
	}

	public void setChequeId(BigDecimal chequeId) {
		this.chequeId = chequeId;
	}

	public String getDivisionIndi() {
		return divisionIndi;
	}

	public void setDivisionIndi(String divisionIndi) {
		this.divisionIndi = divisionIndi;
	}

	public String getReplyCode() {
		return replyCode;
	}

	public void setReplyCode(String replyCode) {
		this.replyCode = replyCode;
	}

	public String getTriggerCode() {
		return triggerCode;
	}

	public void setTriggerCode(String triggerCode) {
		this.triggerCode = triggerCode;
	}

	public String getPremYear() {
		return premYear;
	}

	public void setPremYear(String premYear) {
		this.premYear = premYear;
	}

	public BigDecimal getCommStatus() {
		return commStatus;
	}

	public void setCommStatus(BigDecimal commStatus) {
		this.commStatus = commStatus;
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

	public Long getExchangeDate() {
		return exchangeDate;
	}

	public void setExchangeDate(Long exchangeDate) {
		this.exchangeDate = exchangeDate;
	}

	public String getExchangeRate() {
		return exchangeRate;
	}

	public void setExchangeRate(String exchangeRate) {
		this.exchangeRate = exchangeRate;
	}

	public BigDecimal getHeadAgentId() {
		return headAgentId;
	}

	public void setHeadAgentId(BigDecimal headAgentId) {
		this.headAgentId = headAgentId;
	}
}
