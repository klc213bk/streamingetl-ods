package com.transglobe.streamingetl.ods.consumer.model;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Json Property properties based on EBAO.T_ContractExtendLog
 * Using Long for DATE type.
 *
 */
public class TContractExtendLog {

	@JsonProperty("CHANGE_ID")
	private BigDecimal changeId;
	
	@JsonProperty("LOG_TYPE")
	private String logType;
	
	@JsonProperty("POLICY_CHG_ID")
	private BigDecimal policyChgId;
	
	@JsonProperty("ITEM_ID")
	private BigDecimal itemId;
	
	@JsonProperty("DUE_DATE")
	private Long dueDate;
	
	@JsonProperty("POLICY_YEAR")
	private BigDecimal policyYear;
	
	@JsonProperty("POLICY_PERIOD")
	private BigDecimal policyPeriod;
	
	@JsonProperty("STRGY_DUE_DATE")
	private Long strgyDueDate;
	
	@JsonProperty("PREM_STATUS")
	private BigDecimal premStatus;
	
	@JsonProperty("LOG_ID")
	private BigDecimal logId;
	
	@JsonProperty("SA_DUE_DATE")
	private Long saDueDate;
	
	@JsonProperty("LAST_CMT_FLG")
	private String lastCmtFlg;
	
	@JsonProperty("EMS_VERSION")
	private BigDecimal emaVersion;
	
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
	
	@JsonProperty("POLICY_ID")
	private BigDecimal policyId;
	
	@JsonProperty("BILLING_DATE")
	private Long billingDate;
	
	@JsonProperty("REMINDER_DATE")
	private Long reminderDate;
	
	@JsonProperty("INDX_DUE_DATE")
	private Long indxDueDate;
	
	@JsonProperty("INDX_REJECT")
	private BigDecimal indxReject;
	
	@JsonProperty("INSURABILITY_DUE_DATE")
	private Long insurabilityDueDate;
	
	@JsonProperty("INSURABILITY_REJECT_COUNT")
	private BigDecimal insurabilityRejectCount;
	
	@JsonProperty("INSURABILITY_REJECT_REASON")
	private String insurabilityRejectReason;
	
	@JsonProperty("BILL_TO_DATE")
	private Long billToDate;
	
	@JsonProperty("BUCKET_FILLING_DUE_DATE")
	private Long bucketFillingDueDate;
	
	@JsonProperty("ILP_DUE_DATE")
	private Long ilpDueDate;
	
	@JsonProperty("WAIVER_SOURCE")
	private BigDecimal waiverSource;

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

	public Long getDueDate() {
		return dueDate;
	}

	public void setDueDate(Long dueDate) {
		this.dueDate = dueDate;
	}

	public BigDecimal getPolicyYear() {
		return policyYear;
	}

	public void setPolicyYear(BigDecimal policyYear) {
		this.policyYear = policyYear;
	}

	public BigDecimal getPolicyPeriod() {
		return policyPeriod;
	}

	public void setPolicyPeriod(BigDecimal policyPeriod) {
		this.policyPeriod = policyPeriod;
	}

	public Long getStrgyDueDate() {
		return strgyDueDate;
	}

	public void setStrgyDueDate(Long strgyDueDate) {
		this.strgyDueDate = strgyDueDate;
	}

	public BigDecimal getPremStatus() {
		return premStatus;
	}

	public void setPremStatus(BigDecimal premStatus) {
		this.premStatus = premStatus;
	}

	public BigDecimal getLogId() {
		return logId;
	}

	public void setLogId(BigDecimal logId) {
		this.logId = logId;
	}

	public Long getSaDueDate() {
		return saDueDate;
	}

	public void setSaDueDate(Long saDueDate) {
		this.saDueDate = saDueDate;
	}

	public String getLastCmtFlg() {
		return lastCmtFlg;
	}

	public void setLastCmtFlg(String lastCmtFlg) {
		this.lastCmtFlg = lastCmtFlg;
	}

	public BigDecimal getEmaVersion() {
		return emaVersion;
	}

	public void setEmaVersion(BigDecimal emaVersion) {
		this.emaVersion = emaVersion;
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

	public BigDecimal getPolicyId() {
		return policyId;
	}

	public void setPolicyId(BigDecimal policyId) {
		this.policyId = policyId;
	}

	public Long getBillingDate() {
		return billingDate;
	}

	public void setBillingDate(Long billingDate) {
		this.billingDate = billingDate;
	}

	public Long getReminderDate() {
		return reminderDate;
	}

	public void setReminderDate(Long reminderDate) {
		this.reminderDate = reminderDate;
	}

	public Long getIndxDueDate() {
		return indxDueDate;
	}

	public void setIndxDueDate(Long indxDueDate) {
		this.indxDueDate = indxDueDate;
	}

	public BigDecimal getIndxReject() {
		return indxReject;
	}

	public void setIndxReject(BigDecimal indxReject) {
		this.indxReject = indxReject;
	}

	public Long getInsurabilityDueDate() {
		return insurabilityDueDate;
	}

	public void setInsurabilityDueDate(Long insurabilityDueDate) {
		this.insurabilityDueDate = insurabilityDueDate;
	}

	public BigDecimal getInsurabilityRejectCount() {
		return insurabilityRejectCount;
	}

	public void setInsurabilityRejectCount(BigDecimal insurabilityRejectCount) {
		this.insurabilityRejectCount = insurabilityRejectCount;
	}

	public String getInsurabilityRejectReason() {
		return insurabilityRejectReason;
	}

	public void setInsurabilityRejectReason(String insurabilityRejectReason) {
		this.insurabilityRejectReason = insurabilityRejectReason;
	}

	public Long getBillToDate() {
		return billToDate;
	}

	public void setBillToDate(Long billToDate) {
		this.billToDate = billToDate;
	}

	public Long getBucketFillingDueDate() {
		return bucketFillingDueDate;
	}

	public void setBucketFillingDueDate(Long bucketFillingDueDate) {
		this.bucketFillingDueDate = bucketFillingDueDate;
	}

	public Long getIlpDueDate() {
		return ilpDueDate;
	}

	public void setIlpDueDate(Long ilpDueDate) {
		this.ilpDueDate = ilpDueDate;
	}

	public BigDecimal getWaiverSource() {
		return waiverSource;
	}

	public void setWaiverSource(BigDecimal waiverSource) {
		this.waiverSource = waiverSource;
	}
	
	
}
