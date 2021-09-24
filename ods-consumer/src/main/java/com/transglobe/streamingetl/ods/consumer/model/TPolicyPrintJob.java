package com.transglobe.streamingetl.ods.consumer.model;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Json Property properties based on EBAO.T_POLICY_PRINT_JOB
 * Using Long for DATE, TIMESTAMP type. CLOB to String type
 *
 */
public class TPolicyPrintJob {
	
	@JsonProperty("JOB_ID")
	private BigDecimal jobId;

	@JsonProperty("POLICY_ID")
	private BigDecimal policyId;

	@JsonProperty("PAYCARD_INDI")
	private String paycardIndi;

	@JsonProperty("LETTER_INDI")
	private String letterIndi;

	@JsonProperty("ACKLETTER_INDI")
	private String ackletterIndi;

	@JsonProperty("PIS_INDI")
	private String pisIndi;

	@JsonProperty("HEALTH_CARD_INDI")
	private String healthCardIndi;

	@JsonProperty("COVER_INDI")
	private String coverIndi;

	@JsonProperty("SCHEDULE_INDI")
	private String scheduleIndi;

	@JsonProperty("CLAUSE_INDI")
	private String clauseIndi;

	@JsonProperty("ANNEXURE_INDI")
	private String annexureIndi;

	@JsonProperty("ENDORSE_INDI")
	private String endorseIndi;

	@JsonProperty("EXCLUSION_INDI")
	private String exclusionIndi;

	@JsonProperty("PRINT_CATEGORY")
	private BigDecimal printCategory;

	@JsonProperty("PRINT_TYPE")
	private BigDecimal printType;

	@JsonProperty("PRINT_COPYSET")
	private String printCopyset;

	@JsonProperty("PRINT_REASON")
	private String printReason;

	@JsonProperty("VALID_STATUS")
	private String validStatus;

	@JsonProperty("PRINT_DATE")
	private Long printDate;

	@JsonProperty("OPERATOR_ID")
	private BigDecimal operatorId;

	@JsonProperty("INSERT_TIME")
	private Long insertTime;

	@JsonProperty("INSERTED_BY")
	private BigDecimal insertedBy;

	@JsonProperty("INSERT_TIMESTAMP")
	private Long insertTimestamp;

	@JsonProperty("UPDATED_BY")
	private BigDecimal updatedBy;

	@JsonProperty("UPDATE_TIMESTAMP")
	private Long updateTimestamp;

	@JsonProperty("PREMVOUCHER_INDI")
	private String premvoucherIndi;

	@JsonProperty("ARCHIVE_ID")
	private BigDecimal archiveId;

	@JsonProperty("UPDATE_TIME")
	private Long updateTime;

	@JsonProperty("JOB_TYPE_DESC")
	private String jobTypeDesc;

	@JsonProperty("JOB_READY_DATE")
	private Long jobReadyDate;

	@JsonProperty("CONTENT")
	private String content;

	@JsonProperty("COPY")
	private BigDecimal copy;

	@JsonProperty("ERROR_CODE")
	private BigDecimal errorCode;

	@JsonProperty("LANG_ID")
	private String langId;

	@JsonProperty("CHANGE_ID")
	private BigDecimal changeId;

	@JsonProperty("PRINT_COMP_INDI")
	private BigDecimal printCompIndi;

	@JsonProperty("REMAKE_ID")
	private BigDecimal remakeId;

	@JsonProperty("CHECK_ID")
	private BigDecimal checkId;

	@JsonProperty("EVA_LIST_ID")
	private BigDecimal evaListId;

	@JsonProperty("URGENT_INDI")
	private String urgentIndi;

	public BigDecimal getJobId() {
		return jobId;
	}

	public void setJobId(BigDecimal jobId) {
		this.jobId = jobId;
	}

	public BigDecimal getPolicyId() {
		return policyId;
	}

	public void setPolicyId(BigDecimal policyId) {
		this.policyId = policyId;
	}

	public String getPaycardIndi() {
		return paycardIndi;
	}

	public void setPaycardIndi(String paycardIndi) {
		this.paycardIndi = paycardIndi;
	}

	public String getLetterIndi() {
		return letterIndi;
	}

	public void setLetterIndi(String letterIndi) {
		this.letterIndi = letterIndi;
	}

	public String getAckletterIndi() {
		return ackletterIndi;
	}

	public void setAckletterIndi(String ackletterIndi) {
		this.ackletterIndi = ackletterIndi;
	}

	public String getPisIndi() {
		return pisIndi;
	}

	public void setPisIndi(String pisIndi) {
		this.pisIndi = pisIndi;
	}

	public String getHealthCardIndi() {
		return healthCardIndi;
	}

	public void setHealthCardIndi(String healthCardIndi) {
		this.healthCardIndi = healthCardIndi;
	}

	public String getCoverIndi() {
		return coverIndi;
	}

	public void setCoverIndi(String coverIndi) {
		this.coverIndi = coverIndi;
	}

	public String getScheduleIndi() {
		return scheduleIndi;
	}

	public void setScheduleIndi(String scheduleIndi) {
		this.scheduleIndi = scheduleIndi;
	}

	public String getClauseIndi() {
		return clauseIndi;
	}

	public void setClauseIndi(String clauseIndi) {
		this.clauseIndi = clauseIndi;
	}

	public String getAnnexureIndi() {
		return annexureIndi;
	}

	public void setAnnexureIndi(String annexureIndi) {
		this.annexureIndi = annexureIndi;
	}

	public String getEndorseIndi() {
		return endorseIndi;
	}

	public void setEndorseIndi(String endorseIndi) {
		this.endorseIndi = endorseIndi;
	}

	public String getExclusionIndi() {
		return exclusionIndi;
	}

	public void setExclusionIndi(String exclusionIndi) {
		this.exclusionIndi = exclusionIndi;
	}

	public BigDecimal getPrintCategory() {
		return printCategory;
	}

	public void setPrintCategory(BigDecimal printCategory) {
		this.printCategory = printCategory;
	}

	public BigDecimal getPrintType() {
		return printType;
	}

	public void setPrintType(BigDecimal printType) {
		this.printType = printType;
	}

	public String getPrintCopyset() {
		return printCopyset;
	}

	public void setPrintCopyset(String printCopyset) {
		this.printCopyset = printCopyset;
	}

	public String getPrintReason() {
		return printReason;
	}

	public void setPrintReason(String printReason) {
		this.printReason = printReason;
	}

	public String getValidStatus() {
		return validStatus;
	}

	public void setValidStatus(String validStatus) {
		this.validStatus = validStatus;
	}

	public Long getPrintDate() {
		return printDate;
	}

	public void setPrintDate(Long printDate) {
		this.printDate = printDate;
	}

	public BigDecimal getOperatorId() {
		return operatorId;
	}

	public void setOperatorId(BigDecimal operatorId) {
		this.operatorId = operatorId;
	}

	public Long getInsertTime() {
		return insertTime;
	}

	public void setInsertTime(Long insertTime) {
		this.insertTime = insertTime;
	}

	public BigDecimal getInsertedBy() {
		return insertedBy;
	}

	public void setInsertedBy(BigDecimal insertedBy) {
		this.insertedBy = insertedBy;
	}

	public Long getInsertTimestamp() {
		return insertTimestamp;
	}

	public void setInsertTimestamp(Long insertTimestamp) {
		this.insertTimestamp = insertTimestamp;
	}

	public BigDecimal getUpdatedBy() {
		return updatedBy;
	}

	public void setUpdatedBy(BigDecimal updatedBy) {
		this.updatedBy = updatedBy;
	}

	public Long getUpdateTimestamp() {
		return updateTimestamp;
	}

	public void setUpdateTimestamp(Long updateTimestamp) {
		this.updateTimestamp = updateTimestamp;
	}

	public String getPremvoucherIndi() {
		return premvoucherIndi;
	}

	public void setPremvoucherIndi(String premvoucherIndi) {
		this.premvoucherIndi = premvoucherIndi;
	}

	public BigDecimal getArchiveId() {
		return archiveId;
	}

	public void setArchiveId(BigDecimal archiveId) {
		this.archiveId = archiveId;
	}

	public Long getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Long updateTime) {
		this.updateTime = updateTime;
	}

	public String getJobTypeDesc() {
		return jobTypeDesc;
	}

	public void setJobTypeDesc(String jobTypeDesc) {
		this.jobTypeDesc = jobTypeDesc;
	}

	public Long getJobReadyDate() {
		return jobReadyDate;
	}

	public void setJobReadyDate(Long jobReadyDate) {
		this.jobReadyDate = jobReadyDate;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public BigDecimal getCopy() {
		return copy;
	}

	public void setCopy(BigDecimal copy) {
		this.copy = copy;
	}

	public BigDecimal getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(BigDecimal errorCode) {
		this.errorCode = errorCode;
	}

	public String getLangId() {
		return langId;
	}

	public void setLangId(String langId) {
		this.langId = langId;
	}

	public BigDecimal getChangeId() {
		return changeId;
	}

	public void setChangeId(BigDecimal changeId) {
		this.changeId = changeId;
	}

	public BigDecimal getPrintCompIndi() {
		return printCompIndi;
	}

	public void setPrintCompIndi(BigDecimal printCompIndi) {
		this.printCompIndi = printCompIndi;
	}

	public BigDecimal getRemakeId() {
		return remakeId;
	}

	public void setRemakeId(BigDecimal remakeId) {
		this.remakeId = remakeId;
	}

	public BigDecimal getCheckId() {
		return checkId;
	}

	public void setCheckId(BigDecimal checkId) {
		this.checkId = checkId;
	}

	public BigDecimal getEvaListId() {
		return evaListId;
	}

	public void setEvaListId(BigDecimal evaListId) {
		this.evaListId = evaListId;
	}

	public String getUrgentIndi() {
		return urgentIndi;
	}

	public void setUrgentIndi(String urgentIndi) {
		this.urgentIndi = urgentIndi;
	}
	
	
}
