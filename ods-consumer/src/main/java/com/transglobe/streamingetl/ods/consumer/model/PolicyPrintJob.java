package com.transglobe.streamingetl.ods.consumer.model;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * properties based on ODS.O_PRODUCTION_DETAIL
 * Json Property properties based on EBAO.T_PRODUCTION_DETAIL
 * @author steven
 *
 */
public class PolicyPrintJob {

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
	private Long printDateMilli;
	
	@JsonProperty("OPERATOR_ID")
	private BigDecimal operatorId;
	
	@JsonProperty("INSERT_TIME")
	private Long insertTimeMillis;
	
	@JsonProperty("INSERTED_BY")
	private BigDecimal insertedBy;
	
	@JsonProperty("INSERT_TIMESTAMP")
	private Long insertTimestampMillis;
	
	@JsonProperty("UPDATED_BY")
	private BigDecimal updatedBy;
	
	@JsonProperty("UPDATE_TIMESTAMP")
	private Long upateTimestampMillis;
	
	@JsonProperty("PREMVOUCHER_INDI")
	private String preMvoucherIndi;
	
	@JsonProperty("ARCHIVE_ID")
	private BigDecimal archiveId;
	
	@JsonProperty("UPDATE_TIME")
	private Long updateTimeMillis;
	
	@JsonProperty("JOB_TYPE_DESC")
	private String jobTypeDesc;
	
	@JsonProperty("JOB_READY_DATE")
	private Long jobReadyDateMillis;
	
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

	public Long getPrintDateMilli() {
		return printDateMilli;
	}

	public void setPrintDateMilli(Long printDateMilli) {
		this.printDateMilli = printDateMilli;
	}

	public BigDecimal getOperatorId() {
		return operatorId;
	}

	public void setOperatorId(BigDecimal operatorId) {
		this.operatorId = operatorId;
	}

	public Long getInsertTimeMillis() {
		return insertTimeMillis;
	}

	public void setInsertTimeMillis(Long insertTimeMillis) {
		this.insertTimeMillis = insertTimeMillis;
	}

	public BigDecimal getInsertedBy() {
		return insertedBy;
	}

	public void setInsertedBy(BigDecimal insertedBy) {
		this.insertedBy = insertedBy;
	}

	public Long getInsertTimestampMillis() {
		return insertTimestampMillis;
	}

	public void setInsertTimestampMillis(Long insertTimestampMillis) {
		this.insertTimestampMillis = insertTimestampMillis;
	}

	public BigDecimal getUpdatedBy() {
		return updatedBy;
	}

	public void setUpdatedBy(BigDecimal updatedBy) {
		this.updatedBy = updatedBy;
	}

	public Long getUpateTimestampMillis() {
		return upateTimestampMillis;
	}

	public void setUpateTimestampMillis(Long upateTimestampMillis) {
		this.upateTimestampMillis = upateTimestampMillis;
	}

	public String getPreMvoucherIndi() {
		return preMvoucherIndi;
	}

	public void setPreMvoucherIndi(String preMvoucherIndi) {
		this.preMvoucherIndi = preMvoucherIndi;
	}

	public BigDecimal getArchiveId() {
		return archiveId;
	}

	public void setArchiveId(BigDecimal archiveId) {
		this.archiveId = archiveId;
	}

	public Long getUpdateTimeMillis() {
		return updateTimeMillis;
	}

	public void setUpdateTimeMillis(Long updateTimeMillis) {
		this.updateTimeMillis = updateTimeMillis;
	}

	public String getJobTypeDesc() {
		return jobTypeDesc;
	}

	public void setJobTypeDesc(String jobTypeDesc) {
		this.jobTypeDesc = jobTypeDesc;
	}

	public Long getJobReadyDateMillis() {
		return jobReadyDateMillis;
	}

	public void setJobReadyDateMillis(Long jobReadyDateMillis) {
		this.jobReadyDateMillis = jobReadyDateMillis;
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
