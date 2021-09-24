package com.transglobe.streamingetl.ods.consumer.model;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Json Property properties based on EBAO.T_POLICY_CHANGE
 * Using Long for DATE, TIMESTAMP type.
 *
 */
public class TPolicyChange {
	@JsonProperty("POLICY_CHG_ID")
	private BigDecimal policyChgId;

	@JsonProperty("POLICY_ID")
	private BigDecimal policyId;

	@JsonProperty("SERVICE_ID")
	private BigDecimal serviceId;

	@JsonProperty("CHANGE_RECORD")
	private String changeRecord;

	@JsonProperty("INSERT_TIME")
	private Long insertTime;

	@JsonProperty("VALIDATE_TIME")
	private Long validateTime;

	@JsonProperty("NEED_UNDERWRITE")
	private String needUnderwrite;

	@JsonProperty("APPLY_TIME")
	private Long applyTime;

	@JsonProperty("CHANGE_CAUSE")
	private String changeCause;

	@JsonProperty("CANCEL_ID")
	private BigDecimal cancelId;

	@JsonProperty("CANCEL_TIME")
	private Long cancelTime;

	@JsonProperty("REJECTER_ID")
	private BigDecimal rejecterId;

	@JsonProperty("REJECT_TIME")
	private Long rejectTime;

	@JsonProperty("REJECT_NOTE")
	private String rejectNote;

	@JsonProperty("UPDATE_TIME")
	private Long updateTime;

	@JsonProperty("MASTER_CHG_ID")
	private BigDecimal masterChgId;

	@JsonProperty("CANCEL_CAUSE")
	private String cancelCause;

	@JsonProperty("CANCEL_NOTE")
	private String cancelNote;

	@JsonProperty("REJECT_CAUSE")
	private String rejectCause;

	@JsonProperty("LAST_HANDLER_ID")
	private BigDecimal lastHandlerId;

	@JsonProperty("LAST_ENTRY_TIME")
	private Long lastEntryTime;

	@JsonProperty("LAST_UW_DISP_ID")
	private BigDecimal lastUwDispId;

	@JsonProperty("LAST_UW_DISP_TIME")
	private Long lastUwDispTime;

	@JsonProperty("ORDER_ID")
	private BigDecimal orderId;

	@JsonProperty("POLICY_CHG_STATUS")
	private BigDecimal policyChgStatus;

	@JsonProperty("DISPATCH_EMP")
	private BigDecimal dispatchEmp;

	@JsonProperty("LETTER_EFFECT_TYPE")
	private String letterEffectType;

	@JsonProperty("DISPATCH_TYPE")
	private String dispatchType;

	@JsonProperty("DISPATCH_LETTER")
	private String dispatchLetter;

	@JsonProperty("SUB_SERVICE_ID")
	private BigDecimal subServiceId;

	@JsonProperty("CHANGE_NOTE")
	private String changeNote;

	@JsonProperty("INSERTED_BY")
	private BigDecimal insertedBy;

	@JsonProperty("INSERT_TIMESTAMP")
	private Long insertTimestamp;

	@JsonProperty("UPDATED_BY")
	private BigDecimal updatedBy;

	@JsonProperty("UPDATE_TIMESTAMP")
	private Long updateTimestamp;

	@JsonProperty("PRE_POLICY_CHG")
	private BigDecimal prePolicyChg;

	@JsonProperty("CHANGE_SEQ")
	private BigDecimal changeSeq;

	@JsonProperty("FINISH_TIME")
	private Long finishTime;

	@JsonProperty("ORG_ID")
	private BigDecimal orgId;

	@JsonProperty("REQUEST_EFFECT_DATE")
	private Long requestEffectDate;

	@JsonProperty("TASK_DUE_DATE")
	private Long taskDueDate;

	@JsonProperty("ALTERATION_INFO_ID")
	private BigDecimal alternationInfoId;

	@JsonProperty("POLICY_CHG_ORDER_SEQ")
	private BigDecimal policyChgOrderSeq;

	public BigDecimal getPolicyChgId() {
		return policyChgId;
	}

	public void setPolicyChgId(BigDecimal policyChgId) {
		this.policyChgId = policyChgId;
	}

	public BigDecimal getPolicyId() {
		return policyId;
	}

	public void setPolicyId(BigDecimal policyId) {
		this.policyId = policyId;
	}

	public BigDecimal getServiceId() {
		return serviceId;
	}

	public void setServiceId(BigDecimal serviceId) {
		this.serviceId = serviceId;
	}

	public String getChangeRecord() {
		return changeRecord;
	}

	public void setChangeRecord(String changeRecord) {
		this.changeRecord = changeRecord;
	}

	public Long getInsertTime() {
		return insertTime;
	}

	public void setInsertTime(Long insertTime) {
		this.insertTime = insertTime;
	}

	public Long getValidateTime() {
		return validateTime;
	}

	public void setValidateTime(Long validateTime) {
		this.validateTime = validateTime;
	}

	public String getNeedUnderwrite() {
		return needUnderwrite;
	}

	public void setNeedUnderwrite(String needUnderwrite) {
		this.needUnderwrite = needUnderwrite;
	}

	public Long getApplyTime() {
		return applyTime;
	}

	public void setApplyTime(Long applyTime) {
		this.applyTime = applyTime;
	}

	public String getChangeCause() {
		return changeCause;
	}

	public void setChangeCause(String changeCause) {
		this.changeCause = changeCause;
	}

	public BigDecimal getCancelId() {
		return cancelId;
	}

	public void setCancelId(BigDecimal cancelId) {
		this.cancelId = cancelId;
	}

	public Long getCancelTime() {
		return cancelTime;
	}

	public void setCancelTime(Long cancelTime) {
		this.cancelTime = cancelTime;
	}

	public BigDecimal getRejecterId() {
		return rejecterId;
	}

	public void setRejecterId(BigDecimal rejecterId) {
		this.rejecterId = rejecterId;
	}

	public Long getRejectTime() {
		return rejectTime;
	}

	public void setRejectTime(Long rejectTime) {
		this.rejectTime = rejectTime;
	}

	public String getRejectNote() {
		return rejectNote;
	}

	public void setRejectNote(String rejectNote) {
		this.rejectNote = rejectNote;
	}

	public Long getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Long updateTime) {
		this.updateTime = updateTime;
	}

	public BigDecimal getMasterChgId() {
		return masterChgId;
	}

	public void setMasterChgId(BigDecimal masterChgId) {
		this.masterChgId = masterChgId;
	}

	public String getCancelCause() {
		return cancelCause;
	}

	public void setCancelCause(String cancelCause) {
		this.cancelCause = cancelCause;
	}

	public String getCancelNote() {
		return cancelNote;
	}

	public void setCancelNote(String cancelNote) {
		this.cancelNote = cancelNote;
	}

	public String getRejectCause() {
		return rejectCause;
	}

	public void setRejectCause(String rejectCause) {
		this.rejectCause = rejectCause;
	}

	public BigDecimal getLastHandlerId() {
		return lastHandlerId;
	}

	public void setLastHandlerId(BigDecimal lastHandlerId) {
		this.lastHandlerId = lastHandlerId;
	}

	public Long getLastEntryTime() {
		return lastEntryTime;
	}

	public void setLastEntryTime(Long lastEntryTime) {
		this.lastEntryTime = lastEntryTime;
	}

	public BigDecimal getLastUwDispId() {
		return lastUwDispId;
	}

	public void setLastUwDispId(BigDecimal lastUwDispId) {
		this.lastUwDispId = lastUwDispId;
	}

	public Long getLastUwDispTime() {
		return lastUwDispTime;
	}

	public void setLastUwDispTime(Long lastUwDispTime) {
		this.lastUwDispTime = lastUwDispTime;
	}

	public BigDecimal getOrderId() {
		return orderId;
	}

	public void setOrderId(BigDecimal orderId) {
		this.orderId = orderId;
	}

	public BigDecimal getPolicyChgStatus() {
		return policyChgStatus;
	}

	public void setPolicyChgStatus(BigDecimal policyChgStatus) {
		this.policyChgStatus = policyChgStatus;
	}

	public BigDecimal getDispatchEmp() {
		return dispatchEmp;
	}

	public void setDispatchEmp(BigDecimal dispatchEmp) {
		this.dispatchEmp = dispatchEmp;
	}

	public String getLetterEffectType() {
		return letterEffectType;
	}

	public void setLetterEffectType(String letterEffectType) {
		this.letterEffectType = letterEffectType;
	}

	public String getDispatchType() {
		return dispatchType;
	}

	public void setDispatchType(String dispatchType) {
		this.dispatchType = dispatchType;
	}

	public String getDispatchLetter() {
		return dispatchLetter;
	}

	public void setDispatchLetter(String dispatchLetter) {
		this.dispatchLetter = dispatchLetter;
	}

	public BigDecimal getSubServiceId() {
		return subServiceId;
	}

	public void setSubServiceId(BigDecimal subServiceId) {
		this.subServiceId = subServiceId;
	}

	public String getChangeNote() {
		return changeNote;
	}

	public void setChangeNote(String changeNote) {
		this.changeNote = changeNote;
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

	public BigDecimal getPrePolicyChg() {
		return prePolicyChg;
	}

	public void setPrePolicyChg(BigDecimal prePolicyChg) {
		this.prePolicyChg = prePolicyChg;
	}

	public BigDecimal getChangeSeq() {
		return changeSeq;
	}

	public void setChangeSeq(BigDecimal changeSeq) {
		this.changeSeq = changeSeq;
	}

	public Long getFinishTime() {
		return finishTime;
	}

	public void setFinishTime(Long finishTime) {
		this.finishTime = finishTime;
	}

	public BigDecimal getOrgId() {
		return orgId;
	}

	public void setOrgId(BigDecimal orgId) {
		this.orgId = orgId;
	}

	public Long getRequestEffectDate() {
		return requestEffectDate;
	}

	public void setRequestEffectDate(Long requestEffectDate) {
		this.requestEffectDate = requestEffectDate;
	}

	public Long getTaskDueDate() {
		return taskDueDate;
	}

	public void setTaskDueDate(Long taskDueDate) {
		this.taskDueDate = taskDueDate;
	}

	public BigDecimal getAlternationInfoId() {
		return alternationInfoId;
	}

	public void setAlternationInfoId(BigDecimal alternationInfoId) {
		this.alternationInfoId = alternationInfoId;
	}

	public BigDecimal getPolicyChgOrderSeq() {
		return policyChgOrderSeq;
	}

	public void setPolicyChgOrderSeq(BigDecimal policyChgOrderSeq) {
		this.policyChgOrderSeq = policyChgOrderSeq;
	}
	
	
}
