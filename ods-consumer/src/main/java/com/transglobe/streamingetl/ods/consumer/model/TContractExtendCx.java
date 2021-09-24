package com.transglobe.streamingetl.ods.consumer.model;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Json Property properties based on EBAO.T_ContractExtendCx
 * Using Long for DATE type.
 *
 */
public class TContractExtendCx {

	@JsonProperty("POLICY_CHG_ID")
	private BigDecimal policyChgId;
	
	@JsonProperty("ITEM_ID")
	private BigDecimal itemId;
	
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

	public BigDecimal getPolicyId() {
		return policyId;
	}

	public void setPolicyId(BigDecimal policyId) {
		this.policyId = policyId;
	}

	public BigDecimal getChangeId() {
		return changeId;
	}

	public void setChangeId(BigDecimal changeId) {
		this.changeId = changeId;
	}

	public BigDecimal getLogId() {
		return logId;
	}

	public void setLogId(BigDecimal logId) {
		this.logId = logId;
	}

	public BigDecimal getPreLogId() {
		return preLogId;
	}

	public void setPreLogId(BigDecimal preLogId) {
		this.preLogId = preLogId;
	}

	public String getOperType() {
		return operType;
	}

	public void setOperType(String operType) {
		this.operType = operType;
	}

	public String getLogType() {
		return logType;
	}

	public void setLogType(String logType) {
		this.logType = logType;
	}

	public BigDecimal getChangeSeq() {
		return changeSeq;
	}

	public void setChangeSeq(BigDecimal changeSeq) {
		this.changeSeq = changeSeq;
	}

	@JsonProperty("POLICY_ID")
	private BigDecimal policyId;
	
	@JsonProperty("CHANGE_ID")
	private BigDecimal changeId;
	
	@JsonProperty("LOG_ID")
	private BigDecimal logId;
	
	@JsonProperty("PRE_LOG_ID")
	private BigDecimal preLogId;
	
	@JsonProperty("OPER_TYPE")
	private String operType;
	
	@JsonProperty("LOG_TYPE")
	private String logType;
	
	@JsonProperty("CHANGE_SEQ")
	private BigDecimal changeSeq;
	
}
