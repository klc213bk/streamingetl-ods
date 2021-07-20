package com.transglobe.streamingetl.ods.consumer.model;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * properties based on ODS.O_PRODUCTION_DETAIL
 * Json Property properties based on EBAO.T_PRODUCTION_DETAIL
 * @author steven
 *
 */
public class ProductionDetail {

	@JsonProperty("DETAIL_ID")
	private BigDecimal detailId;
	
	@JsonProperty("PRODUCTION_ID")
	private BigDecimal productionId;
	
	@JsonProperty("POLICY_ID")
	private String policyId;
	
	@JsonProperty("ITEM_ID")
	private String itemId;
	
	@JsonProperty("PRODUCT_ID")
	private String productId;
	
	@JsonProperty("POLICY_YEAR")
	private String policyYear;
	
	@JsonProperty("PRODUCTION_VALUE")
	private BigDecimal productionValue;
	
	@JsonProperty("EFFECTIVE_DATE")
	private String effectiveDate;

	@JsonProperty("HIERARCHY_DATE")
	private String hierarchydate;

	@JsonProperty("PRODUCER_ID")
	private Long producerId;
	
	@JsonProperty("PRODUCER_POSITION")
	private String producerPosition;
	
	@JsonProperty("BENEFIT_TYPE")
	private String benefitType;
	
	@JsonProperty("FEE_TYPE")
	private BigDecimal feeType;
	
	@JsonProperty("CHARGE_MODE")
	private String chargeMode;
	
	@JsonProperty("PREM_LIST_ID")
	private BigDecimal premListId;
	
	@JsonProperty("COMM_ITEM_ID")
	private BigDecimal commItemId;
	
	@JsonProperty("POLICY_CHG_ID")
	private BigDecimal policyChgId;
	
	@JsonProperty("EXCHANGE_RATE")
	private BigDecimal exchangeRate;
	
	@JsonProperty("RELATED_ID")
	private BigDecimal relatedId;
	
	@JsonProperty("INSURED_ID")
	private BigDecimal insuredId;
	
	@JsonProperty("POL_PRODUCTION_VALUE")
	private BigDecimal polProductionValue;
	
	@JsonProperty("POL_CURRENCY_ID")
	private BigDecimal polCurrencyId;
	
	@JsonProperty("HIERARCHY_EXIST_INDI")
	private String hierarchyExistIndi;
	
	@JsonProperty("AGGREGATION_ID")
	private BigDecimal aggregationId;
	
	@JsonProperty("PRODUCT_VERSION")
	private BigDecimal productVersion;
	
	@JsonProperty("SOURCE_TABLE")
	private String sourceTable;
	
	@JsonProperty("SOURCE_ID")
	private BigDecimal sourceId;
	
	@JsonProperty("RESULT_LIST_ID")
	private BigDecimal resultListId;
	
	@JsonProperty("FINISH_TIME")
	private Long finishTime; 
	
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
	
	@JsonProperty("COMMISSION_RATE")
	private BigDecimal commissionRate;
	
	@JsonProperty("CHEQUE_INDI")
	private String chequeIndi; 
	
	@JsonProperty("PREM_ALLOCATE_YEAR")
	private BigDecimal premAllocateYear;
	
	@JsonProperty("RECALCULATED_INDI")
	private String recalculatedindi;
	
	@JsonProperty("EXCLUDE_POLICY_INDI")
	private String excludePolicyIndi;
	
	@JsonProperty("CHANNEL_ORG_ID")
	private BigDecimal channelOrgId;
	
	@JsonProperty("AGENT_CATE")
	private String agentCate;
	
	@JsonProperty("YEAR_MONTH")
	private String yearMonth;
	
	@JsonProperty("CONVERSION_CATE")
	private BigDecimal conversionCate;
	
	@JsonProperty("ORDER_ID")
	private BigDecimal orderId;
	
	@JsonProperty("ASSIGN_RATE")
	private BigDecimal assignRate;
	
	@JsonProperty("ACCEPT_ID")	
	private BigDecimal acceptId;

	private Long dataDate; 
	
	private Long tblUpdTime;
    
	public BigDecimal getDetailId() {
		return detailId;
	}

	public void setDetailId(BigDecimal detailId) {
		this.detailId = detailId;
	}

	public BigDecimal getProductionId() {
		return productionId;
	}

	public void setProductionId(BigDecimal productionId) {
		this.productionId = productionId;
	}

	public String getPolicyId() {
		return policyId;
	}

	public void setPolicyId(String policyId) {
		this.policyId = policyId;
	}

	public String getItemId() {
		return itemId;
	}

	public void setItemId(String itemId) {
		this.itemId = itemId;
	}

	public String getProductId() {
		return productId;
	}

	public void setProductId(String productId) {
		this.productId = productId;
	}

	public String getPolicyYear() {
		return policyYear;
	}

	public void setPolicyYear(String policyYear) {
		this.policyYear = policyYear;
	}

	public BigDecimal getProductionValue() {
		return productionValue;
	}

	public void setProductionValue(BigDecimal productionValue) {
		this.productionValue = productionValue;
	}

	public String getEffectiveDate() {
		return effectiveDate;
	}

	public void setEffectiveDate(String effectiveDate) {
		this.effectiveDate = effectiveDate;
	}

	public String getHierarchydate() {
		return hierarchydate;
	}

	public void setHierarchydate(String hierarchydate) {
		this.hierarchydate = hierarchydate;
	}

	public Long getProducerId() {
		return producerId;
	}

	public void setProducerId(Long producerId) {
		this.producerId = producerId;
	}

	public String getProducerPosition() {
		return producerPosition;
	}

	public void setProducerPosition(String producerPosition) {
		this.producerPosition = producerPosition;
	}

	public String getBenefitType() {
		return benefitType;
	}

	public void setBenefitType(String benefitType) {
		this.benefitType = benefitType;
	}

	public BigDecimal getFeeType() {
		return feeType;
	}

	public void setFeeType(BigDecimal feeType) {
		this.feeType = feeType;
	}

	public String getChargeMode() {
		return chargeMode;
	}

	public void setChargeMode(String chargeMode) {
		this.chargeMode = chargeMode;
	}

	public BigDecimal getPremListId() {
		return premListId;
	}

	public void setPremListId(BigDecimal premListId) {
		this.premListId = premListId;
	}

	public BigDecimal getCommItemId() {
		return commItemId;
	}

	public void setCommItemId(BigDecimal commItemId) {
		this.commItemId = commItemId;
	}

	public BigDecimal getPolicyChgId() {
		return policyChgId;
	}

	public void setPolicyChgId(BigDecimal policyChgId) {
		this.policyChgId = policyChgId;
	}

	public BigDecimal getExchangeRate() {
		return exchangeRate;
	}

	public void setExchangeRate(BigDecimal exchangeRate) {
		this.exchangeRate = exchangeRate;
	}

	public BigDecimal getRelatedId() {
		return relatedId;
	}

	public void setRelatedId(BigDecimal relatedId) {
		this.relatedId = relatedId;
	}

	public BigDecimal getInsuredId() {
		return insuredId;
	}

	public void setInsuredId(BigDecimal insuredId) {
		this.insuredId = insuredId;
	}

	public BigDecimal getPolProductionValue() {
		return polProductionValue;
	}

	public void setPolProductionValue(BigDecimal polProductionValue) {
		this.polProductionValue = polProductionValue;
	}

	public BigDecimal getPolCurrencyId() {
		return polCurrencyId;
	}

	public void setPolCurrencyId(BigDecimal polCurrencyId) {
		this.polCurrencyId = polCurrencyId;
	}

	public String getHierarchyExistIndi() {
		return hierarchyExistIndi;
	}

	public void setHierarchyExistIndi(String hierarchyExistIndi) {
		this.hierarchyExistIndi = hierarchyExistIndi;
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

	public String getSourceTable() {
		return sourceTable;
	}

	public void setSourceTable(String sourceTable) {
		this.sourceTable = sourceTable;
	}

	public BigDecimal getSourceId() {
		return sourceId;
	}

	public void setSourceId(BigDecimal sourceId) {
		this.sourceId = sourceId;
	}

	public BigDecimal getResultListId() {
		return resultListId;
	}

	public void setResultListId(BigDecimal resultListId) {
		this.resultListId = resultListId;
	}

	public Long getFinishTime() {
		return finishTime;
	}

	public void setFinishTime(Long finishTime) {
		this.finishTime = finishTime;
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

	public BigDecimal getCommissionRate() {
		return commissionRate;
	}

	public void setCommissionRate(BigDecimal commissionRate) {
		this.commissionRate = commissionRate;
	}

	public String getChequeIndi() {
		return chequeIndi;
	}

	public void setChequeIndi(String chequeIndi) {
		this.chequeIndi = chequeIndi;
	}

	public BigDecimal getPremAllocateYear() {
		return premAllocateYear;
	}

	public void setPremAllocateYear(BigDecimal premAllocateYear) {
		this.premAllocateYear = premAllocateYear;
	}

	public String getRecalculatedindi() {
		return recalculatedindi;
	}

	public void setRecalculatedindi(String recalculatedindi) {
		this.recalculatedindi = recalculatedindi;
	}

	public String getExcludePolicyIndi() {
		return excludePolicyIndi;
	}

	public void setExcludePolicyIndi(String excludePolicyIndi) {
		this.excludePolicyIndi = excludePolicyIndi;
	}

	public BigDecimal getChannelOrgId() {
		return channelOrgId;
	}

	public void setChannelOrgId(BigDecimal channelOrgId) {
		this.channelOrgId = channelOrgId;
	}

	public String getAgentCate() {
		return agentCate;
	}

	public void setAgentCate(String agentCate) {
		this.agentCate = agentCate;
	}

	public String getYearMonth() {
		return yearMonth;
	}

	public void setYearMonth(String yearMonth) {
		this.yearMonth = yearMonth;
	}

	public BigDecimal getConversionCate() {
		return conversionCate;
	}

	public void setConversionCate(BigDecimal conversionCate) {
		this.conversionCate = conversionCate;
	}

	public BigDecimal getOrderId() {
		return orderId;
	}

	public void setOrderId(BigDecimal orderId) {
		this.orderId = orderId;
	}

	public BigDecimal getAssignRate() {
		return assignRate;
	}

	public void setAssignRate(BigDecimal assignRate) {
		this.assignRate = assignRate;
	}

	public BigDecimal getAcceptId() {
		return acceptId;
	}

	public void setAcceptId(BigDecimal acceptId) {
		this.acceptId = acceptId;
	}

	public Long getDataDate() {
		return dataDate;
	}

	public void setDataDate(Long dataDate) {
		this.dataDate = dataDate;
	}

	public Long getTblUpdTime() {
		return tblUpdTime;
	}

	public void setTblUpdTime(Long tblUpdTime) {
		this.tblUpdTime = tblUpdTime;
	}
	
	
	
}
