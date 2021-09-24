package com.transglobe.streamingetl.ods.consumer.model;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Json Property properties based on EBAO.T_IMAGE
 * Using Long for DATE type.
 *
 */
public class TImage {
	@JsonProperty("IMAGE_ID")
	private BigDecimal imageId;

	@JsonProperty("POLICY_ID")
	private BigDecimal policyId;

	@JsonProperty("IMAGE_TYPE_ID")
	private BigDecimal imageTypeId;

	@JsonProperty("SEQ_NUMBER")
	private BigDecimal seqNumber;

	@JsonProperty("IMAGE_FORMAT")
	private String imageFormat;

	//ignore @JsonProperty("IMAGE_DATA")
	//BLOB
	
	@JsonProperty("SCAN_TIME")
	private Long scanTime;

	@JsonProperty("EMP_ID")
	private BigDecimal empId;

	@JsonProperty("HEAD_ID")
	private BigDecimal headId;

	@JsonProperty("FILE_CODE")
	private String fileCode;

	@JsonProperty("PROCESS_STATUS")
	private String processStatus;

	@JsonProperty("GROUP_POLICY_ID")
	private BigDecimal groupPolicyId;

	@JsonProperty("CASE_ID")
	private BigDecimal caseId;

	@JsonProperty("CHANGE_ID")
	private BigDecimal changeId;

	@JsonProperty("IMAGE_LOCATION")
 	private BigDecimal imageLocation;

	@JsonProperty("IMAGE_FILE_NAME")
	private String imageFileName;

	@JsonProperty("AUTH_CODE")
	private String authCode;

	@JsonProperty("ORGAN_ID")
	private String organId;

	@JsonProperty("SUB_FILE_CODE")
	private String subFileCode;

	@JsonProperty("BUSINESS_ORGAN")
	private String businessOrgan;

	@JsonProperty("IS_PRIORITY")
	private String isPriority;

	@JsonProperty("ZIP_DATE")
	private Long zipDate;

	@JsonProperty("IS_CLEAR")
	private String isClear;

	@JsonProperty("LIST_ID")
	private BigDecimal listId;

	@JsonProperty("INSERT_TIME")
	private Long insertTime;

	@JsonProperty("INSERTED_BY")
	private BigDecimal insertedBy;

	@JsonProperty("UPDATE_TIME")
	private Long updateTime;

	@JsonProperty("UPDATED_BY")
	private BigDecimal updatedBy;

	@JsonProperty("INSERT_TIMESTAMP")
	private Long insertTimestamp;

	@JsonProperty("UPDATE_TIMESTAMP")
	private Long updateTimestamp;

	@JsonProperty("DEPT_ID")
	private BigDecimal deptId;

	@JsonProperty("COMPANY_CODE")
	private String companyCode;

	@JsonProperty("PERSONAL_CODE")
	private String personCode;

	@JsonProperty("BATCH_DEPT_TYPE")
	private String batchDeptType;

	@JsonProperty("BATCH_DATE")
	private String batchDate;

	@JsonProperty("BATCH_AREA")
	private String batchArea;

	@JsonProperty("BATCH_DOC_TYPE")
	private String batchDocType;

	@JsonProperty("BOX_NO")
	private String boxNo;

	@JsonProperty("REMARK")
	private String remark;

	@JsonProperty("SIGNATURE")
	private String signature;

	@JsonProperty("REAL_WIDTH")
	private BigDecimal realWidth;

	@JsonProperty("SIG_SEQ_NUMBER")
	private BigDecimal sigSeqNumber;

	@JsonProperty("SCAN_ORDER")
	private BigDecimal scanOrder;

	public BigDecimal getImageId() {
		return imageId;
	}

	public void setImageId(BigDecimal imageId) {
		this.imageId = imageId;
	}

	public BigDecimal getPolicyId() {
		return policyId;
	}

	public void setPolicyId(BigDecimal policyId) {
		this.policyId = policyId;
	}

	public BigDecimal getImageTypeId() {
		return imageTypeId;
	}

	public void setImageTypeId(BigDecimal imageTypeId) {
		this.imageTypeId = imageTypeId;
	}

	public BigDecimal getSeqNumber() {
		return seqNumber;
	}

	public void setSeqNumber(BigDecimal seqNumber) {
		this.seqNumber = seqNumber;
	}

	public String getImageFormat() {
		return imageFormat;
	}

	public void setImageFormat(String imageFormat) {
		this.imageFormat = imageFormat;
	}

	public Long getScanTime() {
		return scanTime;
	}

	public void setScanTime(Long scanTime) {
		this.scanTime = scanTime;
	}

	public BigDecimal getEmpId() {
		return empId;
	}

	public void setEmpId(BigDecimal empId) {
		this.empId = empId;
	}

	public BigDecimal getHeadId() {
		return headId;
	}

	public void setHeadId(BigDecimal headId) {
		this.headId = headId;
	}

	public String getFileCode() {
		return fileCode;
	}

	public void setFileCode(String fileCode) {
		this.fileCode = fileCode;
	}

	public String getProcessStatus() {
		return processStatus;
	}

	public void setProcessStatus(String processStatus) {
		this.processStatus = processStatus;
	}

	public BigDecimal getGroupPolicyId() {
		return groupPolicyId;
	}

	public void setGroupPolicyId(BigDecimal groupPolicyId) {
		this.groupPolicyId = groupPolicyId;
	}

	public BigDecimal getCaseId() {
		return caseId;
	}

	public void setCaseId(BigDecimal caseId) {
		this.caseId = caseId;
	}

	public BigDecimal getChangeId() {
		return changeId;
	}

	public void setChangeId(BigDecimal changeId) {
		this.changeId = changeId;
	}

	public BigDecimal getImageLocation() {
		return imageLocation;
	}

	public void setImageLocation(BigDecimal imageLocation) {
		this.imageLocation = imageLocation;
	}

	public String getImageFileName() {
		return imageFileName;
	}

	public void setImageFileName(String imageFileName) {
		this.imageFileName = imageFileName;
	}

	public String getAuthCode() {
		return authCode;
	}

	public void setAuthCode(String authCode) {
		this.authCode = authCode;
	}

	public String getOrganId() {
		return organId;
	}

	public void setOrganId(String organId) {
		this.organId = organId;
	}

	public String getSubFileCode() {
		return subFileCode;
	}

	public void setSubFileCode(String subFileCode) {
		this.subFileCode = subFileCode;
	}

	public String getBusinessOrgan() {
		return businessOrgan;
	}

	public void setBusinessOrgan(String businessOrgan) {
		this.businessOrgan = businessOrgan;
	}

	public String getIsPriority() {
		return isPriority;
	}

	public void setIsPriority(String isPriority) {
		this.isPriority = isPriority;
	}

	public Long getZipDate() {
		return zipDate;
	}

	public void setZipDate(Long zipDate) {
		this.zipDate = zipDate;
	}

	public String getIsClear() {
		return isClear;
	}

	public void setIsClear(String isClear) {
		this.isClear = isClear;
	}

	public BigDecimal getListId() {
		return listId;
	}

	public void setListId(BigDecimal listId) {
		this.listId = listId;
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

	public Long getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Long updateTime) {
		this.updateTime = updateTime;
	}

	public BigDecimal getUpdatedBy() {
		return updatedBy;
	}

	public void setUpdatedBy(BigDecimal updatedBy) {
		this.updatedBy = updatedBy;
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

	public BigDecimal getDeptId() {
		return deptId;
	}

	public void setDeptId(BigDecimal deptId) {
		this.deptId = deptId;
	}

	public String getCompanyCode() {
		return companyCode;
	}

	public void setCompanyCode(String companyCode) {
		this.companyCode = companyCode;
	}

	public String getPersonCode() {
		return personCode;
	}

	public void setPersonCode(String personCode) {
		this.personCode = personCode;
	}

	public String getBatchDeptType() {
		return batchDeptType;
	}

	public void setBatchDeptType(String batchDeptType) {
		this.batchDeptType = batchDeptType;
	}

	public String getBatchDate() {
		return batchDate;
	}

	public void setBatchDate(String batchDate) {
		this.batchDate = batchDate;
	}

	public String getBatchArea() {
		return batchArea;
	}

	public void setBatchArea(String batchArea) {
		this.batchArea = batchArea;
	}

	public String getBatchDocType() {
		return batchDocType;
	}

	public void setBatchDocType(String batchDocType) {
		this.batchDocType = batchDocType;
	}

	public String getBoxNo() {
		return boxNo;
	}

	public void setBoxNo(String boxNo) {
		this.boxNo = boxNo;
	}

	public String getRemark() {
		return remark;
	}

	public void setRemark(String remark) {
		this.remark = remark;
	}

	public String getSignature() {
		return signature;
	}

	public void setSignature(String signature) {
		this.signature = signature;
	}

	public BigDecimal getRealWidth() {
		return realWidth;
	}

	public void setRealWidth(BigDecimal realWidth) {
		this.realWidth = realWidth;
	}

	public BigDecimal getSigSeqNumber() {
		return sigSeqNumber;
	}

	public void setSigSeqNumber(BigDecimal sigSeqNumber) {
		this.sigSeqNumber = sigSeqNumber;
	}

	public BigDecimal getScanOrder() {
		return scanOrder;
	}

	public void setScanOrder(BigDecimal scanOrder) {
		this.scanOrder = scanOrder;
	}
	
	
}
