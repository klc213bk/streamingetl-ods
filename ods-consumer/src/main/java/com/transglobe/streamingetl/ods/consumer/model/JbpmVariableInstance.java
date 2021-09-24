package com.transglobe.streamingetl.ods.consumer.model;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Json Property properties based on EBAO.JBPM_VARIABLEINSTANCE
 * Using Long for DATE, TIMESTAMP type., Double for Float type
 *
 */
public class JbpmVariableInstance {
	@JsonProperty("ID_")
	private BigDecimal id_;

	@JsonProperty("CLASS_")
	private String class_;

	@JsonProperty("VERSION_")
	private BigDecimal version_;

	@JsonProperty("NAME_")
	private String name_;

	@JsonProperty("CONVERTER_")
	private String converter_;

	@JsonProperty("TOKEN_")
	private BigDecimal token;

	@JsonProperty("TOKENVARIABLEMAP_")
	private BigDecimal tokenvariablemap_;

	@JsonProperty("PROCESSINSTANCE_")
	private BigDecimal processinstance_;

	@JsonProperty("BYTEARRAYVALUE_")
	private BigDecimal bytearrayvalue;

	@JsonProperty("DATEVALUE")
	private Long datevalue;
	
	@JsonProperty("DOUBLEVALUE")
	private Double doublevalue;
	
	@JsonProperty("LONGIDCLASS_")
	private String longidclass_;

	@JsonProperty("LONGVALUE_")
	private BigDecimal longvalue_;

	@JsonProperty("STRINGIDCLASS_")
	private String stringidclass;

	@JsonProperty("STRINGVALUE_")
	private String stringvalue_;

	@JsonProperty("TASKINSTANCE_")
	private BigDecimal taskinstance_;

	public BigDecimal getId_() {
		return id_;
	}

	public void setId_(BigDecimal id_) {
		this.id_ = id_;
	}

	public String getClass_() {
		return class_;
	}

	public void setClass_(String class_) {
		this.class_ = class_;
	}

	public BigDecimal getVersion_() {
		return version_;
	}

	public void setVersion_(BigDecimal version_) {
		this.version_ = version_;
	}

	public String getName_() {
		return name_;
	}

	public void setName_(String name_) {
		this.name_ = name_;
	}

	public String getConverter_() {
		return converter_;
	}

	public void setConverter_(String converter_) {
		this.converter_ = converter_;
	}

	public BigDecimal getToken() {
		return token;
	}

	public void setToken(BigDecimal token) {
		this.token = token;
	}

	public BigDecimal getTokenvariablemap_() {
		return tokenvariablemap_;
	}

	public void setTokenvariablemap_(BigDecimal tokenvariablemap_) {
		this.tokenvariablemap_ = tokenvariablemap_;
	}

	public BigDecimal getProcessinstance_() {
		return processinstance_;
	}

	public void setProcessinstance_(BigDecimal processinstance_) {
		this.processinstance_ = processinstance_;
	}

	public BigDecimal getBytearrayvalue() {
		return bytearrayvalue;
	}

	public void setBytearrayvalue(BigDecimal bytearrayvalue) {
		this.bytearrayvalue = bytearrayvalue;
	}

	public Long getDatevalue() {
		return datevalue;
	}

	public void setDatevalue(Long datevalue) {
		this.datevalue = datevalue;
	}

	public Double getDoublevalue() {
		return doublevalue;
	}

	public void setDoublevalue(Double doublevalue) {
		this.doublevalue = doublevalue;
	}

	public String getLongidclass_() {
		return longidclass_;
	}

	public void setLongidclass_(String longidclass_) {
		this.longidclass_ = longidclass_;
	}

	public BigDecimal getLongvalue_() {
		return longvalue_;
	}

	public void setLongvalue_(BigDecimal longvalue_) {
		this.longvalue_ = longvalue_;
	}

	public String getStringidclass() {
		return stringidclass;
	}

	public void setStringidclass(String stringidclass) {
		this.stringidclass = stringidclass;
	}

	public String getStringvalue_() {
		return stringvalue_;
	}

	public void setStringvalue_(String stringvalue_) {
		this.stringvalue_ = stringvalue_;
	}

	public BigDecimal getTaskinstance_() {
		return taskinstance_;
	}

	public void setTaskinstance_(BigDecimal taskinstance_) {
		this.taskinstance_ = taskinstance_;
	}
	
	
}
