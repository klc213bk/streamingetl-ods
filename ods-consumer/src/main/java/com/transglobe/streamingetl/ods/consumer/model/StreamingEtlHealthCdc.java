package com.transglobe.streamingetl.ods.consumer.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class StreamingEtlHealthCdc {

	@JsonProperty("CDC_TIME")
	private Long cdctime;

	public Long getCdctime() {
		return cdctime;
	}

	public void setCdctime(Long cdctime) {
		this.cdctime = cdctime;
	}

}
