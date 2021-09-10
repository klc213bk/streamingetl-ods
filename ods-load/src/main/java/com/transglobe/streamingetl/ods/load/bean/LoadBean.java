package com.transglobe.streamingetl.ods.load.bean;

import java.sql.Timestamp;

public class LoadBean {
	public int seq;
	public int loadBeanSize;
	public long startTime;
	public String tableName;
	public long startSeq;
	public long endSeq;
	public long span;
	public long count = 0L;
	public Long currentScn;

}
