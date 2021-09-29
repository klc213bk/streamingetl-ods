package com.transglobe.streamingetl.ods.update.bean;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.transglobe.streamingetl.common.bean.CommonConstants;
import com.transglobe.streamingetl.common.util.OracleUtils;
import com.transglobe.streamingetl.common.util.StreamingEtlUtils;
import com.transglobe.streamingetl.ods.load.Config;
import com.transglobe.streamingetl.ods.load.bean.DataLoader;

public abstract class UpdateDataLoader extends DataLoader {
	private static final Logger logger = LoggerFactory.getLogger(UpdateDataLoader.class);

	public UpdateDataLoader() {}

	public UpdateDataLoader(
			Config config
			, String fromUpdateTime
			, String toUpdateTime) throws Exception {
		this(DataLoader.DEFAULT_THREADS, DataLoader.DEFAULT_BATCH_COMMIT_SIZE, config, fromUpdateTime, toUpdateTime);

	}
	public UpdateDataLoader(
			int threads
			, int batchCommitSize
			, Config config
			, String fromUpdateTime
			, String toUpdateTime) throws Exception {
		super(threads, batchCommitSize, config, null);
	

	}
	public void createUpdateTable() throws Exception {
		String updateTableCreateFileName = getUpdateTableCreateFileName();
		Connection sinkConn = null;
		try {
			sinkConn = getUpdateConnection();
			OracleUtils.executeScriptFromFile(updateTableCreateFileName, sinkConn);

		} finally {
			try {
				if (sinkConn != null) sinkConn.close();
			} catch (Exception e) {
				logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			}
		}
	}
	private Connection getUpdateConnection() throws SQLException {
		return sinkConnectionPool.getConnection();
	}
	public void dropUpdateTable() throws Exception {
		String updateTableName = getUpdateTableName();
		Connection updateConn = null;
		try {
			updateConn = getUpdateConnection();
			OracleUtils.dropTable(updateTableName, updateConn);

		} finally {
			try {
				if (updateConn != null) updateConn.close();
			} catch (Exception e) {
				logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			}
		}
	}

	@Override
	protected String getSinkTableName() {
		return null;
	}
	@Override
	protected String getSinkTableCreateFileName() {
		return null;
	}
	@Override
	protected String getSinkTableIndexesCreateFileName() {
		return null;
	}
	abstract protected String getUpdateTableName();
	abstract protected String getUpdateTableCreateFileName();
}
